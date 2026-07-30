/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.flink;

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.sink.AutoTagForSavepointCommitterOperator;
import org.apache.paimon.flink.sink.FlinkSinkBuilder;
import org.apache.paimon.flink.source.AbstractNonCoordinatedSource;
import org.apache.paimon.flink.source.AbstractNonCoordinatedSourceReader;
import org.apache.paimon.flink.source.SimpleSourceSplit;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that a {@code savepoint-<id>} tag survives a restore-from-savepoint, and that the
 * coordinator-commit path behaves identically to the classic operator-commit path across the
 * restore. This exercises the coordinator's {@code recover()} re-tagging (the coordinator rebuilds
 * its pending savepoint set from the writer's replayed committables rather than from committer
 * state), which has no counterpart in the operator path other than the shared outcome: the tag is
 * still present and unchanged after restore.
 *
 * <p>Uses a source that emits continuously so the async savepoint deterministically lands on a
 * data-carrying checkpoint; the empty-savepoint boundary (a separate, shared limitation) is
 * intentionally avoided here.
 */
public class AppendTableSavepointRestoreTagITCase {

    private static final long WAIT_TIMEOUT_MILLIS = 120_000L;

    @RegisterExtension
    protected static final org.apache.paimon.flink.util.MiniClusterWithClientExtension
            MINI_CLUSTER_EXTENSION =
                    new org.apache.paimon.flink.util.MiniClusterWithClientExtension(
                            new MiniClusterResourceConfiguration.Builder()
                                    .setNumberTaskManagers(1)
                                    .setNumberSlotsPerTaskManager(1)
                                    .build());

    @TempDir Path tempPath;

    @AfterEach
    public final void cleanupRunningJobs() throws Exception {
        ClusterClient<?> clusterClient = MINI_CLUSTER_EXTENSION.createRestClusterClient();
        for (JobStatusMessage job : clusterClient.listJobs().get()) {
            if (!job.getJobState().isTerminalState()) {
                try {
                    clusterClient.cancel(job.getJobId()).get(30, TimeUnit.SECONDS);
                } catch (Exception ignored) {
                    // best-effort cleanup
                }
            }
        }
    }

    @ParameterizedTest(name = "coordinatorCommit = {0}")
    @ValueSource(booleans = {true, false})
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    public void testSavepointTagSurvivesRestore(boolean coordinatorCommit) throws Exception {
        String tableName = coordinatorCommit ? "T_COORD_RESTORE" : "T_CLASSIC_RESTORE";
        FileStoreTable table = createTable(tableName, coordinatorCommit);

        // Phase 1: run the job, take an async savepoint, and let its tag materialize. The source
        // emits every poll so every checkpoint (and thus the savepoint) carries data. After the tag
        // exists it is deleted on purpose, so phase 2 can only pass if restore RE-creates it.
        String savepointPath;
        long taggedIdentifierBeforeRestore;
        String deletedTagName;
        JobClient firstClient = runSink(table, null);
        try {
            waitUntilSnapshotWithData(table);
            savepointPath =
                    firstClient
                            .triggerSavepoint(
                                    tempPath + "/savepoint_" + tableName,
                                    SavepointFormatType.DEFAULT)
                            .get(60, TimeUnit.SECONDS);
            Map<Snapshot, List<String>> tags = waitUntilSavepointTagCreated(table);
            assertThat(tags).hasSize(1);
            taggedIdentifierBeforeRestore = tags.keySet().iterator().next().commitIdentifier();
            deletedTagName =
                    AutoTagForSavepointCommitterOperator.SAVEPOINT_TAG_PREFIX
                            + taggedIdentifierBeforeRestore;
        } finally {
            firstClient.cancel().get(30, TimeUnit.SECONDS);
        }

        // Delete the tag so its presence after restore is proof that restore re-created it, not a
        // leftover from phase 1. This makes the assertion able to fail if recover() stops tagging.
        table.deleteTag(deletedTagName);
        assertThat(savepointTags(table)).isEmpty();

        // Phase 2: restore the same job from that savepoint. The writer replays its pending
        // committables (carrying the savepoint bit); for coordinator commit this drives recover(),
        // which re-creates the tag. The classic path re-creates it from its committer ListState on
        // initializeState. Either way the savepoint tag must reappear, point at the same id, and be
        // unique.
        JobClient secondClient = runSink(table, savepointPath);
        try {
            Map<Snapshot, List<String>> tags = waitUntilSavepointTagCreated(table);
            assertThat(tags).hasSize(1);
            Map.Entry<Snapshot, List<String>> entry = tags.entrySet().iterator().next();
            assertThat(entry.getValue())
                    .containsExactly(
                            AutoTagForSavepointCommitterOperator.SAVEPOINT_TAG_PREFIX
                                    + entry.getKey().commitIdentifier());
            assertThat(entry.getKey().commitIdentifier())
                    .isEqualTo(taggedIdentifierBeforeRestore);
        } finally {
            secondClient.cancel().get(30, TimeUnit.SECONDS);
        }
    }

    private FileStoreTable createTable(String tableName, boolean coordinatorCommit)
            throws Exception {
        TableEnvironment tEnv =
                TableEnvironment.create(
                        EnvironmentSettings.newInstance().inStreamingMode().build());
        tEnv.executeSql(
                "CREATE CATALOG mycat WITH ( 'type' = 'paimon', 'warehouse' = '"
                        + tempPath
                        + "' )");
        tEnv.executeSql("USE CATALOG mycat");
        // A stable operator-uid suffix so the writer's state maps back on restore-from-savepoint.
        String coordinatorOption =
                coordinatorCommit
                        ? ", 'sink.coordinator-commit.enabled' = 'true', 'write-only' = 'true'"
                        : "";
        tEnv.executeSql(
                "CREATE TABLE "
                        + tableName
                        + " (id INT, data STRING) WITH ("
                        + "'bucket' = '-1', "
                        + "'sink.savepoint.auto-tag' = 'true', "
                        + "'commit.force-create-snapshot' = 'true', "
                        + "'sink.operator-uid.suffix' = 'restore-tag'"
                        + coordinatorOption
                        + ")");
        return (FileStoreTable)
                ((FlinkCatalog) tEnv.getCatalog("mycat").get())
                        .catalog()
                        .getTable(Identifier.create("default", tableName));
    }

    /** Runs the sink job, optionally resuming from {@code savepointPath} when non-null. */
    private JobClient runSink(FileStoreTable table, String savepointPath) throws Exception {
        // fixed-delay restart so the coordinator's intentional post-recover failover throw is
        // retried instead of failing the job outright.
        Configuration conf = new Configuration();
        conf.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, Integer.MAX_VALUE);
        conf.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(1));
        if (savepointPath != null) {
            SavepointRestoreSettings.toConfiguration(
                    SavepointRestoreSettings.forPath(savepointPath, false), conf);
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        env.enableCheckpointing(200);
        DataStreamSource<RowData> stream =
                env.fromSource(
                        new ContinuousSource(), WatermarkStrategy.noWatermarks(), "restore-source");
        new FlinkSinkBuilder(table).forRowData(stream).build();
        return env.executeAsync("savepoint-restore-tag");
    }

    private void waitUntilSnapshotWithData(FileStoreTable table) throws Exception {
        long deadline = System.currentTimeMillis() + WAIT_TIMEOUT_MILLIS;
        while (System.currentTimeMillis() < deadline) {
            Snapshot latest = table.snapshotManager().latestSnapshot();
            if (latest != null && latest.totalRecordCount() > 0) {
                return;
            }
            Thread.sleep(200);
        }
        throw new IllegalStateException("no data-carrying snapshot committed within timeout");
    }

    private Map<Snapshot, List<String>> waitUntilSavepointTagCreated(FileStoreTable table)
            throws Exception {
        long deadline = System.currentTimeMillis() + WAIT_TIMEOUT_MILLIS;
        Map<Snapshot, List<String>> tags = savepointTags(table);
        while (tags.isEmpty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(200);
            tags = savepointTags(table);
        }
        assertThat(tags).describedAs("no savepoint tag was created").isNotEmpty();
        return tags;
    }

    private Map<Snapshot, List<String>> savepointTags(FileStoreTable table) {
        return table.tagManager()
                .tags(
                        name ->
                                name.startsWith(
                                        AutoTagForSavepointCommitterOperator.SAVEPOINT_TAG_PREFIX));
    }

    /** Emits one row per poll so every checkpoint window carries data. */
    private static class ContinuousSource extends AbstractNonCoordinatedSource<RowData> {
        private static final long serialVersionUID = 1L;

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.CONTINUOUS_UNBOUNDED;
        }

        @Override
        public SourceReader<RowData, SimpleSourceSplit> createReader(SourceReaderContext ctx) {
            return new AbstractNonCoordinatedSourceReader<RowData>() {
                private int next;

                @Override
                public InputStatus pollNext(ReaderOutput<RowData> output)
                        throws InterruptedException {
                    output.collect(
                            GenericRowData.of(next, StringData.fromString("v" + next)));
                    next++;
                    Thread.sleep(20);
                    return InputStatus.MORE_AVAILABLE;
                }
            };
        }
    }
}
