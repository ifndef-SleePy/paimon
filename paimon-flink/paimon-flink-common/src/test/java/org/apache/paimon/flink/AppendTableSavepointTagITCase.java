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
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end savepoint auto-tag tests for unaware-bucket append tables, parameterized over the two
 * commit paths (coordinator-commit and the classic global committer). Both must create the same
 * {@code savepoint-<checkpointId>} tag for a triggered savepoint.
 */
public class AppendTableSavepointTagITCase {

    // The savepoint tag only materializes once a checkpoint *after* the savepoint completes and
    // cumulatively commits the savepoint's snapshot (same catch-up as the classic path). Give the
    // poll generous headroom so a transient checkpoint stall under load cannot trip the assertion.
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
    public void testSavepointCreatesTag(boolean coordinatorCommit) throws Exception {
        TableEnvironment tEnv =
                TableEnvironment.create(
                        EnvironmentSettings.newInstance().inStreamingMode().build());
        tEnv.getConfig().getConfiguration().setString("execution.checkpointing.interval", "200 ms");

        tEnv.executeSql(
                "CREATE CATALOG mycat WITH ( 'type' = 'paimon', 'warehouse' = '"
                        + tempPath
                        + "' )");
        tEnv.executeSql("USE CATALOG mycat");

        String tableName = coordinatorCommit ? "T_COORD" : "T_CLASSIC";
        // force-create-snapshot ensures every completed checkpoint yields a snapshot, so the
        // savepoint's snapshot (and thus its tag, created when a later checkpoint completes and
        // cumulatively materializes it) appears without depending on new data arriving.
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
                        + "'commit.force-create-snapshot' = 'true'"
                        + coordinatorOption
                        + ")");
        tEnv.executeSql(
                "CREATE TEMPORARY TABLE src (id INT, data STRING) WITH ("
                        + "'connector' = 'datagen', "
                        + "'rows-per-second' = '20')");

        FileStoreTable table =
                (FileStoreTable)
                        ((FlinkCatalog) tEnv.getCatalog("mycat").get())
                                .catalog()
                                .getTable(Identifier.create("default", tableName));

        TableResult tableResult =
                tEnv.executeSql("INSERT INTO " + tableName + " SELECT * FROM src");
        JobClient client = tableResult.getJobClient().get();
        try {
            // Wait until the job has committed at least one snapshot before triggering a savepoint,
            // so the intended commit path is actually running.
            waitUntilSnapshotCommitted(table);

            client.triggerSavepoint(
                            tempPath + "/savepoint_" + tableName, SavepointFormatType.DEFAULT)
                    .get(60, TimeUnit.SECONDS);

            // Poll until exactly one savepoint-prefixed tag appears, then assert it is consistent
            // with the snapshot it points at.
            Map<Snapshot, List<String>> savepointTags = waitUntilSavepointTagCreated(table);
            assertThat(savepointTags).hasSize(1);
            Map.Entry<Snapshot, List<String>> entry = savepointTags.entrySet().iterator().next();
            Snapshot tagged = entry.getKey();
            assertThat(entry.getValue())
                    .containsExactly(
                            AutoTagForSavepointCommitterOperator.SAVEPOINT_TAG_PREFIX
                                    + tagged.commitIdentifier());
            assertThat(table.snapshotManager().snapshotExists(tagged.id())).isTrue();
        } finally {
            client.cancel().get(30, TimeUnit.SECONDS);
        }
    }

    private void waitUntilSnapshotCommitted(FileStoreTable table) throws Exception {
        long deadline = System.currentTimeMillis() + WAIT_TIMEOUT_MILLIS;
        while (System.currentTimeMillis() < deadline) {
            if (table.snapshotManager().latestSnapshot() != null) {
                return;
            }
            Thread.sleep(200);
        }
        assertThat(table.snapshotManager().latestSnapshot())
                .describedAs("no snapshot was committed before triggering savepoint")
                .isNotNull();
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
}
