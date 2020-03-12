package com.on;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;

public class NormalCbtTest extends CbtTest {

  public NormalCbtTest(String projectId, String instanceId, String tableId) {
    super(projectId, instanceId, tableId);
  }

  public static void main(String[] args) throws Exception {
    if (System.getenv("GOOGLE_CLOUD_ENABLE_DIRECT_PATH").equals("bigtable")) {
      System.out.println("env variable GOOGLE_CLOUD_ENABLE_DIRECT_PATH shouldn't be bigtable");
      System.exit(1);
    }
    String projectId = "directpath-prod-manual-testing";
    String instanceId = "jihuncho-rls";
    String tableId = "test-table";
    new NormalCbtTest(projectId, instanceId, tableId + "2").run();
  }

  @Override
  public BigtableDataClient dataClient() throws Exception {
    BigtableDataSettings settings =
        BigtableDataSettings.newBuilder().setProjectId(projectId).setInstanceId(instanceId).build();
    return BigtableDataClient.create(settings);
  }

  @Override
  public BigtableTableAdminClient adminClient() throws Exception {
    BigtableTableAdminSettings adminSettings =
        BigtableTableAdminSettings.newBuilder()
            .setProjectId(projectId)
            .setInstanceId(instanceId)
            .build();
    return BigtableTableAdminClient.create(adminSettings);
  }
}
