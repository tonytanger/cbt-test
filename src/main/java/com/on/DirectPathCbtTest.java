package com.on;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;

public class DirectPathCbtTest extends CbtTest {

  public DirectPathCbtTest(String projectId, String instanceId, String tableId) {
    super(projectId, instanceId, tableId);
  }

  public static void main(String[] args) throws Exception {
    if (!System.getenv("GOOGLE_CLOUD_ENABLE_DIRECT_PATH").equals("bigtable")) {
      System.out.println("set env variable GOOGLE_CLOUD_ENABLE_DIRECT_PATH to bigtable");
      System.exit(1);
    }
    String projectId = "directpath-prod-manual-testing";
    String instanceId = "jihuncho-rls";
    String tableId = "test-table";
    new DirectPathCbtTest(projectId, instanceId, tableId + "2").run();
  }

  @Override
  public void before() {
    System.setProperty(
        "bigtable.directpath-data-endpoint", "directpath-bigtable.googleapis.com:443");
    System.setProperty("bigtable.directpath-admin-endpoint", "bigtableadmin.googleapis.com:443");
  }

  @Override
  public void after() {
    System.clearProperty("bigtable.directpath-data-endpoint");
    System.clearProperty("bigtable.directpath-admin-endpoint");
  }

  @Override
  public BigtableDataClient dataClient() throws Exception {
    BigtableDataSettings settings =
        BigtableDataSettings.newBuilder()
            .setProjectId(projectId)
            .setInstanceId(instanceId)
            .setCredentialsProvider(
                FixedCredentialsProvider.create(ComputeEngineCredentials.create()))
            .build();
    return BigtableDataClient.create(settings);
  }

  @Override
  public BigtableTableAdminClient adminClient() throws Exception {
    BigtableTableAdminSettings adminSettings =
        BigtableTableAdminSettings.newBuilder()
            .setProjectId(projectId)
            .setInstanceId(instanceId)
            .setCredentialsProvider(
                FixedCredentialsProvider.create(ComputeEngineCredentials.create()))
            .build();
    return BigtableTableAdminClient.create(adminSettings);
  }
}
