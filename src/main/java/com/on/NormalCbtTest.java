package com.on;

import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;

public class NormalCbtTest extends CbtTest {

  public NormalCbtTest(String projectId, String instanceId, String tableId) {
    super(projectId, instanceId, tableId);
  }

  public static void main(String[] args) throws Exception {
    String projectId = System.getenv("PROJECT");
    String instanceId = "mycluster";
    String tableId = "test-table";
    new NormalCbtTest(projectId, instanceId, tableId + "3").run();
  }

  @Override
  public BigtableDataClient dataClient() throws Exception {
    BigtableDataSettings.Builder settingsBuilder = BigtableDataSettings.newBuilder();
    InstantiatingGrpcChannelProvider transportProvider =
        (InstantiatingGrpcChannelProvider)
            settingsBuilder.stubSettings().setEndpoint("test-bigtable.sandbox.googleapis.com:443")
                .getTransportChannelProvider();
    settingsBuilder
        .stubSettings().setTransportChannelProvider(transportProvider)
        .setProjectId(projectId)
        .setInstanceId(instanceId);
    return BigtableDataClient.create(settingsBuilder.build());
  }

  @Override
  public BigtableTableAdminClient adminClient() throws Exception {
	  BigtableTableAdminSettings.Builder adminSettingsBuilder =
        BigtableTableAdminSettings.newBuilder()
            .setProjectId(projectId)
            .setInstanceId(instanceId);
        adminSettingsBuilder.stubSettings().setEndpoint("test-bigtableadmin.sandbox.googleapis.com:443");
    return BigtableTableAdminClient.create(adminSettingsBuilder.build());
  }
}
