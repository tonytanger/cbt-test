package com.on;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;

public class NormalCbtTest extends CbtTest {

  public NormalCbtTest(String projectId, String instanceId, String tableId) {
    super(projectId, instanceId, tableId);
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
