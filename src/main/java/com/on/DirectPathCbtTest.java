package com.on;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.stub.EnhancedBigtableStubSettings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import javax.annotation.Nullable;

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
    String instanceId = "zdapeng-rls";
    String tableId = "test-table";
    new DirectPathCbtTest(projectId, instanceId, tableId + "2").run();
  }

  @Override
  public void before() {
    System.setProperty(
        "bigtable.directpath-data-endpoint", "testdirectpath-bigtable.googleapis.com:443");
    System.setProperty("bigtable.directpath-admin-endpoint", "test-bigtableadmin.googleapis.com:443");
  }

  @Override
  public void after() {
    System.clearProperty("bigtable.directpath-data-endpoint");
    System.clearProperty("bigtable.directpath-admin-endpoint");
  }

  @Override
  public BigtableDataClient dataClient() throws Exception {
    BigtableDataSettings.Builder settingsBuilder = BigtableDataSettings.newBuilder();
    InstantiatingGrpcChannelProvider transportProvider =
        (InstantiatingGrpcChannelProvider)
            settingsBuilder.stubSettings().getTransportChannelProvider();
    transportProvider = transportProvider.toBuilder()
        .setDirectPathServiceConfig(getRlsServiceConfig(null))
        .build();

    settingsBuilder
        .stubSettings().setTransportChannelProvider(transportProvider)
        .setProjectId(projectId)
        .setInstanceId(instanceId)
        .setCredentialsProvider(
            FixedCredentialsProvider.create(ComputeEngineCredentials.create()));
    System.out.println("Client Settings: " + settingsBuilder.build());
    return BigtableDataClient.create(settingsBuilder.build());
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

  private static ImmutableMap<String, ?> getRlsServiceConfig(@Nullable String defaultTarget) {
    ImmutableMap<String, ?> pickFirstStrategy = ImmutableMap.of("pick_first", ImmutableMap.of());
    ImmutableMap<String, ?> childPolicy =
        ImmutableMap.of("childPolicy", ImmutableList.of(pickFirstStrategy));
    ImmutableMap<String, ?> grpcLbPolicy = ImmutableMap.of("grpclb", childPolicy);
    ImmutableMap<String, ?> rlsConfig = ImmutableMap.of(
        "routeLookupConfig",
        getLookupConfig(defaultTarget),
        "childPolicy",
        grpcLbPolicy,
        "childPolicyConfigTargetFieldName",
        "serviceName");
    ImmutableMap<String, ?> lbConfig = ImmutableMap.of(
        "rls-experimental", rlsConfig);
    return ImmutableMap.of("loadBalancingConfig", lbConfig);
  }

  private static ImmutableMap<String, ?> getLookupConfig(@Nullable String defaultTarget) {
    ImmutableMap<String, ?> grpcKeyBuilders =
        ImmutableMap.of(
            "names",
            ImmutableList.of(
                ImmutableMap.of("service", "google.bigtable.v2.Bigtable")),
            "headers",
            ImmutableList.of(
                ImmutableMap.of(
                    "key",
                    "x-goog-request-params",
                    "names",
                    ImmutableList.of("x-goog-request-params"),
                    "optional",
                    true),
                ImmutableMap.of(
                    "key",
                    "google-cloud-resource-prefix",
                    "names",
                    ImmutableList.of("google-cloud-resource-prefix"),
                    "optional",
                    true)));
    ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<String, Object>()
        .put("grpcKeyBuilders", grpcKeyBuilders)
        .put("lookupService", "test-bigtablerls.sandbox.googleapis.com")
        .put("lookupServiceTimeout", 2D)
        .put("maxAge", 300D)
        .put("staleAge", 240D)
        .put(
            "validTargets",
            ImmutableList.of(
                "test-bigtable.sandbox.googleapis.com",
                "testdirectpath-bigtable.sandbox.googleapis.com"))
        .put("cacheSizeBytes", 1000D)
        .put("requestProcessingStrategy", "SYNC_LOOKUP_DEFAULT_TARGET_ON_ERROR");
    if (defaultTarget != null) {
      builder.put("defaultTarget", defaultTarget);
    }
    return builder.build();
  }
}
