package com.on;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import javax.annotation.Nullable;

public class DirectPathCbtTest extends CbtTest {

  public DirectPathCbtTest(String projectId, String instanceId, String tableId) {
    super(projectId, instanceId, tableId);
  }

  public static void main(String[] args) throws Exception {
    if(System.getenv("PROJECT") == null) {
	    System.out.println("PROJECT environment variable is not set.");
	    System.exit(1);
    }
    String projectId = System.getenv("PROJECT");
    String instanceId = "mycluster";
    String tableId = "test-table";
    new DirectPathCbtTest(projectId, instanceId, tableId + "2").run();
  }


  @Override
  public BigtableDataClient dataClient() throws Exception {
    BigtableDataSettings.Builder settingsBuilder = BigtableDataSettings.newBuilder();
    InstantiatingGrpcChannelProvider transportProvider =
        (InstantiatingGrpcChannelProvider)
            settingsBuilder.stubSettings().setEndpoint("test-bigtable.sandbox.googleapis.com:443")
                .getTransportChannelProvider();
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
    BigtableTableAdminSettings.Builder adminSettingsBuilder =
        BigtableTableAdminSettings.newBuilder()
            .setProjectId(projectId)
            .setInstanceId(instanceId)
            .setCredentialsProvider(
                FixedCredentialsProvider.create(ComputeEngineCredentials.create()));
        adminSettingsBuilder.stubSettings().setEndpoint("test-bigtableadmin.sandbox.googleapis.com:443");
    return BigtableTableAdminClient.create(adminSettingsBuilder.build());
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
        ImmutableList.of(grpcLbPolicy),
        "childPolicyConfigTargetFieldName",
        "serviceName");
    ImmutableMap<String, ?> lbConfig = ImmutableMap.of(
        "rls-experimental", rlsConfig);
    return ImmutableMap.of("loadBalancingConfig", ImmutableList.of(lbConfig));
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
                    true)),
	    "extraKeys", ImmutableMap.of("host", "server", "service","service","method","method"));
    ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<String, Object>()
        .put("grpcKeyBuilders", ImmutableList.of(grpcKeyBuilders))
        .put("lookupService", "test-bigtablerls.sandbox.googleapis.com")
        .put("lookupServiceTimeout", 2D)
        .put("maxAge", 300D)
        .put("staleAge", 240D)
        .put(
            "validTargets",
            ImmutableList.of(
		 "us-central1-test-bigtable.sandbox.googleapis.com",
		    "europe-north1-test-bigtable.sandbox.googleapis.com",
		    "us-east1-test-bigtable.sandbox.googleapis.com",
                "test-bigtable.sandbox.googleapis.com"))
        .put("cacheSizeBytes", 1000D)
        .put("requestProcessingStrategy", "SYNC_LOOKUP_DEFAULT_TARGET_ON_ERROR");
    if (defaultTarget != null) {
      builder.put("defaultTarget", defaultTarget);
    }
    return builder.build();
  }
}
