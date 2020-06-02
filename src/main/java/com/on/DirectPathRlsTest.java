package com.on;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.grpc.ManagedChannel;
import io.grpc.alts.ComputeEngineChannelBuilder;
import io.grpc.lookup.v1.RouteLookupRequest;
import io.grpc.lookup.v1.RouteLookupResponse;
import io.grpc.lookup.v1.RouteLookupServiceGrpc;
import java.util.Map;

public class DirectPathRlsTest {

  public static void main(String[] args) {
    String target = "test-bigtablerls.sandbox.googleapis.com";
    System.out.println("connecting to target: " + target);
    ManagedChannel channel =
        ComputeEngineChannelBuilder.forTarget(target)
            .defaultServiceConfig(serviceConfig())
            .disableServiceConfigLookUp()
            .build();

    System.out.println("Channel state: " + channel.getState(true));

    RouteLookupResponse response =
        RouteLookupServiceGrpc
            .newBlockingStub(channel)
            .routeLookup(
                RouteLookupRequest.newBuilder()
                    .setTargetType("grpc")
                    .setPath("/google.bigtable.v2.Bigtable/ReadRows")
                    .setServer("bigtable.googleapis.com")
                    .putKeyMap("x-goog-request-params", "table_name=projects/directpath-prod-manual-testing/instances/jihuncho-rls/tables/test-table&app_profile=default")
                    .build());

    System.out.println("Response: " + response);
  }

  private static Map<String, Object> serviceConfig() {
    ImmutableMap<String, Object> pickFirstStrategy =
        ImmutableMap.of("pick_first", ImmutableMap.of());

    ImmutableMap<String, Object> childPolicy =
        ImmutableMap.of("childPolicy", ImmutableList.of(pickFirstStrategy));

    ImmutableMap<String, Object> grpcLbPolicy =
        ImmutableMap.of("grpclb", childPolicy);

    ImmutableMap<String, Object> loadBalancingConfig =
        ImmutableMap.of("loadBalancingConfig", ImmutableList.of(grpcLbPolicy));

    System.out.println("Service Config: " + loadBalancingConfig);
    return loadBalancingConfig;
  }
}
