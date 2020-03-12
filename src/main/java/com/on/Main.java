package com.on;

import java.util.ArrayList;
import java.util.List;

public class Main {

  public static void main(String[] args) {
    String projectId = "directpath-prod-manual-testing";
    String instanceId = "jihuncho-rls";
    String tableId = "test-table";

    List<CbtTest> tests = new ArrayList<>();
    tests.add(new DirectPathCbtTest(projectId, instanceId, tableId + "2"));
    tests.add(new NormalCbtTest(projectId, instanceId, tableId + "1"));

    for (CbtTest test : tests) {
      try {
        test.run();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

}
