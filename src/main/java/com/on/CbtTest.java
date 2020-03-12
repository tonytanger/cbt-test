/*
 * Copyright 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.on;

import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.RowMutation;

/**
 * Test simple CRUD in different mode (directpath, rls, normal, etc).
 *
 * <p>This example is a very simple "hello world" application, that illustrates how to create a new
 * table, write to the table, read the data back, and delete the table.
 *
 * <ul>
 *   <li>create table
 *   <li>read single row
 *   <li>read table
 *   <li>delete table
 * </ul>
 */
public abstract class CbtTest {

  private static final String COLUMN_FAMILY = "cf1";
  private static final String COLUMN_QUALIFIER = "greeting";
  private static final String ROW_KEY_PREFIX = "rowKey";
  protected final String projectId;
  protected final String instanceId;
  protected final String tableId;

  public CbtTest(String projectId, String instanceId, String tableId) {
    this.projectId = projectId;
    this.instanceId = instanceId;
    this.tableId = tableId;
  }

  public final void run() throws Exception {
    before();
    BigtableDataClient dataClient = dataClient();
    BigtableTableAdminClient adminClient = adminClient();
    performCrud(dataClient, adminClient);
    after();
  }

  public void before() {}

  public void after() {}

  public abstract BigtableDataClient dataClient() throws Exception;

  public abstract BigtableTableAdminClient adminClient() throws Exception;

  public void performCrud(
      BigtableDataClient dataClient, BigtableTableAdminClient adminClient) {
    try {
      createTable(adminClient);
      writeToTable(dataClient);
      readSingleRow(dataClient);
      readTable(dataClient);
      deleteTable(adminClient);
    } finally {
      dataClient.close();
      adminClient.close();
    }
  }

  /** Demonstrates how to create a table. */
  public void createTable(BigtableTableAdminClient adminClient) {
    // [START bigtable_hw_create_table_veneer]
    // Checks if table exists, creates table if does not exist.
    if (!adminClient.exists(tableId)) {
      System.out.println("Creating table: " + tableId);
      CreateTableRequest createTableRequest =
          CreateTableRequest.of(tableId).addFamily(COLUMN_FAMILY);
      adminClient.createTable(createTableRequest);
      System.out.printf("Table %s created successfully%n", tableId);
    }
    // [END bigtable_hw_create_table_veneer]
  }

  /** Demonstrates how to write some rows to a table. */
  public void writeToTable(BigtableDataClient dataClient) {
    // [START bigtable_hw_write_rows_veneer]
    try {
      System.out.println("\nWriting some greetings to the table");
      String[] greetings = {"Hello World!", "Hello Bigtable!", "Hello Java!"};
      for (int i = 0; i < greetings.length; i++) {
        RowMutation rowMutation =
            RowMutation.create(tableId, ROW_KEY_PREFIX + i)
                .setCell(COLUMN_FAMILY, COLUMN_QUALIFIER, greetings[i]);
        dataClient.mutateRow(rowMutation);
        System.out.println(greetings[i]);
      }
    } catch (NotFoundException e) {
      System.err.println("Failed to write to non-existent table: " + e.getMessage());
    }
    // [END bigtable_hw_write_rows_veneer]
  }

  /** Demonstrates how to read a single row from a table. */
  public void readSingleRow(BigtableDataClient dataClient) {
    // [START bigtable_hw_get_by_key_veneer]
    try {
      System.out.println("\nReading a single row by row key");
      Row row = dataClient.readRow(tableId, ROW_KEY_PREFIX + 0);
      System.out.println("Row: " + row.getKey().toStringUtf8());
      for (RowCell cell : row.getCells()) {
        System.out.printf(
            "Family: %s    Qualifier: %s    Value: %s%n",
            cell.getFamily(), cell.getQualifier().toStringUtf8(), cell.getValue().toStringUtf8());
      }
    } catch (NotFoundException e) {
      System.err.println("Failed to read from a non-existent table: " + e.getMessage());
    }
    // [END bigtable_hw_get_by_key_veneer]
  }

  /** Demonstrates how to read an entire table. */
  public void readTable(BigtableDataClient dataClient) {
    // [START bigtable_hw_scan_all_veneer]
    try {
      System.out.println("\nReading the entire table");
      Query query = Query.create(tableId);
      ServerStream<Row> rowStream = dataClient.readRows(query);
      for (Row r : rowStream) {
        System.out.println("Row Key: " + r.getKey().toStringUtf8());
        for (RowCell cell : r.getCells()) {
          System.out.printf(
              "Family: %s    Qualifier: %s    Value: %s%n",
              cell.getFamily(), cell.getQualifier().toStringUtf8(), cell.getValue().toStringUtf8());
        }
      }
    } catch (NotFoundException e) {
      System.err.println("Failed to read a non-existent table: " + e.getMessage());
    }
    // [END bigtable_hw_scan_all_veneer]
  }

  /** Demonstrates how to delete a table. */
  public void deleteTable(BigtableTableAdminClient adminClient) {
    // [START bigtable_hw_delete_table_veneer]
    System.out.println("\nDeleting table: " + tableId);
    try {
      adminClient.deleteTable(tableId);
      System.out.printf("Table %s deleted successfully%n", tableId);
    } catch (NotFoundException e) {
      System.err.println("Failed to delete a non-existent table: " + e.getMessage());
    }
    // [END bigtable_hw_delete_table_veneer]
  }
}

