/**
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

/**
 * Testing class for all standard gora-cassandra functionality.
 * We extend DataStoreTestBase enabling us to run the entire base test
 * suite for Gora. 
 */
package org.apache.gora.cassandra.store;

import java.io.IOException;

import org.apache.gora.cassandra.GoraCassandraTestDriver;
import org.apache.gora.examples.generated.Employee;
import org.apache.gora.examples.generated.WebPage;
import org.apache.gora.store.DataStore;
import org.apache.gora.store.DataStoreFactory;
import org.apache.gora.store.DataStoreTestBase;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Ignore;

/**
 * Test for CassandraStore.
 */
public class TestCassandraStore extends DataStoreTestBase{

  private Configuration conf;

  static {
    setTestDriver(new GoraCassandraTestDriver());
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

  @SuppressWarnings("unchecked")
  @Override
  protected DataStore<String, Employee> createEmployeeDataStore() throws IOException {
    return DataStoreFactory.getDataStore(CassandraStore.class, String.class, Employee.class, conf);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected DataStore<String, WebPage> createWebPageDataStore() throws IOException {
    return DataStoreFactory.getDataStore(CassandraStore.class, String.class, WebPage.class, conf);
  }

  public GoraCassandraTestDriver getTestDriver() {
    return (GoraCassandraTestDriver) testDriver;
  }


// ============================================================================
    //We need to skip the following tests for a while until we fix some issues..

  @Ignore("skipped until some bugs are fixed")
  @Override
  public void testGetWebPageDefaultFields() throws IOException {}
  @Ignore("skipped until some bugs are fixed")
  @Override
  public void testQuery() throws IOException {}
  @Ignore("skipped until some bugs are fixed")
  @Override
  public void testQueryStartKey() throws IOException {}
  @Ignore("skipped until some bugs are fixed")
  @Override
  public void testQueryEndKey() throws IOException {}
  @Ignore("skipped until some bugs are fixed")
  @Override
  public void testQueryKeyRange() throws IOException {}
  @Ignore("skipped until some bugs are fixed")
  @Override
  public void testQueryWebPageSingleKeyDefaultFields() throws IOException {}
  @Ignore("skipped until some bugs are fixed")
  @Override
  public void testDelete() throws IOException {}
  @Ignore("skipped until some bugs are fixed")
  @Override
  public void testDeleteByQuery() throws IOException {}
  @Ignore("skipped until some bugs are fixed")
  @Override
  public void testDeleteByQueryFields() throws IOException {}
  @Ignore("skipped until some bugs are fixed")
  @Override
  public void testGetPartitions() throws IOException {}
  @Ignore("skipped until some bugs are fixed")
  @Override
  public void testGetRecursive() throws IOException {}
  @Ignore("skipped until some bugs are fixed")
  @Override
  public void testGetDoubleRecursive() throws IOException{}
  @Ignore("skipped until some bugs are fixed")
  @Override
  public void testGetNested() throws IOException {}
  @Ignore("skipped until some bugs are fixed")
  @Override
  public void testGet3UnionField() throws IOException {}
// ============================================================================


  public static void main(String[] args) throws Exception {
    TestCassandraStore test = new TestCassandraStore();
    setUpClass();
    test.setUp();

    test.tearDown();
    tearDownClass();
  }
}
