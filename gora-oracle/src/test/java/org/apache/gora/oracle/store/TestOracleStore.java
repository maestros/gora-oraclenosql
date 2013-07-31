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

package org.apache.gora.oracle.store;

import oracle.kv.Direction;
import oracle.kv.KeyValueVersion;
import oracle.kv.Value;
import org.apache.gora.examples.generated.Employee;
import org.apache.gora.examples.generated.WebPage;
import org.apache.gora.oracle.GoraOracleTestDriver;
import org.apache.gora.oracle.util.OracleUtil;
import org.apache.gora.store.DataStore;
import org.apache.gora.store.DataStoreFactory;
import org.apache.gora.store.DataStoreTestBase;
import org.apache.hadoop.conf.Configuration;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import oracle.kv.Key;

/**
 * Test case for OracleNoSQLStore.
 */
public class TestOracleStore extends DataStoreTestBase {

  public static final Logger log = LoggerFactory.getLogger(TestOracleStore.class);
  private Configuration conf;

  static {
    setTestDriver(new GoraOracleTestDriver());
  }

  @SuppressWarnings("unchecked")
  @Deprecated
  @Override
  protected DataStore<String, Employee> createEmployeeDataStore() throws IOException {
    return DataStoreFactory.createDataStore(OracleStore.class, String.class,
            Employee.class, conf);
  }

  @SuppressWarnings("unchecked")
  @Deprecated
  @Override
  protected DataStore<String, WebPage> createWebPageDataStore() throws IOException {
    return DataStoreFactory.createDataStore(OracleStore.class, String.class,
            WebPage.class, conf);
  }

  public GoraOracleTestDriver getTestDriver() {
    return (GoraOracleTestDriver) testDriver;
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();

    String[] tables = {"Employee", "WebPage"};
    dumpDB(tables);
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

  private void dumpDB(String[] tables){
    log.info("start: dumpDB");
    ArrayList<String> majorComponents = new ArrayList<String>();

    for (String table: tables){
      majorComponents.add(table);

      Key myKey = Key.createKey(majorComponents);

      Iterator<KeyValueVersion> i =
                getTestDriver().getKvstore().storeIterator(Direction.UNORDERED, 0,
                        myKey, null, null);

      while (i.hasNext()) {
        KeyValueVersion kvv =  i.next();

        log.info(kvv.getKey().toString()+":"+new String(kvv.getValue().getValue()));
      }

      majorComponents.remove(table);
    }

    log.info("end: dumpDB");
  }

  @Test
  @Ignore
  @Override
  public void testNewInstance() throws IOException, Exception {
    super.testNewInstance();
  }

  public void populateTestSchema() {

    String[] keyPathStrings = {"/Employee",
            "/Employee/1",
            "/Employee/1/-/info",
            "/Employee/1/-/info/fullname",
            "/Employee/2",
            "/Employee/2/-/info",
            "/Employee/2/-/info/fullname",
            "/Webpage"};

    for(String keyPathString : keyPathStrings){
      System.out.println(keyPathString);
      Key fullKey = Key.fromString(keyPathString);
      getTestDriver().getKvstore().put(fullKey, Value.EMPTY_VALUE);
    }
  }

  @Test
  @Ignore
  @Override
  public void testCreateSchema() throws Exception {
    super.testCreateSchema();
  }

  @Ignore
  @Override
  public void assertSchemaExists(String schemaName) throws Exception {
    super.assertSchemaExists(schemaName);
  }

  @Test
  @Ignore
  @Override
  public void testAutoCreateSchema() throws Exception {
    super.testAutoCreateSchema();
  }

  @Test
  @Ignore
  @Override
  public void assertAutoCreateSchema() throws Exception {
    super.assertAutoCreateSchema();
  }

  @Test
  @Ignore
  @Override
  public void testTruncateSchema() throws Exception {
    super.testTruncateSchema();
  }

  @Test
  @Ignore
  @Override
  public void testDeleteSchema() throws IOException, Exception {
    super.testDeleteSchema();
  }

  @Test
  @Ignore
  @Override
  public void testSchemaExists() throws Exception {
    super.testSchemaExists();
  }

  @Test
  @Ignore
  @Override
  public void testPut() throws IOException, Exception {
    super.testPut();
  }

  @Ignore
  @Override
  public void assertPut(Employee employee) throws IOException {
    super.assertPut(employee);
  }

  @Test
  @Ignore
  @Override
  public void testPutNested() throws IOException, Exception {
    super.testPutNested();
  }

  @Test
  @Ignore
  @Override
  public void testPutArray() throws IOException, Exception {
    super.testPutArray();
  }

  @Test
  @Ignore
  @Override
  public void assertPutArray() throws IOException {
    super.assertPutArray();
  }

  /**
   * Asserts that writing bytes actually works.
   * TODO: Check writing null unions too.
   */
  @Override
  public void assertPutBytes(byte[] contentBytes) throws IOException {

    log.info("assertPutBytes was called");

    String fields[] = {"content"};

   // final byte[] actualBytes = webPageStore.get("com.example/http/",fields).getContent().array();

    List<String> majorComponents = new ArrayList<String>();
    majorComponents.add("WebPage");
    majorComponents.add("com.example");
    majorComponents.add("http");

    //get the actualBytes directly from the Oracle NoSQL
    final byte[] actualBytes = getTestDriver().getKvstore().get(Key.createKey(majorComponents, "content")).getValue().getValue();

    assertNotNull(actualBytes);
    assertTrue(Arrays.equals(contentBytes, actualBytes));
    /*
    log.info("assert null");

    WebPage page = webPageStore.get("com.example/http") ;
    page.setContent(null) ;
    webPageStore.put("com.example/http", page) ;
    webPageStore.close() ;

    webPageStore = testDriver.createDataStore(String.class, WebPage.class);
    page = webPageStore.get("com.example/http") ;
    assertNull(page.getContent()) ;
           */
  }

  @Test
  @Ignore
  @Override
  public void testPutMap() throws IOException, Exception {
    super.testPutMap();
  }

  @Test
  @Ignore
  @Override
  public void assertPutMap() throws IOException {
    super.assertPutMap();
  }

  @Test
  @Ignore
  @Override
  public void testUpdate() throws IOException, Exception {
    super.testUpdate();
  }

  @Test
  @Ignore
  @Override
  public void testEmptyUpdate() throws IOException, Exception {
    super.testEmptyUpdate();
  }

  @Test
  @Ignore
  @Override
  public void testGet() throws IOException, Exception {
    super.testGet();
  }

  @Test
  @Ignore
  @Override
  public void testGetRecursive() throws IOException, Exception {
    super.testGetRecursive();
  }

  @Test
  @Ignore
  @Override
  public void testGetDoubleRecursive() throws IOException, Exception {
    super.testGetDoubleRecursive();
  }

  @Test
  @Ignore
  @Override
  public void testGetNested() throws IOException, Exception {
    super.testGetNested();
  }

  @Test
  @Ignore
  @Override
  public void testGet3UnionField() throws IOException, Exception {
    super.testGet3UnionField();
  }

  @Test
  @Ignore
  @Override
  public void testGetWithFields() throws IOException, Exception {
    super.testGetWithFields();
  }

  @Test
  @Ignore
  @Override
  public void testGetWebPage() throws IOException, Exception {
    super.testGetWebPage();
  }

  @Test
  @Ignore
  @Override
  public void testGetWebPageDefaultFields() throws IOException, Exception {
    super.testGetWebPageDefaultFields();
  }

  @Test
  @Ignore
  @Override
  public void testGetNonExisting() throws Exception, Exception {
    super.testGetNonExisting();
  }

  @Test
  @Ignore
  @Override
  public void testQuery() throws IOException, Exception {
    super.testQuery();
  }

  @Test
  @Ignore
  @Override
  public void testQueryStartKey() throws IOException, Exception {
    super.testQueryStartKey();
  }

  @Test
  @Ignore
  @Override
  public void testQueryEndKey() throws IOException, Exception {
    super.testQueryEndKey();
  }

  @Test
  @Ignore
  @Override
  public void testQueryKeyRange() throws IOException, Exception {
    super.testQueryKeyRange();
  }

  @Test
  @Ignore
  @Override
  public void testQueryWebPageSingleKey() throws IOException, Exception {
    super.testQueryWebPageSingleKey();
  }

  @Test
  @Ignore
  @Override
  public void testQueryWebPageSingleKeyDefaultFields() throws IOException, Exception {
    super.testQueryWebPageSingleKeyDefaultFields();
  }

  @Test
  @Ignore
  @Override
  public void testQueryWebPageQueryEmptyResults() throws IOException, Exception {
    super.testQueryWebPageQueryEmptyResults();
  }

  @Test
  @Ignore
  @Override
  public void testDelete() throws IOException, Exception {
    super.testDelete();
  }

  @Test
  @Ignore
  @Override
  public void testDeleteByQuery() throws IOException, Exception {
    super.testDeleteByQuery();
  }

  @Test
  @Ignore
  @Override
  public void testDeleteByQueryFields() throws IOException, Exception {
    super.testDeleteByQueryFields();
  }

  @Test
  @Ignore
  @Override
  public void testGetPartitions() throws IOException, Exception {
    super.testGetPartitions();
  }
}