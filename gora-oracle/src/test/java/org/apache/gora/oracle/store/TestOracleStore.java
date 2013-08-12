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

import oracle.kv.*;
import org.apache.avro.util.Utf8;
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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

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

    String[] tables = {"Employee", "WebPage", "PrimaryKeys"};
    dumpDB(tables);
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    String[] tables = {"Employee", "WebPage", "PrimaryKeys"};
    dumpDB(tables);
  }

  private void dumpDB(String[] tables){
    log.info("start: dumpDB");
    ArrayList<String> majorComponents = new ArrayList<String>();

    int records=0;
    for (String table: tables){
      majorComponents.add(table);

      Key myKey = Key.createKey(majorComponents);

      Iterator<KeyValueVersion> i =
                getTestDriver().getKvstore().storeIterator(Direction.UNORDERED, 0,
                        myKey, null, null);

      while (i.hasNext()) {
        KeyValueVersion kvv =  i.next();

        log.info(kvv.getKey().toString()+":"+new String(kvv.getValue().getValue()));
        records++;
      }

      majorComponents.remove(table);
    }

    if (records==0)
      log.info("no records found.");

    log.info("end: dumpDB");
  }

  @Test
  @Override
  public void testAutoCreateSchema() throws Exception {
    super.testAutoCreateSchema();
  }

  @Test
  @Override
  public void testTruncateSchema() throws Exception {
    super.testTruncateSchema();
  }

  @Test
  @Override
  public void testDeleteSchema() throws IOException, Exception {
    super.testDeleteSchema();
  }

  @Test
  @Override
  public void testSchemaExists() throws Exception {
    super.testSchemaExists();
  }

  @Override
  public void assertSchemaExists(String schemaName) throws Exception {

    //create the key that will be used to lookup the schema
    Key key = Key.createKey(schemaName);

    //use the Oracle NoSQL kvstore directly to get the key/value pair
    ValueVersion vv = getTestDriver().getKvstore().get(key);

    //assert that get() did not return null
    assertTrue(vv != null);
  }

  @Test
  @Override
  public void testPut() throws IOException, Exception {
    super.testPut();
  }

  @Test
  @Override
  public void testPutNested() throws IOException, Exception {
    super.testPutNested();
  }

  @Test
  @Override
  public void testPutArray() throws IOException, Exception {
    super.testPutArray();
  }

  @Override
  public void assertPutArray() throws IOException {
    // set the major key components for retrieval of the correct record
    List<String> majorComponents = new ArrayList<String>();
    majorComponents.add("WebPage");
    majorComponents.add("com.example");
    majorComponents.add("http");

    byte[] bytes; //byte array to store the retrieved bytes

    //get the bytes directly from the Oracle NoSQL database
    bytes = getTestDriver().get(Key.createKey(majorComponents, "parsedContent"));

    // check that the bytes that were retrieved are not null
    assertNotNull(bytes);

    String[] tokens = {"example", "content", "in", "example.com"};

    WebPage page = webPageStore.get("com.example/http") ;

    assertEquals(page.getParsedContent().size(), 4);

    //assert that all retrieved tokens are correct
    Iterator<Utf8> iter = page.getParsedContent().iterator();
    int i=0;
    while(iter.hasNext()){
      String token = iter.next().toString();
      //retrieved token equals as the one that was added
      assertTrue(token.equals(tokens[i]));
      i++;
    }
  }


  /**
   * Asserts that writing bytes actually works.
   * It also checks the cases of empty bytes and null; this is
   * because of the union type of WebPage.content avro schema.
   */
  @Override
  public void assertPutBytes(byte[] contentBytes) throws IOException {

    // set the major key components for retrieval of the correct record
    List<String> majorComponents = new ArrayList<String>();
    majorComponents.add("WebPage");
    majorComponents.add("com.example");
    majorComponents.add("http");

    byte[] actualBytes; //byte array to store the retrieved bytes

    //get the actualBytes directly from the Oracle NoSQL database
    actualBytes = getTestDriver().get(Key.createKey(majorComponents, "content"));

    /* check that the contentBytes (provided by the testPutBytes())
     * is equal to the actualBytes (retrieved directly from the database).
     */
    assertNotNull(actualBytes);
    assertTrue(Arrays.equals(contentBytes, actualBytes));

    // check the case of null content
    WebPage page = webPageStore.get("com.example/http") ;
    page.setContent(null) ;
    webPageStore.put("com.example/http", page) ;
    webPageStore.flush();
    webPageStore = testDriver.createDataStore(String.class, WebPage.class);
    page = webPageStore.get("com.example/http") ;
    assertNull(page.getContent()) ;

    //get the actualBytes directly from the Oracle NoSQL database
    actualBytes = getTestDriver().get(Key.createKey(majorComponents, "content"));
    assertNull(actualBytes) ;

    // check the case of an empty string content
    page = webPageStore.get("com.example/http") ;
    page.setContent(ByteBuffer.wrap("".getBytes())) ;
    webPageStore.put("com.example/http", page) ;
    webPageStore.flush();
    webPageStore = testDriver.createDataStore(String.class, WebPage.class);
    page = webPageStore.get("com.example/http") ;
    assertTrue(Arrays.equals("".getBytes(),page.getContent().array())) ;
  }

  @Test
  @Override
  public void testPutMap() throws IOException, Exception {
    super.testPutMap();
  }

  @Test
  @Override
  public void testUpdate() throws IOException, Exception {
    super.testUpdate();
  }

  @Test
  @Override
  public void testEmptyUpdate() throws IOException, Exception {
    super.testEmptyUpdate();
  }

  @Test
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
  @Override
  public void testGetWithFields() throws IOException, Exception {
    super.testGetWithFields();
  }

  @Test
  @Override
  public void testGetWebPage() throws IOException, Exception {
    super.testGetWebPage();
  }

  @Test
  @Override
  public void testGetWebPageDefaultFields() throws IOException, Exception {
    super.testGetWebPageDefaultFields();
  }

  @Test
  @Override
  public void testGetNonExisting() throws Exception, Exception {
    super.testGetNonExisting();
  }

  @Test
  @Override
  public void testQuery() throws IOException, Exception {
    super.testQuery();
  }

  @Test
  @Override
  public void testQueryStartKey() throws IOException, Exception {
    super.testQueryStartKey();
  }

  @Test
  @Override
  public void testQueryEndKey() throws IOException, Exception {
    super.testQueryEndKey();
  }

  @Test
  @Override
  public void testQueryKeyRange() throws IOException, Exception {
    super.testQueryKeyRange();
  }

  @Test
  @Override
  public void testQueryWebPageSingleKey() throws IOException, Exception {
    super.testQueryWebPageSingleKey();
  }

  @Test
  @Override
  public void testQueryWebPageSingleKeyDefaultFields() throws IOException, Exception {
    super.testQueryWebPageSingleKeyDefaultFields();
  }

  @Test
  @Override
  public void testQueryWebPageQueryEmptyResults() throws IOException, Exception {
    super.testQueryWebPageQueryEmptyResults();
  }

  @Test
  @Override
  public void testDelete() throws IOException, Exception {
    super.testDelete();
  }

  @Test
  @Override
  public void testDeleteByQuery() throws IOException, Exception {
    super.testDeleteByQuery();
  }

  /* Until GORA-66 is resolved this test will always fail.
   * The problem relies on the different semantics of the exclusiveness
   * of the endKey. We ignore this test for the time being.
   */
  @Ignore("skipped until GORA-66 is resolved")
  @Test
  @Override
  public void testDeleteByQueryFields() throws IOException, Exception {
    super.testDeleteByQueryFields();
  }

  @Test
  public void testDeletePersistentObject() throws IOException, Exception {

    OracleStore<String,WebPage> dataStore = (OracleStore)getTestDriver().createDataStore(String.class,WebPage.class);

    WebPage webPage;

    webPage = dataStore.get("www.google.com");

    assertNull(webPage);

    ByteBuffer content = ByteBuffer.wrap("sample content".getBytes());

    webPage = dataStore.newPersistent();
    webPage.setUrl(new Utf8(("www.google.com")));
    webPage.setContent(content);

    assertNotNull(webPage);

    dataStore.put(webPage.getUrl().toString(),webPage);
    dataStore.flush();

    webPage = null;

    webPage = dataStore.get("www.google.com");
    assertNotNull(webPage);
    assertArrayEquals(webPage.getContent().array(), content.array());

    dataStore.delete(webPage);
    dataStore.flush();

    webPage = dataStore.get("www.google.com");
    assertNull(webPage);
  }

  @Test
  @Ignore
  @Override
  public void testGetPartitions() throws IOException, Exception {
    super.testGetPartitions();
  }
}