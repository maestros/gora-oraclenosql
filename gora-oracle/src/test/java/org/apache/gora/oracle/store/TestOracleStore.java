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
import java.util.*;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

/**
 * Test case for OracleNoSQLStore.
 * @author Apostolos Giannakidis
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
    dumpTables(tables);
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    String[] tables = {"Employee", "WebPage", "PrimaryKeys"};
    dumpTables(tables);
  }

  /**
   * Utility method that dumps the contents of the database tables.
   * @param tables the tables to be dumped.
   */
  private void dumpTables(String[] tables){
    log.debug("start: dumpTables");
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

        log.debug(kvv.getKey().toString()+":"+new String(kvv.getValue().getValue()));
        records++;
      }

      majorComponents.remove(table);
    }

    if (records==0)
      log.debug("no records found.");

    log.debug("end: dumpTables");
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

  /**
   * Asserts that the given schema name exists in the database.
   * @param schemaName the schema name to be checked
   * @throws Exception
   */
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

  /**
   * Asserts that the put() method works as expected by checking
   * that the given employee object is correctly persisted.
   * @param employee
   * @throws IOException
   */
  @Override
  public void assertPut(Employee employee) throws IOException {
    employeeStore.put(employee.getSsn().toString(),employee);
    employeeStore.flush();
    assertTrue(((OracleStore) employeeStore).exists(employee));
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

  /**
   * Asserts that the array that was persisted by the testPutArray()
   * have been persisted as expected.
   * @throws IOException
   */
  @Override
  public void assertPutArray() throws IOException {
    //set the major key components for retrieval of the correct record
    List<String> majorComponents = new ArrayList<String>();
    majorComponents.add("WebPage");
    majorComponents.add(OracleUtil.encodeKey("com.example/http"));

    byte[] bytes; //byte array to store the retrieved bytes

    //get the bytes directly from the Oracle NoSQL database
    bytes = getTestDriver().get(Key.createKey(majorComponents, "parsedContent"));

    //check that the bytes that were retrieved are not null
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
    majorComponents.add(OracleUtil.encodeKey("com.example/http"));

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

  /**
   * Asserts that the map that was persisted by the testPutMap()
   * have been persisted as expected.
   * @throws IOException
   */
  @Override
  public void assertPutMap() throws IOException {
    //get the WebPage that was created in testPutMap()
    WebPage page = webPageStore.get("com.example/http") ;

    //get its outlinks
    Map<Utf8,Utf8> outlinks = page.getOutlinks();

    //check that outlinks map is not null
    assertNotNull(outlinks);

    //get the value from the outlinks map with the key "http://example2.com"
    String anchor2 = outlinks.get(new Utf8("http://example2.com")).toString();

    //check that it is the same with the one that was added in testPutMap()
    assertEquals("anchor2", anchor2);
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
  @Override
  public void testGetRecursive() throws IOException, Exception {
    super.testGetRecursive();
  }

  @Test
  @Override
  public void testGetDoubleRecursive() throws IOException, Exception {
    super.testGetDoubleRecursive();
  }

  @Test
  @Override
  public void testGetNested() throws IOException, Exception {
    super.testGetNested();
  }

  @Test
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

  /**
   * Assert that dataStore.delete(T) works as expected.
   * @throws Exception
   */
  @Test
  public void testDeletePersistentObject() throws Exception {

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

  /**
   * Checks that when writing a top level union <code>['null','type']</code> the value is written in raw format.
   * @throws Exception
   */
  @Test
  public void assertTopLevelUnions() throws Exception {
    WebPage page = webPageStore.newPersistent();

    // Write webpage data
    page.setUrl(new Utf8("http://example.com"));
    byte[] contentBytes = "example content in example.com".getBytes();
    ByteBuffer buff = ByteBuffer.wrap(contentBytes);
    page.setContent(buff);
    webPageStore.put("com.example/http", page);
    webPageStore.flush() ;

    // set the major key components for retrieval of the correct record
    List<String> majorComponents = new ArrayList<String>();
    majorComponents.add("WebPage");
    majorComponents.add(OracleUtil.encodeKey("com.example/http"));

    byte[] actualBytes; //byte array to store the retrieved bytes

    //get the actualBytes directly from the Oracle NoSQL database
    Key key = Key.createKey(majorComponents, "content");
    actualBytes = getTestDriver().get(key);

    assertNotNull(actualBytes);
    assertTrue(Arrays.equals(actualBytes, contentBytes));
  }

  /**
   * Checks that when writing a top level union <code>['null','type']
   * the <code>null</code> value is treated properly.
   * @throws Exception
   */
  @Test
  public void assertTopLevelUnionsNull() throws Exception {
    WebPage page = webPageStore.newPersistent();

    // Write webpage data
    page.setUrl(new Utf8("http://example.com"));
    page.setContent(null);     // This won't change internal field status to dirty, so
    page.setDirty("content") ; // need to change it manually
    webPageStore.put("com.example/http", page);
    webPageStore.flush() ;

    // Read directly from Oracle NoSQL
    // set the major key components for retrieval of the correct record
    List<String> majorComponents = new ArrayList<String>();
    majorComponents.add("WebPage");
    majorComponents.add(OracleUtil.encodeKey("com.example/http"));

    byte[] actualBytes; //byte array to store the retrieved bytes

    //get the actualBytes directly from the Oracle NoSQL database
    Key key = Key.createKey(majorComponents, "content");
    actualBytes = getTestDriver().get(key);

    assertTrue(actualBytes == null || actualBytes.length == 0) ;
  }

  @Test
  @Ignore
  @Override
  public void testGetPartitions() throws IOException, Exception {
    super.testGetPartitions();
  }
}