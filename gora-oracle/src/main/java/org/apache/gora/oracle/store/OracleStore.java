/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.apache.gora.oracle.query.OracleQuery;
import org.apache.gora.oracle.query.OracleResult;
import org.apache.gora.oracle.store.OracleMapping.OracleMappingBuilder;
import org.apache.gora.oracle.util.OracleUtil;

import oracle.kv.*;
import org.apache.gora.persistency.StateManager;
import org.apache.gora.persistency.StatefulHashMap;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.query.PartitionQuery;
import org.apache.gora.query.Query;
import org.apache.gora.query.Result;
import org.apache.gora.store.DataStoreFactory;
import org.apache.gora.store.impl.DataStoreBase;
import org.apache.gora.util.IOUtils;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author Apostolos Giannakidis
 */
public class OracleStore<K,T extends PersistentBase> extends DataStoreBase<K,T> {

  /**
   * Helper to write useful information into the logs
   */
  private static final Logger LOG = LoggerFactory.getLogger(OracleStore.class);

  /**
   * The mapping file to create the tables from
   */
  private static final String DEFAULT_MAPPING_FILE = "gora-oracle-mapping.xml";

  private static final String DURABILITY_SYNCPOLICY = "gora.oracle.durability.syncpolicy";
  private static final String DURABILITY_REPLICAACKPOLICY = "gora.oracle.durability.replicaackpolicy";
  private static final String CONSISTENCY = "gora.oracle.consistency";
  private static final String TIME_UNIT = "gora.oracle.time.unit";
  private static final String REQUEST_TIMEOUT = "gora.oracle.request.timeout";
  private static final String READ_TIMEOUT = "gora.oracle.read.timeout";
  private static final String OPEN_TIMEOUT = "gora.oracle.open.timeout";
  private static final String STORE_NAME = "gora.oracle.storename";
  private static final String HOST_NAME = "gora.oracle.hostname";
  private static final String HOST_PORT = "gora.oracle.hostport";
  private static final String PRIMARYKEY_TABLE_NAME = "gora.oracle.primarykey_tablename";

  private static final String DEFAULT_STORE_NAME = "kvstore";
  private static final String DEFAULT_HOST_NAME = "localhost";
  private static final String DEFAULT_HOST_PORT = "5000";
  private static final String DEFAULT_PRIMARYKEY_TABLE_NAME = "PrimaryKeys";

  private volatile OracleMapping mapping; //the mapping to the datastore

  private final boolean autoCreateSchema = false;

  /*
   * Variables and references to Oracle NoSQL properties
   * and configuration values.
   */
  private static String storeName;  //the name of the oracle kv store
  private static String hostName;   //the name of the host to connect (could be the IP)
  private static String hostPort;   //the port of the oracle kv store to connect to
  private static String mappingFile;  //the filename of the mapping (xml) file
  private static String primaryKeyTable;  //the name of the table that stores the primary keys

  private static int readTimeout;
  private static int openTimeout;
  private static int requestTimeout;

  private static Durability.ReplicaAckPolicy durabilityReplicaAckPolicy;
  private static Durability.SyncPolicy durabilitySyncPolicy;
  private static Consistency consistency;
  private static TimeUnit timeUnit;

  /*
     Set of operations to be executed during flush().
     It is a LinkedHashSet in order to retain the order in which
     each operation was added to the collection.
   */
  LinkedHashSet<List<Operation>> operations;

  private KVStore kvstore;  //reference to the Oracle NoSQL datastore
  private KVStoreConfig conf; //handle to get and set the configuration of the Oracle NoSQL datastore

  /**
   * Initialize the data store by initialising the operations, setting the client (kvstore),
   * setting the client's properties up, and reading the mapping file
   */
  @Override
  public void initialize(Class<K> keyClass, Class<T> persistentClass, Properties properties) {
    super.initialize(keyClass, persistentClass, properties);

    if ( (mapping != null) && (kvstore != null) && (conf != null) ){
      LOG.warn("OracleStore is already initialised");
      return;
    }

    if (properties==null)
      LOG.info("Error: Properties was not found!");
    else
      LOG.info("Properties found");

    operations = new LinkedHashSet();

    readProperties(properties);

    setupClient();

    try {
      LOG.info("mappingFile="+mappingFile);
      mapping = readMapping( mappingFile );
    }
    catch ( IOException e ) {
      LOG.error( e.getMessage() );
      LOG.error( e.getStackTrace().toString() );
    }

    if(autoCreateSchema) {
      createSchema();
    }
  }

  /**
   * Sets the configuration for the client according to the properties
   * and establishes a new connection to the Oracle NoSQL datastore
   */
  private void setupClient(){

    conf = new KVStoreConfig(storeName, hostName + ":" + hostPort);

    conf.setRequestTimeout(requestTimeout, timeUnit);
    conf.setSocketReadTimeout(readTimeout, timeUnit);
    conf.setSocketOpenTimeout(openTimeout, timeUnit);
    conf.setConsistency(consistency);
    Durability newDurability = new Durability(durabilitySyncPolicy,    // Master sync
            durabilitySyncPolicy, // Replica sync
            durabilityReplicaAckPolicy);
    conf.setDurability(newDurability);

    kvstore = KVStoreFactory.getStore(conf);
  }

  /**
   * Reads the properties file, parses it and stores the values
   * to the static variables and references
   */
  private void readProperties(Properties properties) {

    mappingFile = DataStoreFactory.getMappingFile(properties, this, DEFAULT_MAPPING_FILE);
    storeName = DataStoreFactory.findProperty(properties, this, STORE_NAME, DEFAULT_STORE_NAME);
    hostName = DataStoreFactory.findProperty(properties, this, HOST_NAME, DEFAULT_HOST_NAME);
    hostPort = DataStoreFactory.findProperty(properties, this, HOST_PORT, DEFAULT_HOST_PORT);
    primaryKeyTable = DataStoreFactory.findProperty(properties, this, PRIMARYKEY_TABLE_NAME, DEFAULT_PRIMARYKEY_TABLE_NAME);

    try{
      requestTimeout = Integer.parseInt(DataStoreFactory.findProperty( properties, this, REQUEST_TIMEOUT, String.valueOf(KVStoreConfig.DEFAULT_REQUEST_TIMEOUT)));
    }
    catch ( NumberFormatException nfe ) {
      requestTimeout = KVStoreConfig.DEFAULT_REQUEST_TIMEOUT;
      LOG.warn( "Invalid requestTimeout value. Using default " + String.valueOf(KVStoreConfig.DEFAULT_REQUEST_TIMEOUT) );
    }

    try{
      readTimeout = Integer.parseInt(DataStoreFactory.findProperty( properties, this, READ_TIMEOUT, String.valueOf(KVStoreConfig.DEFAULT_READ_TIMEOUT)));
    }
    catch ( NumberFormatException nfe ) {
      readTimeout = KVStoreConfig.DEFAULT_READ_TIMEOUT;
      LOG.warn( "Invalid readTimeout value. Using default " + String.valueOf(KVStoreConfig.DEFAULT_READ_TIMEOUT) );
    }

    try{
      openTimeout = Integer.parseInt(DataStoreFactory.findProperty( properties, this, OPEN_TIMEOUT, String.valueOf(KVStoreConfig.DEFAULT_OPEN_TIMEOUT)));
    }
    catch ( NumberFormatException nfe ) {
      openTimeout = KVStoreConfig.DEFAULT_OPEN_TIMEOUT;
      LOG.warn( "Invalid openTimeout value. Using default " + String.valueOf(KVStoreConfig.DEFAULT_OPEN_TIMEOUT) );
    }

    durabilityReplicaAckPolicy = Durability.ReplicaAckPolicy.valueOf(DataStoreFactory.findProperty( properties, this, DURABILITY_REPLICAACKPOLICY, Durability.ReplicaAckPolicy.SIMPLE_MAJORITY.name() ));
    durabilitySyncPolicy = Durability.SyncPolicy.valueOf(DataStoreFactory.findProperty( properties, this, DURABILITY_SYNCPOLICY, Durability.SyncPolicy.WRITE_NO_SYNC.name() ));

    String tmpConsistency = DataStoreFactory.findProperty( properties, this, CONSISTENCY, Consistency.NONE_REQUIRED.getName() );

    if (tmpConsistency.equals("NONE_REQUIRED"))
      consistency = Consistency.NONE_REQUIRED;
    else if (tmpConsistency.equals("ABSOLUTE"))
      consistency = Consistency.ABSOLUTE;
    else{
      consistency = Consistency.NONE_REQUIRED;
      LOG.debug("Consistency was set to default.");
    }

    String tmpTimeUnit = DataStoreFactory.findProperty( properties, this, TIME_UNIT, "MILLISECONDS" );

    if (tmpTimeUnit.equals("DAYS"))
      timeUnit = TimeUnit.DAYS;
    else if (tmpTimeUnit.equals("HOURS"))
      timeUnit = TimeUnit.HOURS;
    else if (tmpTimeUnit.equals("MICROSECONDS"))
      timeUnit = TimeUnit.MICROSECONDS;
    else if (tmpTimeUnit.equals("MILLISECONDS"))
      timeUnit = TimeUnit.MILLISECONDS;
    else if (tmpTimeUnit.equals("MINUTES"))
      timeUnit = TimeUnit.MINUTES;
    else if (tmpTimeUnit.equals("NANOSECONDS"))
      timeUnit = TimeUnit.NANOSECONDS;
    else if (tmpTimeUnit.equals("SECONDS"))
      timeUnit = TimeUnit.SECONDS;
    else{
      LOG.error("timeUnit was invalid.");
      throw new IllegalStateException();
    }

  }

  /**
   * Reads the schema file and converts it into a data structure to be used
   * @param mappingFilename The schema file to be mapped into a table
   * @return OracleMapping  Object containing all necessary information to create tables
   * @throws IOException
   */
  private OracleMapping readMapping(String mappingFilename) throws IOException {

    OracleMappingBuilder mappingBuilder = new OracleMapping.OracleMappingBuilder();

    try {
      SAXBuilder builder = new SAXBuilder();
      Document doc = builder.build( getClass().getClassLoader().getResourceAsStream( mappingFilename ) );

      List<Element> classes = doc.getRootElement().getChildren( "class" );

      for ( Element classElement : classes ) {

        if ( classElement.getAttributeValue( "keyClass" ).equals( keyClass.getCanonicalName() )
                && classElement.getAttributeValue( "name" ).equals( persistentClass.getCanonicalName() ) ) {

          String tableName = getSchemaName( classElement.getAttributeValue( "table" ), persistentClass );
          mappingBuilder.setTableName( tableName );

          mappingBuilder.setClassName( classElement.getAttributeValue( "name" ) );
          mappingBuilder.setKeyClass( classElement.getAttributeValue( "keyClass" ) );

          Element primaryKeyEl = classElement.getChild( "primarykey" );

          String primaryKeyField = primaryKeyEl.getAttributeValue( "name" );
          String primaryKeyColumn = primaryKeyEl.getAttributeValue( "column" );

          mappingBuilder.setPrimaryKey( primaryKeyField );
          mappingBuilder.addField( primaryKeyField, primaryKeyColumn );

          List<Element> fields = classElement.getChildren( "field" );

          for ( Element field : fields ) {
            String fieldName = field.getAttributeValue( "name" );
            String columnName = field.getAttributeValue( "column" );

            mappingBuilder.addField( fieldName, columnName );
          }
          break;
        }

      }

    }
    catch ( Exception ex ) {
      LOG.info("Error in parsing");
      throw new IOException( ex );
    }

    return mappingBuilder.build();
  }


  /**
   * //TODO the javadoc
   * @return
   */
  public static String getPrimaryKeyTable() {
    return primaryKeyTable;
  }

  /**
   * Gets the schema name
   */
  @Override
  public String getSchemaName() {
    return mapping.getTableName();
  }

  /**
   * //TODO the javadoc
   */
  @Override
  public void createSchema() {

    if (schemaExists()){
      LOG.info("Schema: "+mapping.getMajorKey()+" already exists");
      return;
    }

    int tries=0;
    while (tries<2){
      try {
        kvstore.put(mapping.getMajorKey(), Value.EMPTY_VALUE);
        tries=2;
        LOG.info("Schema: "+mapping.getMajorKey()+" was created successfully");
      } catch (DurabilityException de) {
        // The durability guarantee could not be met.
        if (tries==1)
          LOG.error( de.getMessage(), de.getStackTrace().toString() );
        else {
          LOG.warn("DurabilityException occurred. Retrying one more time after 200 ms.");
          try {
            Thread.sleep(200);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          tries++;
          continue;
        }
      } catch (RequestTimeoutException rte) {
        // The operation was not completed inside of the
        // default request timeout limit.

        if (tries==1)
          LOG.error( rte.getMessage(), rte.getStackTrace().toString() );
        else {
          LOG.warn("RequestTimeoutException occurred. Retrying one more time after 200 ms.");
          try {
            Thread.sleep(200);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          tries++;
          continue;
        }

      } catch (FaultException fe) {
        // A generic error occurred

        if (tries==1)
          LOG.error( fe.getMessage(), fe.getStackTrace().toString() );
        else {
          LOG.warn("FaultException occurred. Retrying one more time after 200 ms.");
          try {
            Thread.sleep(200);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          tries++;
          continue;
        }

      }
    }

  }

  /**
   * //TODO the javadoc
   */
  @Override
  public void deleteSchema() {

    LOG.info("deleteSchema was called.");

    if (!schemaExists())
      return;

    int tries=0;
    while (tries<2){
      try {

        // Efficiently delete a subtree of keys using multiple major keys

        while (true){
          Iterator<Key> i = kvstore.storeKeysIterator
                  (Direction.UNORDERED, 1, mapping.getMajorKey(),
                          null, Depth.PARENT_AND_DESCENDANTS);
          if (!i.hasNext()) {
            break;
          }
          Key descendant = Key.createKey(i.next().getMajorPath());
          LOG.info("Deleting: "+descendant.toString());
          kvstore.multiDelete(descendant, null,
                  Depth.PARENT_AND_DESCENDANTS);
        }

       // while (true){
          List<String> primaryKeys = new ArrayList<String>();
          primaryKeys.add(OracleStore.getPrimaryKeyTable());
          primaryKeys.add(mapping.getTableName());

          Key primary = Key.createKey(primaryKeys);
          LOG.info("Deleting: "+primary.toString());
          kvstore.multiDelete(primary, null,
                  Depth.PARENT_AND_DESCENDANTS);
       // }

        tries=2;

      } catch (DurabilityException de) {
        // The durability guarantee could not be met.
        if (tries==1)
          LOG.error( de.getMessage(), de.getStackTrace().toString() );
        else {
          LOG.warn("DurabilityException occurred. Retrying one more time after 200 ms.");
          try {
            Thread.sleep(200);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          tries++;
          continue;
        }
      } catch (RequestTimeoutException rte) {
        // The operation was not completed inside of the
        // default request timeout limit.

        if (tries==1)
          LOG.error( rte.getMessage(), rte.getStackTrace().toString() );
        else {
          LOG.warn("RequestTimeoutException occurred. Retrying one more time after 200 ms.");
          try {
            Thread.sleep(200);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          tries++;
          continue;
        }

      } catch (FaultException fe) {
        // A generic error occurred

        if (tries==1)
          LOG.error( fe.getMessage(), fe.getStackTrace().toString() );
        else {
          LOG.warn("FaultException occurred. Retrying one more time after 200 ms.");
          try {
            Thread.sleep(200);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          tries++;
          continue;
        }

      }
    }
    LOG.info("deleteSchema finished.");
  }

  /**
   * Checks if the schema exists or not
   * @return true or false depending on whether the schema exists or not
   */
  @Override
  public boolean schemaExists() {
    return kvstore.get(mapping.getMajorKey())!=null ? true : false;
  }

  /**
   * //TODO the javadoc
   * @param result
   * @param fields
   * @return
   * @throws IOException
   */
  public T newInstance(SortedMap<Key, ValueVersion> result, String[] fields)
          throws IOException {
    if(result == null || result.isEmpty())
      return null;

    LOG.info("newInstance");
    T persistent = newPersistent();

    /*
      if no fields are specified,
      then retrieve all fields.
     */
    if ( fields == null ) {
      fields = fieldMap.keySet().toArray( new String[fieldMap.size()] );
    }

    StateManager stateManager = persistent.getStateManager();
    ByteBuffer bb;
    Set<String> fieldsSet = new HashSet<String>();
    Collections.addAll(fieldsSet, fields);

    for (Map.Entry<Key, ValueVersion> entry : result.entrySet()) {
			/* If fields is null, read all fields */
      String field = getFieldFromKey(entry.getKey());
      if (!fieldsSet.isEmpty() && !fieldsSet.contains(field)) {
        //if field retrieved is not contained in the the specified field set
        //then skip this field (thus, do not include it in the new Persistent)
        LOG.info("field:"+field+" not in fieldset. skipped.");
        continue;
      }

      Schema.Field persistentField = fieldMap.get(field);

      byte[] val = entry.getValue().getValue().getValue();
      if (val == null) {
        LOG.info("Field: "+field+" was skipped because its value was null.");
        continue;
      }

      LOG.info("field: "+persistentField.name()+", schema: "+persistentField.schema().getType());

      Object v = null;

      switch (persistentField.schema().getType()){
        case LONG:
          bb = ByteBuffer.wrap(val);
          persistent.put(persistentField.pos(), bb.getLong());
          break;
        case INT:
          bb = ByteBuffer.wrap(val);
          persistent.put(persistentField.pos(), bb.getInt());
          break;
        case BYTES:
          bb = ByteBuffer.wrap(val);
          persistent.put(persistentField.pos(),  bb);
          break;
        case STRING:
          persistent.put(persistentField.pos(), new Utf8(val));
          break;
        case MAP:
          v = IOUtils.deserialize((byte[]) val, datumReader, persistentField.schema(), persistent.get(persistentField.pos()));
          Map map = (StatefulHashMap) v;
          persistent.put( persistentField.pos(), map );
          break;
        case ARRAY:
        case RECORD:
          v = IOUtils.deserialize((byte[]) val, datumReader, persistentField.schema(), persistent.get(persistentField.pos()));
          persistent.put( persistentField.pos(), v );
          break;
        case UNION:
            bb = ByteBuffer.wrap(val);
            persistent.put(persistentField.pos(),  bb);
          break;
        default:
          LOG.info("Type not considered: " + persistentField.schema().getType().name());
      }

    }

    stateManager.clearDirty(persistent);
    return persistent;
  }

  /**
   * //TODO the javadocs
   * @param key
   * @return
   */
  private static String getFieldFromKey(Key key) {
    List<String> minorPath = key.getMinorPath();

    // get the last minor key (which represents the field)
    return minorPath.get(minorPath.size() - 1);
  }

  /**
   * //TODO the javadoc
   * @param key the key of the object
   * @param fields the fields required in the object. Pass null, to retrieve all fields
   * @return
   */
  @Override
  public T get(K key, String[] fields) {

    LOG.info("inside get");

    // trivial check for a non-null key
    if (key==null)
      return null;

    Key myKey = OracleUtil.createTableKey((String) key, mapping.getTableName());

    SortedMap<Key, ValueVersion> kvResult;
    try {
      kvResult = kvstore.multiGet(myKey, null, null);
    } catch (FaultException e) {
      LOG.error("The operation cannot be completed: "+e);
      return null;
    }

    LOG.info("multiGet size: "+kvResult.size());

    if (kvResult.size()==0)
      return null;

    T return_object = null;
    try {
      return_object = newInstance(kvResult, fields);
    } catch (IOException e) {
      e.printStackTrace();
    }

    return return_object;
  }

  /**
   * //TODO the javadoc
   * @param key
   * @param persistent
   */
  @Override
  public void put(K key, T persistent) {

    LOG.info("inside put");

    Schema schema = persistent.getSchema();
    StateManager stateManager = persistent.getStateManager();

    //LOG.info(schema.getField(mapping.getPrimaryKey()));

    if ( !stateManager.isDirty( persistent ) ) {
      // nothing to do
      LOG.info("is not dirty");
      return;
    }

    LOG.info("is dirty");

    List<Schema.Field> fields = schema.getFields();

    List<Operation> opList = new ArrayList<Operation>();
    OperationFactory of = kvstore.getOperationFactory();

    /*
      Add the key to the list of primary keys, for easy access
     */
    List<String> majorComponentsForParent = new ArrayList<String>();
    majorComponentsForParent.add(OracleStore.getPrimaryKeyTable());
    majorComponentsForParent.add(mapping.getTableName());
    Key primaryKey = Key.createKey(majorComponentsForParent, (String) key);

    opList.add(of.createPut(primaryKey, Value.EMPTY_VALUE));
    LOG.info("Added primary key:"+primaryKey);
    operations.add(opList);
    opList = new ArrayList<Operation>();

    //List for majorComponents for the Persistent
    ArrayList<String> majorComponents = new ArrayList<String>();

    // Define the major and minor path components for the key
    majorComponents.add(mapping.getTableName());
    majorComponents.add(key.toString());// keys in Oracle NoSQL are strings

    for ( Schema.Field field : fields ) {
      Object value = persistent.get( field.pos() );
      Schema fieldSchema = field.schema();

      // Create the key
      // The field name will be part of the minor components of the key
      Key oracleKey = OracleUtil.createKey(majorComponents, mapping.getColumn(field.name()));

      LOG.info("fieldSchema="+fieldSchema.getType());

      // in case the value is null then delete the key
      if (value==null){
        LOG.info("value==null");
        of = kvstore.getOperationFactory();
        opList.add(of.createDelete(oracleKey)); //delete the key
      }
      else{
        LOG.info("value!=null");

        // Create the value
        Value oracleValue = OracleUtil.createValue(value, fieldSchema, datumWriter);
        of = kvstore.getOperationFactory();
        opList.add(of.createPut(oracleKey, oracleValue));
      }
    }

    LOG.info("Added a put operation for key: "+majorComponents.get(0)+"/"+majorComponents.get(1));
    operations.add(opList);
  }

  /**
   * //TODO the javadoc
   * @return
   */
  private String[] getAllPersistentFields(){
    String[] fields = null;
    try {
      Field field = beanFactory.getPersistentClass().getDeclaredField("_ALL_FIELDS");
      field.setAccessible(true);
      fields = (String[])field.get(null);
    } catch (NoSuchFieldException e1) {
      e1.printStackTrace();
    } catch (IllegalAccessException e1) {
      e1.printStackTrace();
    }

    return fields;
  }

  /**
   * //TODO the javadoc
   * @return
   */
  private K getKeyFromPersistent(T obj){
    LOG.info("inside getKeyFromPersistent");
    K key = null;
    try {
      LOG.info("Primary key field:"+mapping.getPrimaryKey());
      Field field = obj.getClass().getDeclaredField(mapping.getPrimaryKey());
      field.setAccessible(true);
      Utf8 primary_key = (Utf8)field.get(obj);
      key = (K)primary_key.toString();
    } catch (NoSuchFieldException e1) {
      e1.printStackTrace();
    } catch (IllegalAccessException e1) {
      e1.printStackTrace();
    }

    return key;
  }

  /**
   * Deletes a persistent object from the database.
   * It deletes all its fields and its primary key.
   * The object that is deleted is depended on the primary key field
   * that was specified in the mapping.
   * @param obj the persistent object to delete
   * @return true if the object was deleted, false otherwise
   */
  public boolean delete(T obj) {
    LOG.info("Inside delete()");

    K key = getKeyFromPersistent(obj);

    LOG.info("Key:"+key);
    Query<K,T> query = newQuery();
    query.setKey(key);
    long rowsDeleted = deleteByQuery(query);

    if (rowsDeleted>1)
      LOG.warn("Warning: Single key:"+key+" deleted "+rowsDeleted+" records (primary keys).");

    return rowsDeleted > 0;
  }

  /**
   * //TODO the javadoc
   * @param key the key of the object
   * @return
   */
  @Override
  public boolean delete(K key) {
    LOG.info("Inside delete(). Key:"+key);
    Query<K,T> query = newQuery();
    query.setKey(key);
    long rowsDeleted = deleteByQuery(query);

    if (rowsDeleted>1)
      LOG.warn("Warning: Single key:"+key+" deleted "+rowsDeleted+" records (primary keys).");

    LOG.info("finished deleting.");
    return  rowsDeleted > 0;
  }

  /**
   * //TODO the javadoc
   * @param query matching records to this query will be deleted
   * @return
   */
  @Override
  public long deleteByQuery(Query<K, T> query) {

    LOG.info("inside deleteByQuery()");

    if (((OracleQuery) query).isExecuted()){
      LOG.info("query has already been executed.");
      return 0;
    }

    List<Operation> opList;
    OperationFactory of = kvstore.getOperationFactory();
    Result<K, T> result = this.execute(query);
    int recordsDeleted = 0;

    try {

      while (result.next()) {

        String[] fields = query.getFields();

        if (fields!=null){
          LOG.info("deleteByQuery. fields to be deleted:"+fields.length);

          if (Arrays.equals(fields, getAllPersistentFields())){
            LOG.info("Arrays.equals");
            //Delete all the fields associated with the persistent
            //along with its primary key.
            opList = deleteRecord((String) result.getKey());
            recordsDeleted++;
            operations.add(opList);
          }
          else{
            LOG.info("Arrays not equal");
            opList = new ArrayList<Operation>();
            for (String field : fields){
              deleteFieldFromRecord((String) result.getKey(), field, opList);
              LOG.info("Deleted field:" + field);
              recordsDeleted++;
            }
            operations.add(opList);
          }
        }
        else{
          LOG.info("fields==null");
          //Delete all the fields associated with the persistent
          //along with its primary key.
          opList = deleteRecord((String) result.getKey());
          recordsDeleted++;
          operations.add(opList);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

    try {
      result.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

    LOG.info("recordsDeleted="+recordsDeleted);
    return recordsDeleted;
  }

  /**
   * Deletes specific field from a specific persistent object.
   * To delete the field, it deletes its corresponding  key/value pair.
   * @param key the key to identify the persistent object.
   * @param field the name of the field to delete.
   * @param opList the list of operations in which the delete operation will be added.
   */
  public void deleteFieldFromRecord(String key, String field, List<Operation> opList){
    OperationFactory of = kvstore.getOperationFactory();


    List<String> majorKeyComponents = new ArrayList<String>();
    majorKeyComponents.add(mapping.getTableName());
    majorKeyComponents.add(key);

    Key oracleKey = OracleUtil.createKey(majorKeyComponents, field);

    LOG.info("Field to be deleted: "+oracleKey.toString());
    //Delete the field
    opList.add(of.createDelete(oracleKey));
  }

  /**
   * Deletes a persistent object from the Oracle NoSQL database.
   * It deletes all its fields and the primary key.
   * @param key the key to retrieve the fields and the primary key
   * @return a list of Operations to be executed with flush() is called.
   */
  private List<Operation> deleteRecord(String key){
    List<Operation> opList = new ArrayList<Operation>();
    OperationFactory of = kvstore.getOperationFactory();
    Key oracleKey = OracleUtil.createTableKey(key, mapping.getTableName());
    LOG.info("Key to be deleted: "+key.toString());
    kvstore.multiDelete(oracleKey, null,
            Depth.PARENT_AND_DESCENDANTS);

    //Delete the primary key
    List<String> primaryKeyComponents = new ArrayList<String>();
    primaryKeyComponents.add(OracleStore.getPrimaryKeyTable());
    primaryKeyComponents.add(mapping.getTableName());
    oracleKey = Key.createKey(primaryKeyComponents, key);
    LOG.info("Primary Key to be deleted: "+oracleKey.toString());
    opList.add(of.createDelete(oracleKey));
    return opList;
  }

  /**
   * Executes the given query and returns the results.
   * @param query the query to execute.
   * @return the results as a {@link OracleResult} object.
   * @throws IOException
   */
  @Override
  public Result<K, T> execute(Query<K, T> query) {
    //TODO

    LOG.info("inside execute()");

    if (((OracleQuery) query).isExecuted())
      return ((OracleQuery) query).getResult();

    OracleResult result;
    String startkey = (String) query.getStartKey();
    String endkey = (String) query.getEndKey();
    String setKey = (String) query.getKey();

    LOG.info("startkey="+startkey);
    LOG.info("endkey="+endkey);

    /*
      in case startkey == endkey then
      create a new OracleResult without an iterator
      in order to retrieve a specific key.
     */
    if ( (setKey != null) || ((startkey!=null) && (startkey.equals(endkey))) ) {
      LOG.info("startkey == endkey");
      result = new OracleResult<K, T>(this, query);
      ((OracleQuery) query).setResult(result);
      ((OracleQuery) query).setExecuted(true);
      return result;
    }

    Iterator<Key> iter = OracleUtil.getPrimaryKeys(kvstore, query, mapping.getTableName());

    LOG.info("iterating...");
    while (iter.hasNext())
      LOG.info("key:"+iter.next().toString());

    iter = OracleUtil.getPrimaryKeys(kvstore, query, mapping.getTableName());

    result = new OracleResult<K, T>(this, query, iter);
    ((OracleQuery) query).setResult(result);
    ((OracleQuery) query).setExecuted(true);
    return result;
  }

  /**
   * //TODO the javadoc
   * @return
   */
  @Override
  public Query<K, T> newQuery() {
    LOG.info("newQuery called!");
    return new OracleQuery<K, T>(this);
  }

  @Override
  public List<PartitionQuery<K, T>> getPartitions(Query<K, T> query) throws IOException {
    //TODO
    return null;
  }


  /**
   * Executes the accumulated operations to the backend datastore.
   * The operations are accumulated in the operations LinkedHashSet<List<Operation>>.
   */
  @Override
  public void flush() {

    LOG.info("flush()");
    List<Operation> opList;

    Iterator<List<Operation>> iterOper = operations.iterator();
    while (iterOper.hasNext()){
      try {
        opList = iterOper.next();
        for (Operation op : opList)
          LOG.info("Executing:"+op.getType()+" for key:"+op.getKey());

        kvstore.execute(opList);
        iterOper.remove();

      } catch (OperationExecutionException oee) {
        LOG.error("Some error occurred that prevented the sequence from executing successfully.");
        LOG.error(oee.getFailedOperationIndex() + " " + oee.getFailedOperationResult());
      } catch (DurabilityException de) {
        LOG.error("The durability guarantee could not be met.");
      } catch (IllegalArgumentException iae) {

        LOG.error("An operation in the list was null or empty." +
                "Or at least one operation operates on a key " +
                "with a major path component that is different " +
                "than the others. " +
                "Or more than one operation uses the same key.");

      } catch (RequestTimeoutException rte) {
        LOG.error("The operation was not completed inside of the default request timeout limit.");
      } catch (FaultException fe) {
        LOG.error("A generic error occurred.");
      }
    }
    LOG.info("finished flushing");
  }

  /**
   * //TODO the javadoc
   */
  @Override
  public void close() {
    flush();
    LOG.info("Datastore closed.");
  }

}