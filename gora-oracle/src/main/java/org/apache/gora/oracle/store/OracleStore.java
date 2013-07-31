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
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.util.Utf8;
import org.apache.gora.oracle.store.OracleMapping.OracleMappingBuilder;
import org.apache.gora.oracle.util.OracleUtil;

import oracle.kv.*;
import org.apache.gora.persistency.ListGenericArray;
import org.apache.gora.persistency.StateManager;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.query.PartitionQuery;
import org.apache.gora.query.Query;
import org.apache.gora.query.Result;
import org.apache.gora.store.DataStoreFactory;
import org.apache.gora.store.impl.DataStoreBase;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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

  private static final String DEFAULT_STORE_NAME = "kvstore";
  private static final String DEFAULT_HOST_NAME = "localhost";
  private static final String DEFAULT_HOST_PORT = "5000";

  private volatile OracleMapping mapping; //the mapping to the datastore

  /*
   * Variables and references to Oracle NoSQL properties
   * and configuration values.
   */
  private static String storeName;  //the name of the oracle kv store
  private static String hostName;   //the name of the host to connect (could be the IP)
  private static String hostPort;
  private static String mappingFile;  //the filename of the mapping (xml) file

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
   * Gets the schema name
   */
  @Override
  public String getSchemaName() {
    return mapping.getTableName();
  }

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

  @Override
  public void deleteSchema() {

    LOG.info("deleteSchema was called.");

    if (!schemaExists())
      return;

    int tries=0;
    while (tries<2){
      try {

        /* Efficiently delete a subtree of keys using multiple major keys
        *
        * /
         */
        while (true){
          Iterator<Key> i = kvstore.storeKeysIterator
                  (Direction.UNORDERED, 1, mapping.getMajorKey(),
                          null, Depth.DESCENDANTS_ONLY);
          if (!i.hasNext()) {
            break;
          }
          Key descendant = Key.createKey(i.next().getMajorPath());
          kvstore.multiDelete(descendant, null,
                  Depth.PARENT_AND_DESCENDANTS);
        }
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
  }

  /**
   * Checks if the schema exists or not
   * @return true or false depending on whether the schema exists or not
   */
  @Override
  public boolean schemaExists() {
    return kvstore.get(mapping.getMajorKey())!=null ? true : false;
  }

  private void setField(T persistent, Schema.Field field, byte[] val)
          throws IOException {
    persistent.put(field.pos(),  ByteBuffer.wrap(val));
  }

  public T newInstance(SortedMap<Key, ValueVersion> result, String[] fields)
          throws IOException {
    if(result == null || result.isEmpty())
      return null;

    LOG.info("newInstance");
    T persistent = newPersistent();
    StateManager stateManager = persistent.getStateManager();

    Set<String> fieldsSet = new HashSet<String>();
    Collections.addAll(fieldsSet, fields);

    for (Map.Entry<Key, ValueVersion> entry : result.entrySet()) {
			/* If fields is null, read all fields */
      String field = getFieldFromKey(entry.getKey());
      if (!fieldsSet.isEmpty() && !fieldsSet.contains(field)) {
        continue;
      }

      Schema.Field persistentField = fieldMap.get(field);

      LOG.info("field: "+persistentField.name()+", schema: "+persistentField.schema().getType());

      byte[] val = entry.getValue().getValue().getValue();
      if (val == null) {
        continue;
      }

      switch (persistentField.schema().getType()){
        case BYTES:
          LOG.info("Bytes");
          ByteBuffer bb = ByteBuffer.wrap(val);
          persistent.put(persistentField.pos(),  ByteBuffer.wrap(val));
          //setField(persistent, persistentField, (ByteBuffer) bb ).array());
          break;
        case RECORD:
        case UNION:
          SpecificDatumReader reader = new SpecificDatumReader(persistentField.schema());
          BinaryDecoder decoder = DecoderFactory.defaultFactory().createBinaryDecoder(val, null);
          persistent.put(persistentField.pos(), reader.read(null, decoder));
          break;
      }

    }

    /*
    for (String f : fields) {
      HBaseColumn col = mapping.getColumn(f);
      if (col == null) {
        throw new  RuntimeException("HBase mapping for field ["+ f +"] not found. " +
                "Wrong gora-hbase-mapping.xml?");
      }
      Schema.Field field = fieldMap.get(f);
      Schema fieldSchema = field.schema();
      switch(fieldSchema.getType()) {
        case MAP:
          NavigableMap<byte[], byte[]> qualMap =
                  result.getNoVersionMap().get(col.getFamily());
          if (qualMap == null) {
            continue;
          }
          Schema valueSchema = fieldSchema.getValueType();
          Map map = new HashMap();
          for (Map.Entry<byte[], byte[]> e : qualMap.entrySet()) {
            map.put(new Utf8(Bytes.toString(e.getKey())),
                    fromBytes(valueSchema, e.getValue()));
          }
          setField(persistent, field, map);
          break;
        case ARRAY:
          qualMap = result.getFamilyMap(col.getFamily());
          if (qualMap == null) {
            continue;
          }
          valueSchema = fieldSchema.getElementType();
          ArrayList arrayList = new ArrayList();
          for (Map.Entry<byte[], byte[]> e : qualMap.entrySet()) {
            arrayList.add(fromBytes(valueSchema, e.getValue()));
          }
          ListGenericArray arr = new ListGenericArray(fieldSchema, arrayList);
          setField(persistent, field, arr);
          break;
        default:
          byte[] val = result.getValue(col.getFamily(), col.getQualifier());
          if (val == null) {
            continue;
          }
          setField(persistent, field, val);
          break;
      }
    }
    */
    stateManager.clearDirty(persistent);
    return persistent;
  }

  private static String getFieldFromKey(Key key) {
    return key.getMinorPath().get(0);
  }

  @Override
  public T get(K key, String[] fields) {

    LOG.info("inside get");

    if (key==null)
      return null;


    String majorKey;

    majorKey = mapping.getTableName()+"/"+key;

    Key myKey = OracleUtil.createKey(majorKey);

    LOG.info("Major Key:"+myKey.toString());

    SortedMap<Key, ValueVersion> kvResult;
    try {
      kvResult = kvstore.multiGet(myKey, null, null);
    } catch (FaultException e) {
      LOG.error("The operation cannot be completed: "+e);
      return null;
    }

    LOG.info("after multiGet: "+kvResult.size());

    T return_object = null;
    try {
      return_object = newInstance(kvResult, fields);
    } catch (IOException e) {
      e.printStackTrace();
    }

    return return_object;
  }

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

    ArrayList<String> majorComponents = new ArrayList<String>();

    // Define the major and minor path components for the key
    majorComponents.add(mapping.getTableName());
    majorComponents.add(key.toString());// keys in Oracle NoSQL are strings

    List<Operation> opList = new ArrayList<Operation>();


    for ( Schema.Field field : fields ) {
      Object value = persistent.get( field.pos() );
      Schema fieldSchema = field.schema();

      // Create the key
      // The field name will be part of the minor components of the key
      Key oracleKey = OracleUtil.createKey(majorComponents, mapping.getColumn(field.name()));

      LOG.info("fieldSchema="+fieldSchema.getType());

      if (value==null){
        LOG.info("value==null");
        OperationFactory of = kvstore.getOperationFactory();
        opList.add(of.createDelete(oracleKey));
      }
      else{
        LOG.info("value!=null");

        if (fieldSchema.getType()==Schema.Type.UNION){
          String test = new String(((ByteBuffer) value).array());
          LOG.info("put field:"+field.name()+", value:"+test+" to column:"+mapping.getColumn(field.name()));
        }

        // Create the value
        Value oracleValue = OracleUtil.createValue(value, fieldSchema);

        OperationFactory of = kvstore.getOperationFactory();


        opList.add(of.createPut(oracleKey, oracleValue));
      }
    }

    LOG.info("Added a put operation for key: "+majorComponents.get(0)+"/"+majorComponents.get(1));
    operations.add(opList);
  }

  @Override
  public boolean delete(K key) {
    //TODO
    return false;
  }

  @Override
  public long deleteByQuery(Query<K, T> query) {
    //TODO
    return 0;
  }

  @Override
  public Result<K, T> execute(Query<K, T> query) {
    //TODO
    return null;
  }

  @Override
  public Query<K, T> newQuery() {
    //TODO
    return null;
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
        LOG.info("Some error occurred that prevented the sequence from executing successfully.");
        LOG.info(oee.getFailedOperationIndex()+" "+oee.getFailedOperationResult());
      } catch (DurabilityException de) {
        LOG.info("The durability guarantee could not be met.");
      } catch (IllegalArgumentException iae) {

        LOG.info("An operation in the list was null or empty." +
                "Or at least one operation operates on a key " +
                "with a major path component that is different " +
                "than the others. " +
                "Or more than one operation uses the same key.");

      } catch (RequestTimeoutException rte) {
        LOG.info("The operation was not completed inside of the default request timeout limit.");
      } catch (FaultException fe) {
        LOG.info("A generic error occurred.");
      }
    }
  }

  @Override
  public void close() {
    flush();
    LOG.info("Datastore closed.");
  }

}