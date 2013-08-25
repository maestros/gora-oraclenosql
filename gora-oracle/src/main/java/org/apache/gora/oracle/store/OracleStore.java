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

import oracle.kv.Consistency;
import oracle.kv.Durability;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.Value;
import oracle.kv.DurabilityException;
import oracle.kv.FaultException;
import oracle.kv.Direction;
import oracle.kv.Depth;
import oracle.kv.ValueVersion;
import oracle.kv.RequestTimeoutException;
import oracle.kv.OperationFactory;
import oracle.kv.Operation;
import oracle.kv.OperationExecutionException;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang.StringUtils;
import org.apache.gora.avro.PersistentDatumWriter;
import org.apache.gora.oracle.encoders.Encoder;
import org.apache.gora.oracle.query.OracleQuery;
import org.apache.gora.oracle.query.OracleResult;
import org.apache.gora.oracle.store.OracleMapping.OracleMappingBuilder;
import org.apache.gora.oracle.util.OracleUtil;
import org.apache.gora.util.OperationNotSupportedException;

import org.apache.gora.persistency.State;
import org.apache.gora.persistency.StateManager;
import org.apache.gora.persistency.StatefulHashMap;
import org.apache.gora.persistency.StatefulMap;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.query.PartitionQuery;
import org.apache.gora.query.Query;
import org.apache.gora.query.Result;
import org.apache.gora.store.DataStoreFactory;
import org.apache.gora.store.impl.DataStoreBase;
import org.apache.gora.util.AvroUtils;
import org.apache.gora.util.IOUtils;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
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

  private volatile OracleMapping mapping; //the mapping to the datastore
  private Encoder encoder;

  /*********************************************************************
   * Variables and references to Oracle NoSQL properties
   * and configuration values.
   *********************************************************************/
  private static String storeName;  //the name of the oracle kv store

  /*
   * the name(s) of the host to connect (could be the IP)
   * and the port(s) of the oracle kv store to connect to
   */
  private static String[] hostNamePorts;

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

    encoder = new org.apache.gora.oracle.encoders.BinaryEncoder();

    operations = new LinkedHashSet();

    readProperties(properties);

    setupClient();

    try {
      LOG.debug("mappingFile="+mappingFile);
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
   * and establishes a new connection to the Oracle NoSQL datastore.
   */
  private void setupClient(){

    conf = new KVStoreConfig(storeName, hostNamePorts);

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

      mappingFile = DataStoreFactory.getMappingFile(properties, this, OracleStoreConstants.DEFAULT_MAPPING_FILE);


    storeName = DataStoreFactory.findProperty(properties, this, OracleStoreConstants.STORE_NAME, OracleStoreConstants.DEFAULT_STORE_NAME);
    hostNamePorts = OracleUtil.getHostPorts(DataStoreFactory.findProperty(properties, this, OracleStoreConstants.HOST_NAME_PORT, OracleStoreConstants.DEFAULT_HOST_NAME_PORT), OracleStoreConstants.PROPERTIES_SEPARATOR);
    primaryKeyTable = DataStoreFactory.findProperty(properties, this, OracleStoreConstants.PRIMARYKEY_TABLE_NAME, OracleStoreConstants.DEFAULT_PRIMARYKEY_TABLE_NAME);

    try{
      requestTimeout = Integer.parseInt(DataStoreFactory.findProperty( properties, this, OracleStoreConstants.REQUEST_TIMEOUT, String.valueOf(KVStoreConfig.DEFAULT_REQUEST_TIMEOUT)));
    }
    catch ( NumberFormatException nfe ) {
      requestTimeout = KVStoreConfig.DEFAULT_REQUEST_TIMEOUT;
      LOG.warn( "Invalid requestTimeout value. Using default " + String.valueOf(KVStoreConfig.DEFAULT_REQUEST_TIMEOUT) );
    }

    try{
      readTimeout = Integer.parseInt(DataStoreFactory.findProperty( properties, this, OracleStoreConstants.READ_TIMEOUT, String.valueOf(KVStoreConfig.DEFAULT_READ_TIMEOUT)));
    }
    catch ( NumberFormatException nfe ) {
      readTimeout = KVStoreConfig.DEFAULT_READ_TIMEOUT;
      LOG.warn( "Invalid readTimeout value. Using default " + String.valueOf(KVStoreConfig.DEFAULT_READ_TIMEOUT) );
    }

    try{
      openTimeout = Integer.parseInt(DataStoreFactory.findProperty( properties, this, OracleStoreConstants.OPEN_TIMEOUT, String.valueOf(KVStoreConfig.DEFAULT_OPEN_TIMEOUT)));
    }
    catch ( NumberFormatException nfe ) {
      openTimeout = KVStoreConfig.DEFAULT_OPEN_TIMEOUT;
      LOG.warn( "Invalid openTimeout value. Using default " + String.valueOf(KVStoreConfig.DEFAULT_OPEN_TIMEOUT) );
    }

    durabilityReplicaAckPolicy = Durability.ReplicaAckPolicy.valueOf(DataStoreFactory.findProperty( properties, this, OracleStoreConstants.DURABILITY_REPLICAACKPOLICY, Durability.ReplicaAckPolicy.SIMPLE_MAJORITY.name() ));
    durabilitySyncPolicy = Durability.SyncPolicy.valueOf(DataStoreFactory.findProperty( properties, this, OracleStoreConstants.DURABILITY_SYNCPOLICY, Durability.SyncPolicy.WRITE_NO_SYNC.name() ));

    String tmpConsistency = DataStoreFactory.findProperty( properties, this, OracleStoreConstants.CONSISTENCY, Consistency.NONE_REQUIRED.getName() );

    if (tmpConsistency.equals("NONE_REQUIRED"))
      consistency = Consistency.NONE_REQUIRED;
    else if (tmpConsistency.equals("ABSOLUTE"))
      consistency = Consistency.ABSOLUTE;
    else{
      consistency = Consistency.NONE_REQUIRED;
      LOG.debug("Consistency was set to default.");
    }

    String tmpTimeUnit = DataStoreFactory.findProperty( properties, this, OracleStoreConstants.TIME_UNIT, "MILLISECONDS");

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
   * @return OracleMapping Object containing all necessary information to create tables
   * @throws IOException
   */
  private OracleMapping readMapping(String mappingFilename) throws IOException {

    OracleMappingBuilder mappingBuilder = new OracleMapping.OracleMappingBuilder();

    try {
      SAXBuilder builder = new SAXBuilder();
      LOG.debug("about to parse: "+mappingFilename);
      InputStream mappingFile = getClass().getClassLoader().getResourceAsStream(mappingFilename);

      if (mappingFile==null){
        LOG.error("mappingFile is null");
        throw new IOException("Unable to open "+mappingFilename);
      }

      Document doc = builder.build(mappingFile);

      List<Element> classes = doc.getRootElement().getChildren("class");

      for ( Element classElement : classes ) {

        if ( classElement.getAttributeValue("keyClass").equals( keyClass.getCanonicalName() )
                && classElement.getAttributeValue("name").equals( persistentClass.getCanonicalName() ) ) {

          String tableName = getSchemaName( classElement.getAttributeValue("table"), persistentClass );
          mappingBuilder.setTableName( tableName );

          mappingBuilder.setClassName( classElement.getAttributeValue("name") );
          mappingBuilder.setKeyClass( classElement.getAttributeValue("keyClass") );

          Element primaryKeyEl = classElement.getChild("primarykey");

          String primaryKeyField = primaryKeyEl.getAttributeValue("name");
          String primaryKeyColumn = primaryKeyEl.getAttributeValue("column");

          mappingBuilder.setPrimaryKey( primaryKeyField );
          mappingBuilder.addField(primaryKeyField, primaryKeyColumn);

          List<Element> fields = classElement.getChildren("field");

          for ( Element field : fields ) {
            String fieldName = field.getAttributeValue("name");
            String columnName = field.getAttributeValue("column");

            mappingBuilder.addField(fieldName, columnName);
          }
          break;
        }
      }

    }
    catch ( Exception ex ) {
      LOG.error("Error in parsing: "+ex.getMessage());
      throw new IOException(ex);
    }

    LOG.debug("parse finished.");
    return mappingBuilder.build();
  }


  public Object fromBytes(Schema schema, byte data[]) {
    return fromBytes(encoder, schema, data);
  }

  public static Object fromBytes(Encoder encoder, Schema schema, byte data[]) {
    switch (schema.getType()) {
      case BOOLEAN:
        return encoder.decodeBoolean(data);
      case DOUBLE:
        return encoder.decodeDouble(data);
      case FLOAT:
        return encoder.decodeFloat(data);
      case INT:
        return encoder.decodeInt(data);
      case LONG:
        return encoder.decodeLong(data);
      case STRING:
        return new Utf8(data);
      case BYTES:
        return ByteBuffer.wrap(data);
      case ENUM:
        return AvroUtils.getEnumValue(schema, encoder.decodeInt(data));
    }
    throw new IllegalArgumentException("Unknown type " + schema.getType());

  }

  /**
   * Gets the name of the table that stores the primary keys.
   * @return the name of the table that stores the primary keys.
   */
  public static String getPrimaryKeyTable() {
    return primaryKeyTable;
  }

  /**
   * Gets the schema name.
   * In Gora-Oracle datastore semantics, the schema is the table name,
   * which is in essence the 1st major component of the key.
   * @return String The schema name.
   */
  @Override
  public String getSchemaName() {
    return mapping.getTableName();
  }

  /**
   * Creates the optional schema in the datastore
   * to hold the objects. In essence, this creates the key that
   * will be used as parent key (table) that will hold the keys
   * for all the persistent objects.
   * If the schema is already created previously,
   * the operation is ignored.
   * @throws IOException
   */
  @Override
  public void createSchema() {

    if (schemaExists()){
      return;
    }

    int tries=0;
    while (tries<2){
      try {
        kvstore.put(mapping.getMajorKey(), Value.EMPTY_VALUE);
        tries=2;
        LOG.debug("Schema: "+mapping.getMajorKey()+" was created successfully");
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
   * Deletes all the underlying key/value pairs in the datastore
   * that holds the objects. After the execution of this method,
   * no data (key/value pairs) exist in the database.
   */
  @Override
  public void deleteSchema() {

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
          LOG.debug("Deleting: "+descendant.toString());
          kvstore.multiDelete(descendant, null,
                  Depth.PARENT_AND_DESCENDANTS);
        }

          List<String> primaryKeys = new ArrayList<String>();
          primaryKeys.add(OracleStore.getPrimaryKeyTable());
          primaryKeys.add(mapping.getTableName());

          Key primary = Key.createKey(primaryKeys);
          LOG.debug("Deleting: "+primary.toString());
          kvstore.multiDelete(primary, null,
                  Depth.PARENT_AND_DESCENDANTS);

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


  /**
   * Converts a value into an oracle.kv.Value and returns that Value.
   * @param value the value to be converted
   * @return the value as an oracle.kv.Value
   */
  private Value createValue(Object value, Schema fieldSchema, PersistentDatumWriter datumWriter){
    Value returnValue;

    if (value!=null){
      returnValue = Value.createValue(value.toString().getBytes());

      byte[] byteArrayValue = null;

      switch (fieldSchema.getType()){
        case MAP:
          byte[] data = null;

          try {
            Map map = (Map) value;
            StatefulMap<Utf8,Utf8> new_map = null;
            Map<Utf8,Utf8> temp_map = new HashMap<Utf8,Utf8>();

            if (map.size()>0) {
              Set<?> es = map.entrySet();
              for (Object entry : es) {
                Utf8 mapKey = (Utf8) ((Map.Entry) entry).getKey();
                Utf8 mapVal = (Utf8) ((Map.Entry) entry).getValue();
                temp_map.put(mapKey, mapVal);
              }
            }

            StatefulMap<Utf8,Utf8> old_map = (StatefulMap) value;

            Set<?> es = old_map.states().entrySet();
            for (Object entry : es) {
              Utf8 mapKey = (Utf8)((Map.Entry) entry).getKey();
              State state = (State) ((Map.Entry) entry).getValue();

              switch (state) {
                case DIRTY:
                case CLEAN:
                case NEW:
                  temp_map.put(mapKey, old_map.get(mapKey));
                  break;
                case DELETED:
                  temp_map.remove(mapKey);
                  break;
              }
            }

            new_map = new StatefulHashMap<Utf8,Utf8>(temp_map);

            data = IOUtils.serialize( datumWriter, fieldSchema, new_map );
            returnValue = Value.createValue( data ) ;

          } catch ( IOException e ) {
            LOG.error(e.getMessage(), e.getStackTrace().toString());
          }

          break;
        case ARRAY:
        case RECORD:
          try {
            byteArrayValue = IOUtils.serialize(datumWriter, fieldSchema, value);
          } catch ( IOException e ) {
            LOG.error( e.getMessage(), e.getStackTrace().toString() );
          }
          returnValue = Value.createValue( byteArrayValue ) ;
          break;
        case UNION:

          if (value instanceof ByteBuffer)
            returnValue = Value.createValue(((ByteBuffer) value).array());
          else {
            SpecificDatumWriter writer = new SpecificDatumWriter(fieldSchema);
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            BinaryEncoder encoder = new BinaryEncoder(os);
            try {
              writer.write(value, encoder);
              encoder.flush();
            } catch (IOException e) {
              e.printStackTrace();
            }

            returnValue = Value.createValue( os.toByteArray() ) ;
          }

          break;
        default:
          returnValue = Value.createValue(OracleUtil.toBytes(encoder, value));
          break;
      }

    }
    else
      returnValue = Value.EMPTY_VALUE;

    return returnValue;
  }

  /**
   * Creates a new Persistent instance with the values in 'result' for the fields listed.
   * @param result result form a HTable#get()
   * @param fields List of fields queried, or null for all
   * @return A new instance with default values for not listed fields
   *         null if 'result' is null.
   * @throws IOException
   */
  public T newInstance(SortedMap<Key, ValueVersion> result, String[] fields)
          throws IOException {
    if(result == null || result.isEmpty())
      return null;

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
      String field = OracleUtil.getFieldFromKey(entry.getKey());
      if (!fieldsSet.isEmpty() && !fieldsSet.contains(field)) {
        //if field retrieved is not contained in the the specified field set
        //then skip this field (thus, do not include it in the new Persistent)
        LOG.debug("field:"+field+" not in fieldset. skipped.");
        continue;
      }

      Schema.Field persistentField = fieldMap.get(field);

      byte[] val = entry.getValue().getValue().getValue();
      if (val == null) {
        LOG.debug("Field: "+field+" was skipped because its value was null.");
        continue;
      }

      LOG.debug("field: "+persistentField.name()+", schema: "+persistentField.schema().getType());

      Object v = null;

      switch (persistentField.schema().getType()){
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
          String type = persistentField.schema().getTypes().get(1).getName();

          if (type.equals("bytes")){
            bb = ByteBuffer.wrap(val);
            persistent.put(persistentField.pos(),  bb);
          }
          else {
            SpecificDatumReader reader = new SpecificDatumReader(persistentField.schema());
            BinaryDecoder decoder = DecoderFactory.defaultFactory().createBinaryDecoder(val, null);
            persistent.put(persistentField.pos(), reader.read(null, decoder));
          }

          break;
        default:
          persistent.put(persistentField.pos(), fromBytes(persistentField.schema(), val));
          break;
      }

    }

    stateManager.clearDirty(persistent);
    return persistent;
  }

  /**
   * Returns the object corresponding to the given key.
   * @param key the key of the object
   * @param fields the fields required in the object. Pass null, to retrieve all fields.
   * @return the Persistent object with the specified fields
   */
  @Override
  public T get(K key, String[] fields) {
    // trivial check for a null key
    if (key==null)
      return null;

    String persistentKey = OracleUtil.encodeKey((String)key);

    Key myKey = OracleUtil.createTableKey(persistentKey, mapping.getTableName());

    SortedMap<Key, ValueVersion> kvResult;
    try {
      kvResult = kvstore.multiGet(myKey, null, null);
    } catch (FaultException e) {
      LOG.error("The operation cannot be completed: "+e);
      return null;
    }

    LOG.debug("multiGet size: " + kvResult.size());

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
   * Inserts the persistent object with the given key. If an
   * object with the same key already exists it will silently
   * be replaced.
   * @param key the key for the new persistent object
   * @param persistent the persistent object
   */
  @Override
  public void put(K key, T persistent) {

    Schema schema = persistent.getSchema();
    StateManager stateManager = persistent.getStateManager();

    if ( !stateManager.isDirty( persistent ) ) {
      // nothing to do
      LOG.debug("is not dirty");
      return;
    }

    List<Schema.Field> fields = schema.getFields();

    List<Operation> opList = new ArrayList<Operation>();
    OperationFactory of = kvstore.getOperationFactory();

    /*
      Add the key to the list of primary keys, for easy access
     */

    String persistentKey = OracleUtil.encodeKey((String)key);

    List<String> majorComponentsForParent = new ArrayList<String>();
    majorComponentsForParent.add(OracleStore.getPrimaryKeyTable());
    majorComponentsForParent.add(mapping.getTableName());
    Key primaryKey = Key.createKey(majorComponentsForParent, persistentKey);

    opList.add(of.createPut(primaryKey, Value.EMPTY_VALUE));

    operations.add(opList);
    opList = new ArrayList<Operation>();

    //List for majorComponents for the Persistent
    ArrayList<String> majorComponents = new ArrayList<String>();

    // Define the major and minor path components for the key
    majorComponents.add(mapping.getTableName());
    majorComponents.add(persistentKey);

    for ( Schema.Field field : fields ) {
      Object value = persistent.get( field.pos() );
      Schema fieldSchema = field.schema();

      // Create the key
      // The field name will be part of the minor components of the key
      Key oracleKey = OracleUtil.createKey(majorComponents, mapping.getColumn(field.name()));

      LOG.debug("fieldSchema="+fieldSchema.getType());

      // in case the value is null then delete the key
      if (value==null){
        of = kvstore.getOperationFactory();
        opList.add(of.createDelete(oracleKey)); //delete the key
      }
      else{
        // Create the value
        Value oracleValue = createValue(value, fieldSchema, datumWriter);
        of = kvstore.getOperationFactory();
        opList.add(of.createPut(oracleKey, oracleValue));
      }
    }

    LOG.info("Added a put operation for key: "+majorComponents.get(0)+"/"+OracleUtil.decodeKey(majorComponents.get(1)));
    LOG.debug(majorComponents.get(1));
    operations.add(opList);
  }

  /**
   * Helper method that gets all the fields of the persistent object
   * using reflection.
   * @return a string array that contains all the fields of the persistent object
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
   * Helper method that gets the key from a Persistent object.
   * @return the key (K) of the Persistent object.
   */
  private K getKeyFromPersistent(T obj){
    K key = null;
    try {
      LOG.debug("Primary key field:"+mapping.getPrimaryKey());
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
   * Searches the Oracle NoSQL to verify if the persistent object exists or not.
   * @param obj the persistent object to search for
   * @return true of false depending on if the object exists of not
   */
  public boolean exists(T obj){

    //get the key from the persistent object
    String key = (String)getKeyFromPersistent(obj);
    key = OracleUtil.encodeKey(key);

    //create the Oracle Key for the primary key
    List<String> majorComponentsForParent = new ArrayList<String>();
    majorComponentsForParent.add(OracleStore.getPrimaryKeyTable());
    majorComponentsForParent.add(mapping.getTableName());
    Key primaryKey = Key.createKey(majorComponentsForParent, key);

    //create the Oracle Key for the persistent
    List<String> majorComponentsForPersistent = new ArrayList<String>();
    majorComponentsForPersistent.add(mapping.getTableName());
    majorComponentsForPersistent.add(key);
    Key persistentKey = Key.createKey(majorComponentsForPersistent, mapping.getPrimaryKey());

    //check to see if the primary key and the persistent key exist in the db
    if ( (kvstore.get(primaryKey) == null) || (kvstore.get(persistentKey) == null) )
      return false;
    else
      return true;
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
    K key = getKeyFromPersistent(obj);

    LOG.debug("Key:"+key);
    Query<K,T> query = newQuery();
    query.setKey(key);
    long rowsDeleted = deleteByQuery(query);

    if (rowsDeleted>1)
      LOG.warn("Warning: Single key:"+key+" deleted "+rowsDeleted+" records (primary keys).");

    return rowsDeleted > 0;
  }

  /**
   * Deletes the persistent object with the given key.
   * @param key the key of the object
   * @return whether the object was successfully deleted
   * @throws IOException
   */
  @Override
  public boolean delete(K key) {
    LOG.debug("Inside delete(). Key:"+key);
    Query<K,T> query = newQuery();
    query.setKey(key);
    long rowsDeleted = deleteByQuery(query);

    if (rowsDeleted>1)
      LOG.warn("Warning: Single key:"+key+" deleted "+rowsDeleted+" records (primary keys).");

    return  rowsDeleted > 0;
  }

  /**
   * Deletes all the objects matching the query.
   * @param query matching records to this query will be deleted
   * @return number of deleted records
   * @throws IOException
   */
  @Override
  public long deleteByQuery(Query<K, T> query) {

    if (((OracleQuery) query).isExecuted()){
      LOG.debug("query has already been executed.");
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
          LOG.debug("deleteByQuery. fields to be deleted:"+fields.length);

          if (Arrays.equals(fields, getAllPersistentFields())){
            //Delete all the fields associated with the persistent
            //along with its primary key.
            opList = deleteRecord((String) result.getKey());
            recordsDeleted++;
            operations.add(opList);
          }
          else{
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

    LOG.debug("recordsDeleted=" + recordsDeleted);
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

    LOG.debug("Field to be deleted: "+oracleKey.toString());
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
    OperationFactory of = kvstore.getOperationFactory();;
    String persistentKey = OracleUtil.encodeKey((String)key);
    Key oracleKey = OracleUtil.createTableKey(persistentKey, mapping.getTableName());
    LOG.debug("Key to be deleted: "+key.toString());
    kvstore.multiDelete(oracleKey, null,
            Depth.PARENT_AND_DESCENDANTS);

    //Delete the primary key
    List<String> primaryKeyComponents = new ArrayList<String>();
    primaryKeyComponents.add(OracleStore.getPrimaryKeyTable());
    primaryKeyComponents.add(mapping.getTableName());
    oracleKey = Key.createKey(primaryKeyComponents, persistentKey);
    LOG.debug("Primary Key to be deleted: " + oracleKey.toString());
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
    LOG.debug("execute");

    if (((OracleQuery) query).isExecuted())
      return ((OracleQuery) query).getResult();

    OracleResult result;
    String startkey = (String)query.getStartKey();
    String endkey = (String)query.getEndKey();
    String setKey = (String)query.getKey();

    LOG.debug("startkey=" + startkey);
    LOG.debug("endkey=" + endkey);

    /*
     * in case startkey == endkey then
     * create a new OracleResult without an iterator
     * in order to retrieve a specific key.
     */
    if ( (setKey != null) || ((startkey!=null) && (startkey.equals(endkey))) ) {
      result = new OracleResult<K, T>(this, query, null);
      ((OracleQuery) query).setResult(result);
      ((OracleQuery) query).setExecuted(true);
      return result;
    }

    Iterator<Key> iter = OracleUtil.getPrimaryKeys(kvstore, query, mapping.getTableName());

    result = new OracleResult<K, T>(this, query, iter);
    ((OracleQuery) query).setResult(result);
    ((OracleQuery) query).setExecuted(true);
    return result;
  }

  /**
   * Constructs and returns a new OracleQuery.
   * @return a new Query.
   */
  @Override
  public Query<K, T> newQuery() {
    return new OracleQuery<K, T>(this);
  }

  @Override
  public List<PartitionQuery<K, T>> getPartitions(Query<K, T> query)
          throws IOException,OperationNotSupportedException {
    //TODO
    throw new OperationNotSupportedException("getPartitions is not yet supported for Oracle NoSQL store.");
  }

  /**
   * Forces the write caches to be flushed. Gora-Oracle NoSQL datastore
   * optimizes its writing by deferring the actual put / delete operations
   * until this moment.
   * In essence, it executes the accumulated operations to the backend database.
   * The operations are accumulated in the operations LinkedHashSet<List<Operation>>.
   * @throws IOException
   */
  @Override
  public void flush() {

    List<Operation> opList;

    Iterator<List<Operation>> iterOper = operations.iterator();
    while (iterOper.hasNext()){
      try {
        opList = iterOper.next();
        for (Operation op : opList)
          LOG.debug("Executing:"+op.getType()+" for key:"+op.getKey());

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
  }

  /**
   * This Gora API method does not map semantically to
   * the kvstore.close(). Therefore, it only flushes the operations
   * but it does not free any resources, as other datastores do.
   * @throws IOException
   */
  @Override
  public void close() {
    flush();
    LOG.debug("Datastore closed.");
  }

}