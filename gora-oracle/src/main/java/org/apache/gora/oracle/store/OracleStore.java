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

import org.apache.gora.oracle.store.OracleMapping.OracleMappingBuilder;

import oracle.kv.*;
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
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author Apostolos Giannakidis
 */
public class OracleStore<K,T extends PersistentBase> extends DataStoreBase<K,T> {

  private static final Logger LOG = LoggerFactory.getLogger(OracleStore.class);

  private static final String DEFAULT_MAPPING_FILE = "gora-oracle-mapping.xml";
  private static final String DURABILITY_SYNCPOLICY = "gora.oraclenosql.durability.syncpolicy";
  private static final String DURABILITY_REPLICAACKPOLICY = "gora.oraclenosql.durability.replicaackpolicy";
  private static final String CONSISTENCY = "gora.oraclenosql.consistency";
  private static final String TIME_UNIT = "gora.oraclenosql.time.unit";
  private static final String REQUEST_TIMEOUT = "gora.oraclenosql.request.timeout";
  private static final String READ_TIMEOUT = "gora.oraclenosql.read.timeout";
  private static final String OPEN_TIMEOUT = "gora.oraclenosql.open.timeout";
  private static final String STORE_NAME = "gora.oraclenosql.storename";
  private static final String HOST_NAME = "gora.oraclenosql.hostname";
  private static final String HOST_PORT = "gora.oraclenosql.hostport";

  private static final String DEFAULT_STORE_NAME = "kvstore";
  private static final String DEFAULT_HOST_NAME = "localhost";
  private static final String DEFAULT_HOST_PORT = "5000";

  private volatile OracleMapping mapping;

  private static String storeName;
  private static String hostName;
  private static String hostPort;
  private static String mappingFile;

  private static int readTimeout;
  private static int openTimeout;
  private static int requestTimeout;

  private static Durability.ReplicaAckPolicy durabilityReplicaAckPolicy;
  private static Durability.SyncPolicy durabilitySyncPolicy;
  private static Consistency consistency;
  private static TimeUnit timeUnit;

  private KVStore kvstore;
  private KVStoreConfig conf;

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


  @Override
  public String getSchemaName() {
    return mapping.getTableName();
  }

  @Override
  public void createSchema() {

    if (schemaExists())
      return;

    int tries=0;
    while (tries<2){
      try {
        kvstore.put(mapping.getMajorKey(), Value.EMPTY_VALUE);
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

  @Override
  public boolean schemaExists() {
    return kvstore.get(mapping.getMajorKey())!=null ? true : false;
  }

  @Override
  public T get(K key, String[] fields) {
    //TODO
    return null;
  }

  @Override
  public void put(K key, T obj) {
    //TODO

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

  @Override
  public void flush() {
    //TODO
  }

  @Override
  public void close() {
    kvstore.close();
  }

}