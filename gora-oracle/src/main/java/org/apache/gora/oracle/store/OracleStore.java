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

import oracle.kv.*;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.query.PartitionQuery;
import org.apache.gora.query.Query;
import org.apache.gora.query.Result;
import org.apache.gora.store.DataStoreFactory;
import org.apache.gora.store.impl.DataStoreBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
    private static final String LOB_TIMEOUT = "gora.oraclenosql.lob.timeout";
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

    private static int lobTimeout;
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

        readProperties(properties);

        setupClient();

        try {
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
        conf.setLOBTimeout(lobTimeout, timeUnit);
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
        storeName = DataStoreFactory.findProperty( properties, this, STORE_NAME, DEFAULT_STORE_NAME );
        hostName = DataStoreFactory.findProperty( properties, this, HOST_NAME, DEFAULT_HOST_NAME );
        hostPort = DataStoreFactory.findProperty( properties, this, HOST_PORT, DEFAULT_HOST_PORT);

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
            lobTimeout = Integer.parseInt(DataStoreFactory.findProperty( properties, this, LOB_TIMEOUT, String.valueOf(KVStoreConfig.DEFAULT_LOB_TIMEOUT)));
        }
        catch ( NumberFormatException nfe ) {
            lobTimeout = KVStoreConfig.DEFAULT_LOB_TIMEOUT;
            LOG.warn( "Invalid lobTimeout value. Using default " + String.valueOf(KVStoreConfig.DEFAULT_LOB_TIMEOUT) );
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

        String tmpTimeUnit = DataStoreFactory.findProperty( properties, this, TIME_UNIT, null );

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

    private OracleMapping readMapping(String mappingFile) throws IOException {
        return null;
    }


    @Override
    public String getSchemaName() {
        //TODO
        return null;
    }

    @Override
    public void createSchema() {
        //TODO
    }

    @Override
    public void deleteSchema() {
        //TODO
    }

    @Override
    public boolean schemaExists() {
        //TODO
        return false;
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