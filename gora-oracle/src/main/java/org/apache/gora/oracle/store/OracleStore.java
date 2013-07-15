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

import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.query.PartitionQuery;
import org.apache.gora.query.Query;
import org.apache.gora.query.Result;
import org.apache.gora.store.impl.DataStoreBase;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import oracle.kv.KVStore;
import oracle.kv.KVStoreFactory;
import oracle.kv.KVStoreConfig;

/**
 * @author Apostolos Giannakidis
 */
public class OracleStore<K,T extends PersistentBase> extends DataStoreBase<K,T> {

    private static final String DEFAULT_MAPPING_FILE = "gora-oracle-mapping.xml";
    private static String storeName = "kvstore";
    private static String hostName = "localhost";
    private static String hostPort = "5000";

    private volatile OracleMapping mapping;

    private KVStore kvstore;

    @Override
    public void initialize(Class<K> keyClass, Class<T> persistentClass, Properties properties) {
        super.initialize(keyClass, persistentClass, properties);

        kvstore = KVStoreFactory.getStore
                (new KVStoreConfig(storeName, hostName + ":" + hostPort));
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