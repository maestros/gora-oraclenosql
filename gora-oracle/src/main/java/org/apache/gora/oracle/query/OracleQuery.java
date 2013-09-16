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
package org.apache.gora.oracle.query;

import oracle.kv.KVStore;
import org.apache.commons.codec.binary.Base64;
import org.apache.gora.oracle.store.OracleStore;
import org.apache.gora.oracle.util.OracleUtil;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.query.impl.QueryBase;
import org.apache.gora.store.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 * Oracle NoSQL specific implementation of the {@link org.apache.gora.query.Query} interface.
 * @author Apostolos Giannakidis
 */
public class OracleQuery<K, T extends PersistentBase> extends QueryBase<K, T> {

  /**
   * Helper to write useful information into the logs
   */
  private static final Logger LOG = LoggerFactory.getLogger(OracleQuery.class);

  private boolean executed; //flag to indicate if the query has been executed or if it is a new one

  OracleResult result;  //the cached result.

  /**
   * Default Constructor
   */
  public OracleQuery() {
    super(null);
    executed=false;
    result=null;
  }

  public OracleQuery(DataStore<K, T> dataStore) {
    super(dataStore);
  }

  @Override
  public OracleStore<K, T> getDataStore() {
    return (OracleStore<K, T>) super.getDataStore();
  }

  @Override
  public void setKey(K key) {
    setKeyRange(key, key);
  }

  /**
   * Sets the startKey of the query range. The key is encoded properly.
   * @param startKey the lower limit of the query range
   */
  @Override
  public void setStartKey(K startKey) {
    K persistentKey = startKey;

    if (persistentKey!=null)
      persistentKey = (K) OracleUtil.encodeKey((String)startKey);
    this.startKey = persistentKey;
    executed=false;

    LOG.debug("setStartKey="+this.startKey);
  }

  /**
   * Sets the endKey of the query range. The key is encoded properly.
   * @param endKey the upper limit of the query range
   */
  @Override
  public void setEndKey(K endKey) {
    K persistentKey = endKey;

    if (persistentKey!=null)
      persistentKey = (K) OracleUtil.encodeKey((String)endKey);
    this.endKey = persistentKey;
    executed=false;

    LOG.debug("setEndKey="+this.endKey);
  }

  /**
   * Sets the startKey and the endKey of the query range. The keys are encoded properly.
   * @param startKey  the lower limit of the query range
   * @param endKey  the upper limit of the query range
   */
  @Override
  public void setKeyRange(K startKey, K endKey) {
    K persistentStartKey = startKey;

    if (startKey!=null)
      persistentStartKey = (K) OracleUtil.encodeKey((String)startKey);

    K persistentEndKey = endKey;

    if (endKey!=null)
      persistentEndKey = (K) OracleUtil.encodeKey((String)endKey);

    this.startKey = persistentStartKey;
    this.endKey = persistentEndKey;
    executed=false;

    LOG.debug("setStartKey="+this.startKey);
    LOG.debug("setEndKey="+this.endKey);
  }

  /** Setters and getters for the fields **/
  public boolean isExecuted() {
    return executed;
  }

  public void setExecuted(boolean executed) {
    this.executed = executed;
  }

  public OracleResult getResult() {
    return result;
  }

  public void setResult(OracleResult result) {
    this.result = result;
  }
}
