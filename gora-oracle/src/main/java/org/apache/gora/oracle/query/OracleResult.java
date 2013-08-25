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

import oracle.kv.Key;
import org.apache.gora.oracle.store.OracleStore;
import org.apache.gora.oracle.util.OracleUtil;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.query.Query;
import org.apache.gora.query.impl.ResultBase;
import org.apache.gora.store.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.commons.codec.binary.Base64;

public class OracleResult<K, T extends PersistentBase> extends ResultBase<K, T> {

  OracleStore<K, T> store;
  Iterator<Key> resultsIterator;
  Query<K, T> query;
  Set<String> keysVisited;
  boolean singleResult;

  /**
   * Helper to write useful information into the logs
   */
  private static final Logger LOG = LoggerFactory.getLogger(OracleResult.class);

  /**
   * OracleResult Constructor
   * If iter is null then nextInner() will get a single persistent object based
   * on the query.key.
   * @param dataStore the datastore used
   * @param query the query used
   * @param iter the iterator for traversing the result set; it can be null.
   */
  public OracleResult(DataStore<K, T> dataStore, Query<K, T> query, Iterator<Key> iter) {
    super(dataStore, query);

    this.store = (OracleStore<K, T>) dataStore;
    this.query = query;
    this.persistent = null;
    this.key = null;

    if (iter != null){
      this.singleResult = false;
      this.keysVisited = new HashSet<String>();
      this.resultsIterator = iter;
    }
    else{
      this.resultsIterator = null;
      this.keysVisited = null;
      this.singleResult = true;
    }
  }

  @Override
  protected boolean nextInner() throws IOException {

    if (!singleResult){
      if (!resultsIterator.hasNext()){
        keysVisited = new HashSet<String>();
        LOG.debug("return false");
        return false;
      }

      Key result_key = resultsIterator.next();

      if (result_key.getMajorPath().size()==1){
        LOG.debug("skipping parent key:" + result_key);
        return nextInner();
      }

      LOG.debug("result_key=" + result_key.toString() + " size:" + result_key.getMajorPath().size());


      String tmpKey = "";

      if (result_key.getMajorPath().get(0).equals(OracleStore.getPrimaryKeyTable())){
        for (String minorComp : result_key.getMinorPath()){
          tmpKey += minorComp+"/";
        }
        tmpKey = tmpKey.substring(0,tmpKey.length()-1);
      }
      else
        tmpKey = OracleUtil.createKey(result_key.getMajorPath());

      key = (K)tmpKey;

      LOG.debug("getStartKey=" + query.getStartKey() + ", key=" + key);

      if (keysVisited.contains((String) key)){
        LOG.debug("keysVisited contains " + key);
        return nextInner();
      }

      keysVisited.add((String)key);
      key = (K)OracleUtil.decodeKey((String)key);
      LOG.debug("getKey=" + (String) key);
      persistent = store.get(key, null);

      LOG.debug("return true");
      return true;
    }
    else
    {
      if (key==null){
        key = query.getStartKey();
        key = (K)OracleUtil.decodeKey((String)key);
        LOG.debug("single getKey=" + (String) key);
        persistent = store.get(key, null);

        LOG.debug("persistent=" + persistent);
        if (persistent!=null)
          return true;
        else
          return false;
      }
      else{
        LOG.debug("single return false");
        return false;
      }
    }
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return 0;
  }
}