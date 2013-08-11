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
import oracle.kv.KeyValueVersion;
import org.apache.gora.oracle.store.OracleStore;
import org.apache.gora.oracle.util.OracleUtil;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.query.Query;
import org.apache.gora.query.impl.ResultBase;
import org.apache.gora.store.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

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

  public OracleResult(DataStore<K, T> dataStore, Query<K, T> query, Iterator<Key> iter) {
    super(dataStore, query);

    LOG.info("OracleResult constructor");

    store = (OracleStore<K, T>) dataStore;
    this.resultsIterator = iter;
    this.query = query;
    keysVisited = new HashSet<String>();
    singleResult = false;
  }

  public OracleResult(DataStore<K, T> dataStore, Query<K, T> query) {
    super(dataStore, query);

    LOG.info("OracleResult constructor");

    store = (OracleStore<K, T>) dataStore;
    this.resultsIterator = null;
    this.query = query;
    keysVisited = null;
    singleResult = true;
    persistent = null;
    key = null;
  }

  @Override
  protected boolean nextInner() throws IOException {

    LOG.info("inside nextInner()");

    if (!singleResult){
      if (!resultsIterator.hasNext()){
        LOG.info("return false");
        for (String a : keysVisited)
          LOG.info(a);

        keysVisited = new HashSet<String>();
        return false;
      }

      Key result_key = resultsIterator.next();

      if (result_key.getMajorPath().size()==1){
        LOG.info("skipping parent key:"+result_key);
        return nextInner();
      }

      LOG.info("result_key="+result_key.toString()+" size:"+result_key.getMajorPath().size());

      /*
      if ( (result_key.getMajorPath().get(0).equals(OracleStore.getPrimaryKeyTable())) && (result_key.getMinorPath().size()==0) )
      {
        LOG.info("parent:"+result_key.toString());
        return nextInner();
      }
      */

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

      LOG.info("getStartKey="+query.getStartKey()+", key="+key);

      if (keysVisited.contains((String) key)){
        LOG.info("keysVisited contains "+key);
        return nextInner();
      }

      keysVisited.add((String)key);
      LOG.info("getKey="+(String)key);
      persistent = store.get(key, null);

      LOG.info("return true");
      return true;
      }
    else
    {
      if (key==null){
        key = (K)query.getStartKey();
        LOG.info("single getKey="+(String)key);
        persistent = store.get(key, null);

        LOG.info("persistent="+persistent);
        if (persistent!=null)
          return true;
        else
          return false;
      }
      else{
        LOG.info("single return false");
        return false;
      }
    }
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return 0;
  }
}