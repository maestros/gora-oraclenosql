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
import org.apache.gora.oracle.store.OracleStore;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.query.impl.QueryBase;
import org.apache.gora.store.DataStore;

import java.util.ArrayList;

/**
 * Oracle NoSQL specific implementation of the {@link org.apache.gora.query.Query} interface.
 */
public class OracleQuery<K, T extends PersistentBase> extends QueryBase<K, T> {

  private boolean executed;

  OracleResult result;

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
