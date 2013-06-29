/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gora.cassandra.query;

import java.util.ArrayList;

import me.prettyprint.cassandra.serializers.StringSerializer;


/**
 * List of key value pairs representing a row, tagged by a key.
 */
public class CassandraRow<K> extends ArrayList<CassandraColumn> {

  /**
   * 
   */
  private static final long serialVersionUID = -7620939600192859652L;
  private K key;

  public K getKey() {
    return this.key;
  }

  public void setKey(K key) {
    this.key = key;
  }
  
  /**
   * Gets a specific CassandraColumn within a row using its name
   * @param pCassandraColumnName
   * @return CassandraColumn
   */
  public CassandraColumn getCassandraColumn(String pCassandraColumnName){
    for (CassandraColumn cColumn: this)
      if ( pCassandraColumnName.equals(StringSerializer.get().fromByteBuffer(cColumn.getName())) )
        return cColumn;
    
    return null;
  }

}
