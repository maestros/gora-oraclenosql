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
package org.apache.gora.solr.store;

import java.util.HashMap;

public class SolrMapping {
  HashMap<String,String> mapping;
  String coreName;
  String primaryKey;
  
  public SolrMapping() {
    mapping = new HashMap<String,String>();
  }
  
  public void addField(String goraField, String solrField) {
    mapping.put(goraField, solrField);
  }
  
  public void setPrimaryKey(String solrKey) {
    primaryKey = solrKey;
  }
  
  public void setCoreName(String coreName) {
    this.coreName = coreName;
  }
  
  public String getCoreName() {
    return coreName;
  }
  
  public String getPrimaryKey() {
    return primaryKey;
  }
  
  public String getSolrField(String goraField) {
    return mapping.get(goraField);
  }
  
}
