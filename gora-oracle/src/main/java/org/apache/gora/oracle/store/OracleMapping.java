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

package org.apache.gora.oracle.store;

import oracle.kv.Key;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Mapping definitions for Oracle NoSQL. Thread safe.
 * It holds a definition for a single table.
 */
public class OracleMapping {
  private static final Logger LOG = LoggerFactory.getLogger(OracleMapping.class);

  Map<String,String> mapping;

  Key majorKey;  // Partial major key that serves as a table
  String tableName; // the name of the key component that is used for persistence of the specific data bean
  String primaryKey;  // the name of the key component that is used for record identification
  String className;
  String keyClass;

  public OracleMapping() {
    mapping = new HashMap<String,String>();
    LOG.info("Inside OracleMapping constructor");
  }

  public OracleMapping(String tableName, String primaryKey, String className,
                       String keyClass, Map<String,String> mapping) {
    this.setTableName(tableName);
    this.setPrimaryKey(primaryKey);
    this.setClassName(className);
    this.setKeyClass(keyClass);
    this.setMajorKey(tableName);
    this.mapping = mapping;
    LOG.info("Inside OracleMapping constructor");
  }

  public String getClassName() {
    return className;
  }

  private void setClassName(String className) {
    this.className = className;
  }

  public String getKeyClass() {
    return keyClass;
  }

  private void setKeyClass(String keyClass) {
    this.keyClass = keyClass;
  }

  private void addField(String field, String column) {
    mapping.put(field, column);
  }

  private void setMajorKey(String majorKey) {

    List<String> majorKeys = new ArrayList<String>();
    majorKeys.add(this.getTableName());

    Key tableName = Key.createKey(majorKey);

    this.majorKey = tableName;
  }

  public Key getMajorKey() {
    return majorKey;
  }

  private void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getTableName() {
    return tableName;
  }

  private void setPrimaryKey(String field) {
    primaryKey = field;
  }

  public String getPrimaryKey() {
    return primaryKey;
  }

  public String getColumn(String field) {
    return mapping.get(field);
  }


  /**
   * A builder for creating the mapper. This will allow building a thread safe
   * {@link OracleMapping} using simple immutabilty.
   *
   */
  public static class OracleMappingBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(OracleMappingBuilder.class);

    /**
     * Maps data bean fields to Oracle NoSQL keys (called here as columns)
     */
    private Map<String,String> mapping = new HashMap<String,String>();

    /**
     * Table name to be used to build the OracleMapping object
     */
    private String tableName;
    private String primaryKey;
    private String className;
    private String keyClass;

    public String getClassName() {
      return className;
    }

    public void setClassName(String className) {
      this.className = className;
    }

    public String getKeyClass() {
      return keyClass;
    }

    public void setKeyClass(String keyClass) {
      this.keyClass = keyClass;
    }

    /**
     * Sets table name
     * @param tabName
     */
    public void setTableName(String tabName){
      LOG.info("Table was set to: "+tabName);
      tableName = tabName;
    }

    /**
     * Gets the table name for which the table is being mapped
     * @param tabName
     * @return
     */
    public String getTableName(String tabName){
      return tableName;
    }

    public void setPrimaryKey(String column) {
      primaryKey=column;
    }

    public void addField(String field, String column) {

      // verification that column is composed of valid key path
      Key tmpKey;
      try{
        tmpKey = Key.fromString("/"+column);
      }catch (IllegalArgumentException e){
        LOG.error("Invalid column: key path decoding failed.");
        return;
      }finally {
        tmpKey = null;
      }

      LOG.info("field: "+field+" was mapped to column:"+column);
      mapping.put(field, column);
    }

    /**
     * Verifies that all properties are valid and
     * constructs the OracleMapping object.
     * @return A newly constructed mapping.
     */
    public OracleMapping build() {

      // verify that the name of the table was specified
      if (tableName == null)
        throw new IllegalStateException("tableName is not specified.");

      // verify that the primaryKey was specified
      if (primaryKey == null)
        throw new IllegalStateException("primaryKey is not specified.");

      // verify that the className was specified
      if (className == null)
        throw new IllegalStateException("className is not specified.");

      // verify that the keyClass was specified
      if (keyClass == null)
        throw new IllegalStateException("keyClass is not specified.");

      // verifying that there is at least one mapping entry
      if (mapping.isEmpty())
        throw new IllegalStateException("No fields specified.");

      LOG.info("OracleMappingBuilder.build completed all checks.");

      return new OracleMapping(tableName, primaryKey, className, keyClass, mapping);
    }

  }

}