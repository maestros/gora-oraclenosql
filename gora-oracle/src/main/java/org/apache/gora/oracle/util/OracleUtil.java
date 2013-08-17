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
package org.apache.gora.oracle.util;

import oracle.kv.Direction;
import oracle.kv.Depth;
import oracle.kv.KVStore;
import oracle.kv.Key;
import oracle.kv.KeyRange;
import org.apache.avro.util.Utf8;
import org.apache.gora.oracle.encoders.Encoder;
import org.apache.gora.oracle.store.OracleStore;
import org.apache.gora.oracle.store.OracleStoreConstants;
import org.apache.gora.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class OracleUtil{

  /**
   * Helper to write useful information into the logs
   */
  private static final Logger LOG = LoggerFactory.getLogger(OracleUtil.class);

  private static final Charset UTF8_CHARSET = Charset.forName("UTF-8");

  private static byte[] copyIfNeeded(byte b[], int offset, int len) {
    if (len != b.length || offset != 0) {
      byte copy[] = new byte[len];
      System.arraycopy(b, offset, copy, 0, copy.length);
      b = copy;
    }
    return b;
  }

  public static byte[] toBytes(Encoder encoder, Object o) {

    try {
      if (o instanceof String) {
        LOG.debug("String");
        return ((String) o).getBytes("UTF-8");
      } else if (o instanceof Utf8) {
        LOG.debug("Utf8");
        return copyIfNeeded(((Utf8) o).getBytes(), 0, ((Utf8) o).getLength());
      } else if (o instanceof ByteBuffer) {
        LOG.debug("ByteBuffer");
        return copyIfNeeded(((ByteBuffer) o).array(), ((ByteBuffer) o).arrayOffset() + ((ByteBuffer) o).position(), ((ByteBuffer) o).remaining());
      } else if (o instanceof Long) {
        LOG.debug("Long");
        return encoder.encodeLong((Long) o);
      } else if (o instanceof Integer) {
        LOG.debug("Integer");
        return encoder.encodeInt((Integer) o);
      } else if (o instanceof Short) {
        LOG.debug("Short");
        return encoder.encodeShort((Short) o);
      } else if (o instanceof Byte) {
        LOG.debug("Byte");
        return encoder.encodeByte((Byte) o);
      } else if (o instanceof Boolean) {
        LOG.debug("Boolean");
        return encoder.encodeBoolean((Boolean) o);
      } else if (o instanceof Float) {
        LOG.debug("Float");
        return encoder.encodeFloat((Float) o);
      } else if (o instanceof Double) {
        LOG.debug("Double");
        return encoder.encodeDouble((Double) o);
      } else if (o instanceof Enum) {
        LOG.debug("Enum");
        return encoder.encodeInt(((Enum) o).ordinal());
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }

    throw new IllegalArgumentException("Uknown type " + o.getClass().getName());
  }

  public static Key createKey(List<String> majorPath, String minorPath){

    List<String> majorComponents = new ArrayList<String>();

    for (String majorComponent : majorPath){
      if (majorComponent.contains("/")){
        String majorComponentStrings[] = majorComponent.split("/");
        Collections.addAll(majorComponents, majorComponentStrings);
      }
      else
        majorComponents.add(majorComponent);
    }

    if (minorPath==null)
      return Key.createKey(majorComponents);

    Key returnKey;

    List<String> minorComponents = new ArrayList<String>();

    if (minorPath.contains("/")){
      String minorComponentStrings[] = minorPath.split("/");
      Collections.addAll(minorComponents,minorComponentStrings);
    }
    else
      minorComponents.add(minorPath);

    returnKey = Key.createKey(majorComponents, minorComponents);

    LOG.debug("returnKey="+returnKey.toString());
    return returnKey;
  }

  public static Key keyFromString(String key)
  {
    List<String> majorComponents = new ArrayList<String>();
    String [] keyComponents = key.split("/");

    for (int i = 0; i < keyComponents.length ; i ++)
      majorComponents.add(keyComponents[i]);

    return Key.createKey(majorComponents);

  }

  public static Key createKey(String fullKey){

    if (fullKey==null){
      LOG.error("Invalid fullKey: fullKey was null.");
      return null;
    }

    Key returnKey;
    String[] keyPaths;
    List<String> majorComponents = new ArrayList<String>();
    List<String> minorComponents = new ArrayList<String>();

    if (fullKey.contains("-")){
      keyPaths = fullKey.split("-");

      String minorComponentStrings[] = keyPaths[0].split("/");
      Collections.addAll(minorComponents,minorComponentStrings);

      String majorComponentStrings[] = keyPaths[1].split("/");
      Collections.addAll(majorComponents,majorComponentStrings);

      if (minorComponents.size()>0)
        returnKey = Key.createKey(majorComponents, minorComponents);
      else
        returnKey = Key.createKey(majorComponents);
    }
    else{
      String majorComponentStrings[] = fullKey.split("/");
      Collections.addAll(majorComponents, majorComponentStrings);
      returnKey = Key.createKey(majorComponents);
    }

    return returnKey;
  }

  public static Iterator<Key> getPrimaryKeys(KVStore kvStore, Query query, String tableName){

    String startkey = (String)query.getStartKey();
    String endkey = (String)query.getEndKey();

    Key primaryKey = OracleUtil.keyFromString(OracleStore.getPrimaryKeyTable()+"/"+tableName);
    LOG.debug("PrimaryKey:" + primaryKey.toString());

    KeyRange keyRange;
    if ( (startkey==null) && (endkey==null) )
      keyRange = null;  //in case both keys are null, do not create a keyrange in order to get all keys
    else
      keyRange = new KeyRange(startkey, true, endkey, true);

    Iterator<Key> iter = kvStore.multiGetKeysIterator(Direction.FORWARD, 20, primaryKey, keyRange, Depth.CHILDREN_ONLY);

    return iter;
  }

  public static String createKey(List<String> keyComponents){

    if (keyComponents==null)
      return "";

    String keyPath = "";

    for(String component : keyComponents){
      keyPath+="/"+component;
    }

    return keyPath;
  }

  public static Key createTableKey(String key, String tableName){
    /**
     * majorKey stores the table name and
     * the key for the record identification.
     * Will be used to create the Oracle key.
     */
    String majorKey;

    if ( !key.startsWith(tableName) )
      majorKey = tableName+"/"+key;
    else{
      if  ( key.startsWith("/") )
        majorKey = key;
      else
        majorKey = "/"+key;
    }

    LOG.debug("majorKey="+majorKey);

    majorKey = majorKey.replace("//"+tableName,"/"+tableName);

    Key myKey = OracleUtil.createKey(majorKey);
    LOG.debug("Key:"+myKey.toString());

    return myKey;
  }

  /**
   * Helper method that extracts the name of the field
   * from an Oracle NoSQL key. In essence, the field is the last minor component.
   * @param key the Oracle NoSQL key from which the field will be extracted
   * @return the name of the field
   */
  public static String getFieldFromKey(Key key) {
    List<String> minorPath = key.getMinorPath();

    // get the last minor key (which represents the field)
    return minorPath.get(minorPath.size() - 1);
  }

  /**
   * Helper method to split a string into its tokens.
   * @param str the String to be separated into tokens
   * @param separator the separator String
   * @return an array of all the tokens
   */
  public static String[] getHostPorts(String str, String separator){
    String[] tokens;

    if (str.contains(separator)){
      tokens = str.split(separator);

      for(int i=0;i<tokens.length;i++){
        tokens[i] = tokens[i].trim();
        if (!tokens[i].contains(":")){
          LOG.warn(OracleStoreConstants.HOST_NAME_PORT+" has invalid format. Default hostname:port was used.");
          tokens[i] = OracleStoreConstants.DEFAULT_HOST_NAME_PORT;
        }
      }
    }
    else{
      LOG.debug(str+": str does not contain "+separator);
      tokens = new String[1];
      tokens[0] = str;
    }

    return tokens;
  }

}