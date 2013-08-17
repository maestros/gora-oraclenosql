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

import oracle.kv.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.gora.avro.PersistentDatumWriter;
import org.apache.gora.oracle.store.OracleMapping;
import org.apache.gora.oracle.store.OracleStore;
import org.apache.gora.oracle.store.OracleStoreConstants;
import org.apache.gora.persistency.ListGenericArray;
import org.apache.gora.persistency.State;
import org.apache.gora.persistency.StatefulHashMap;
import org.apache.gora.persistency.StatefulMap;
import org.apache.gora.query.Query;
import org.apache.gora.util.GoraException;
import org.apache.gora.util.IOUtils;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;

public class OracleUtil{

  /**
   * Helper to write useful information into the logs
   */
  private static final Logger LOG = LoggerFactory.getLogger(OracleUtil.class);

  private static final Charset UTF8_CHARSET = Charset.forName("UTF-8");

  /**
   * Converts a value into an oracle.kv.Value and returns that Value.
   * @param value the value to be converted
   * @return the value as an oracle.kv.Value
   */
  public static Value createValue(Object value, Schema fieldSchema, PersistentDatumWriter datumWriter){
    Value returnValue;

    if (value!=null){
      returnValue = Value.createValue(value.toString().getBytes());

      byte[] byteArrayValue = null;

      switch (fieldSchema.getType()){
        case LONG:
          returnValue = Value.createValue( ByteBuffer.allocate(8).putLong((Long) value).array() );
          break;
        case INT:
          returnValue = Value.createValue( ByteBuffer.allocate(4).putInt((Integer) value).array() );
          break;
        case BYTES:
          returnValue = Value.createValue( ((ByteBuffer) value).array() );
          break;
        case STRING:
          returnValue = Value.createValue( ((Utf8) value).getBytes() ) ;
          break;
        case MAP:
          byte[] data = null;

          try {
            Map map = (Map) value;
            StatefulMap<Utf8,Utf8> new_map = null;
            Map<Utf8,Utf8> temp_map = new HashMap<Utf8,Utf8>();

            if (map.size()>0) {
              Set<?> es = map.entrySet();
              for (Object entry : es) {
                Utf8 mapKey = (Utf8) ((Map.Entry) entry).getKey();
                Utf8 mapVal = (Utf8) ((Map.Entry) entry).getValue();
                temp_map.put(mapKey, mapVal);
              }
            }

            StatefulMap<Utf8,Utf8> old_map = (StatefulMap) value;

            Set<?> es = old_map.states().entrySet();
            for (Object entry : es) {
              Utf8 mapKey = (Utf8)((Map.Entry) entry).getKey();
              State state = (State) ((Map.Entry) entry).getValue();

              switch (state) {
                case DIRTY:
                case CLEAN:
                case NEW:
                  temp_map.put(mapKey, old_map.get(mapKey));
                  break;
                case DELETED:
                  temp_map.remove(mapKey);
                  break;
              }
            }

            new_map = new StatefulHashMap<Utf8,Utf8>(temp_map);

            data = IOUtils.serialize( datumWriter, fieldSchema, new_map );
            returnValue = Value.createValue( data ) ;

          } catch ( IOException e ) {
            LOG.error(e.getMessage(), e.getStackTrace().toString());
          }

          break;
        case ARRAY:
        case RECORD:
          try {
            byteArrayValue = IOUtils.serialize(datumWriter, fieldSchema, value);
          } catch ( IOException e ) {
            LOG.error( e.getMessage(), e.getStackTrace().toString() );
          }
          returnValue = Value.createValue( byteArrayValue ) ;
          break;
        case UNION:

          if (value instanceof ByteBuffer)
            returnValue = Value.createValue(((ByteBuffer) value).array());
          else if (value instanceof Utf8)
            returnValue = Value.createValue( ((Utf8) value).getBytes() ) ;
          else {
            SpecificDatumWriter writer = new SpecificDatumWriter(fieldSchema);
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            BinaryEncoder encoder = new BinaryEncoder(os);
            try {
              writer.write(value, encoder);
              encoder.flush();
            } catch (IOException e) {
              e.printStackTrace();
            }

            returnValue = Value.createValue( os.toByteArray() ) ;
          }

          break;
      }

    }
    else
      returnValue = Value.EMPTY_VALUE;

    return returnValue;
  }

  public static Key createKey(List<String> majorPath, String minorPath){

    List<String> majorComponents = new ArrayList<String>();

    for (String majorComponent : majorPath){
      if (majorComponent.contains("/")){
        String majorComponentStrings[] = majorComponent.split("/");
        Collections.addAll(majorComponents,majorComponentStrings);
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