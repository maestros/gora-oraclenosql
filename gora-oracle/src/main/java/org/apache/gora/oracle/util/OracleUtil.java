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

import oracle.kv.Key;
import oracle.kv.Value;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.gora.avro.PersistentDatumWriter;
import org.apache.gora.persistency.ListGenericArray;
import org.apache.gora.persistency.State;
import org.apache.gora.persistency.StatefulHashMap;
import org.apache.gora.persistency.StatefulMap;
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
          LOG.info("Create value:"+ new String(((Utf8) value).getBytes(), UTF8_CHARSET) );
          returnValue = Value.createValue( ((Utf8) value).getBytes() ) ;
          break;
        case MAP:
          byte[] data = null;
          StatefulMap<Utf8,Utf8> map = (StatefulMap) value;
          Map<Utf8,Utf8> new_map = new HashMap<Utf8,Utf8>();
          try {

            Set<?> es = map.states().entrySet();
            for (Object entry : es) {
              Utf8 mapKey = (Utf8)((Map.Entry) entry).getKey();
              State state = (State) ((Map.Entry) entry).getValue();

              switch (state) {
                case DIRTY:
                case NEW:
                  new_map.put(mapKey, map.get(mapKey));
                  break;
              }

            }

            map = new StatefulHashMap<Utf8,Utf8>(new_map);
            data = IOUtils.serialize( datumWriter, fieldSchema, map );
          } catch ( IOException e ) {
            LOG.error(e.getMessage(), e.getStackTrace().toString());
          }

          LOG.info("createValue Map.Size:"+map.size());

          returnValue = Value.createValue( data ) ;
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
          LOG.info("Create value:"+ new String(((ByteBuffer) value).array()));
          returnValue = Value.createValue(((ByteBuffer) value).array());
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

    LOG.info("returnKey="+returnKey.toString());
    return returnKey;
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

}