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
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class OracleUtil{

  /**
   * Helper to write useful information into the logs
   */
  private static final Logger LOG = LoggerFactory.getLogger(OracleUtil.class);

  /**
   * Converts a value into an oracle.kv.Value and returns that Value.
   * @param value the value to be converted
   * @return the value as an oracle.kv.Value
   */
  public static Value createValue(Object value, Schema fieldSchema){
    Value returnValue;

    if (value!=null){
      returnValue = Value.createValue(value.toString().getBytes());

      byte[] byteArrayValue = {};

      switch (fieldSchema.getType()){
        case BYTES:
          returnValue = Value.createValue(((ByteBuffer) value).array());
          break;
        case STRING:
          returnValue = Value.createValue( value.toString().getBytes()) ;
          break;
        case RECORD: break;
        case UNION:
       /*   LOG.info("create value before="+new String(((ByteBuffer) value).array()));
          SpecificDatumWriter writer = new SpecificDatumWriter(fieldSchema);
          ByteArrayOutputStream os = new ByteArrayOutputStream();
          BinaryEncoder encoder = new BinaryEncoder(os);

          try {
            writer.write(((ByteBuffer) value), encoder);
            encoder.flush();
            byteArrayValue = os.toByteArray();

          //This is where the problem happens!!
         //   byteArrayValue = Arrays.copyOfRange(byteArrayValue, 2, byteArrayValue.length);

            os.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
       */
          LOG.info("Create value:"+ new String(((ByteBuffer) value).array()));
      //    returnValue = Value.createValue(byteArrayValue) ;
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