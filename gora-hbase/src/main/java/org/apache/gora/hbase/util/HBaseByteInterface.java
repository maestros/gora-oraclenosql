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

package org.apache.gora.hbase.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.gora.util.AvroUtils;
import org.apache.gora.avro.PersistentDatumReader;
import org.apache.gora.avro.PersistentDatumWriter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Contains utility methods for byte[] <-> field
 * conversions.
 */
public class HBaseByteInterface {
  /**
   * Threadlocals maintaining reusable binary decoders and encoders.
   */
  public static final ThreadLocal<BinaryDecoder> decoders =
      new ThreadLocal<BinaryDecoder>();
  public static final ThreadLocal<BinaryEncoderWithStream> encoders =
      new ThreadLocal<BinaryEncoderWithStream>();
  
  /**
   * A BinaryEncoder that exposes the outputstream so that it can be reset
   * every time. (This is a workaround to reuse BinaryEncoder and the buffers,
   * normally provided be EncoderFactory, but this class does not exist yet 
   * in the current Avro version).
   */
  public static final class BinaryEncoderWithStream extends BinaryEncoder {
    public BinaryEncoderWithStream(OutputStream out) {
      super(out);
    }
    
    protected OutputStream getOut() {
      return out;
    }
  }
  
  /*
   * Create a threadlocal map for the datum readers and writers, because
   * they are not thread safe, at least not before Avro 1.4.0 (See AVRO-650).
   * When they are thread safe, it is possible to maintain a single reader and
   * writer pair for every schema, instead of one for every thread.
   */
  
  public static final ThreadLocal<Map<String, SpecificDatumReader<?>>> 
    readerMaps = new ThreadLocal<Map<String, SpecificDatumReader<?>>>() {
      protected Map<String,SpecificDatumReader<?>> initialValue() {
        return new HashMap<String, SpecificDatumReader<?>>();
      };
  };
  
  public static final ThreadLocal<Map<String, SpecificDatumWriter<?>>> 
    writerMaps = new ThreadLocal<Map<String, SpecificDatumWriter<?>>>() {
      protected Map<String,SpecificDatumWriter<?>> initialValue() {
        return new HashMap<String, SpecificDatumWriter<?>>();
      };
  };

  /**
   * Deserializes an array of bytes matching the given schema to the proper basic (enum, Utf8,...) or
   * complex type (Persistent/Record).
   * 
   * Does not handle <code>arrays/maps</code> if not inside a <code>record</code> type.
   * 
   * @param schema Avro schema describing the expected data
   * @param val array of bytes with the data serialized
   * @return Enum|Utf8|ByteBuffer|Integer|Long|Float|Double|Boolean|Persistent|Null
   * @throws IOException
   */
  @SuppressWarnings("rawtypes")
  public static Object fromBytes(Schema schema, byte[] val) throws IOException {
    Type type = schema.getType();
    switch (type) {
    case ENUM:    return AvroUtils.getEnumValue(schema, val[0]);
    case STRING:  return new Utf8(Bytes.toString(val));
    case BYTES:   return ByteBuffer.wrap(val);
    case INT:     return Bytes.toInt(val);
    case LONG:    return Bytes.toLong(val);
    case FLOAT:   return Bytes.toFloat(val);
    case DOUBLE:  return Bytes.toDouble(val);
    case BOOLEAN: return val[0] != 0;
    case UNION:
      // XXX Special case: When reading the top-level field of a record we must handle the
      // special case ["null","type"] definitions: this will be written as if it was ["type"]
      // if not in a special case, will execute "case RECORD".
      
      // if 'val' is empty we ignore the special case (will match Null in "case RECORD")  
      if (schema.getTypes().size() == 2) {
        
        // schema [type0, type1]
        Type type0 = schema.getTypes().get(0).getType() ;
        Type type1 = schema.getTypes().get(1).getType() ;
        
        // Check if types are different and there's a "null", like ["null","type"] or ["type","null"]
        if (!type0.equals(type1)
            && (   type0.equals(Schema.Type.NULL)
                || type1.equals(Schema.Type.NULL))) {

          if (type0.equals(Schema.Type.NULL))
            schema = schema.getTypes().get(1) ;
          else 
            schema = schema.getTypes().get(0) ;
          
          return fromBytes(schema, val) ; // Deserialize as if schema was ["type"] 
        }
        
      }
      // else
      //   type = [type0,type1] where type0=type1
      //   or val == null
      // => deserialize like "case RECORD"

    case RECORD:
      Map<String, SpecificDatumReader<?>> readerMap = readerMaps.get();
      PersistentDatumReader<?> reader = null ;
            
      // For UNION schemas, must use a specific PersistentDatumReader
      // from the readerMap since unions don't have own name
      // (key name in map will be "UNION-type-type-...")
      if (schema.getType().equals(Schema.Type.UNION)) {
        reader = (PersistentDatumReader<?>)readerMap.get(String.valueOf(schema.hashCode()));
        if (reader == null) {
          reader = new PersistentDatumReader(schema, false);// ignore dirty bits
          readerMap.put(String.valueOf(schema.hashCode()), reader);
        }
      } else {
        // ELSE use reader for Record
        reader = (PersistentDatumReader<?>)readerMap.get(schema.getFullName());
        if (reader == null) {
          reader = new PersistentDatumReader(schema, false);// ignore dirty bits
          readerMap.put(schema.getFullName(), reader);
        }
      }
      
      // initialize a decoder, possibly reusing previous one
      BinaryDecoder decoderFromCache = decoders.get();
      BinaryDecoder decoder=DecoderFactory.defaultFactory().
          createBinaryDecoder(val, decoderFromCache);
      // put in threadlocal cache if the initial get was empty
      if (decoderFromCache==null) {
        decoders.set(decoder);
      }
      
      return reader.read((Object)null, schema, decoder);
    default: throw new RuntimeException("Unknown type: "+type);
    }
  }

  /**
   * Converts an array of bytes to the target <em>basic class</em>.
   * @param clazz (Byte|Boolean|Short|Integer|Long|Float|Double|String|Utf8).class
   * @param val array of bytes with the value
   * @return an instance of <code>clazz</code> with the bytes in <code>val</code>
   *         deserialized with org.apache.hadoop.hbase.util.Bytes
   */
  @SuppressWarnings("unchecked")
  public static <K> K fromBytes(Class<K> clazz, byte[] val) {
    if (clazz.equals(Byte.TYPE) || clazz.equals(Byte.class)) {
      return (K) Byte.valueOf(val[0]);
    } else if (clazz.equals(Boolean.TYPE) || clazz.equals(Boolean.class)) {
      return (K) Boolean.valueOf(val[0] == 0 ? false : true);
    } else if (clazz.equals(Short.TYPE) || clazz.equals(Short.class)) {
      return (K) Short.valueOf(Bytes.toShort(val));
    } else if (clazz.equals(Integer.TYPE) || clazz.equals(Integer.class)) {
      return (K) Integer.valueOf(Bytes.toInt(val));
    } else if (clazz.equals(Long.TYPE) || clazz.equals(Long.class)) {
      return (K) Long.valueOf(Bytes.toLong(val));
    } else if (clazz.equals(Float.TYPE) || clazz.equals(Float.class)) {
      return (K) Float.valueOf(Bytes.toFloat(val));
    } else if (clazz.equals(Double.TYPE) || clazz.equals(Double.class)) {
      return (K) Double.valueOf(Bytes.toDouble(val));
    } else if (clazz.equals(String.class)) {
      return (K) Bytes.toString(val);
    } else if (clazz.equals(Utf8.class)) {
      return (K) new Utf8(Bytes.toString(val));
    }
    throw new RuntimeException("Can't parse data as class: " + clazz);
  }

  /**
   * Converts an instance of a <em>basic class</em> to an array of bytes.
   * @param o Instance of Enum|Byte|Boolean|Short|Integer|Long|Float|Double|String|Utf8
   * @return array of bytes with <code>o</code> serialized with org.apache.hadoop.hbase.util.Bytes
   */
  public static byte[] toBytes(Object o) {
    Class<?> clazz = o.getClass();
    if (clazz.equals(Enum.class)) {
      return new byte[] { (byte)((Enum<?>) o).ordinal() }; // yeah, yeah it's a hack
    } else if (clazz.equals(Byte.TYPE) || clazz.equals(Byte.class)) {
      return new byte[] { (Byte) o };
    } else if (clazz.equals(Boolean.TYPE) || clazz.equals(Boolean.class)) {
      return new byte[] { ((Boolean) o ? (byte) 1 :(byte) 0)};
    } else if (clazz.equals(Short.TYPE) || clazz.equals(Short.class)) {
      return Bytes.toBytes((Short) o);
    } else if (clazz.equals(Integer.TYPE) || clazz.equals(Integer.class)) {
      return Bytes.toBytes((Integer) o);
    } else if (clazz.equals(Long.TYPE) || clazz.equals(Long.class)) {
      return Bytes.toBytes((Long) o);
    } else if (clazz.equals(Float.TYPE) || clazz.equals(Float.class)) {
      return Bytes.toBytes((Float) o);
    } else if (clazz.equals(Double.TYPE) || clazz.equals(Double.class)) {
      return Bytes.toBytes((Double) o);
    } else if (clazz.equals(String.class)) {
      return Bytes.toBytes((String) o);
    } else if (clazz.equals(Utf8.class)) {
      return ((Utf8) o).getBytes();
    }
    throw new RuntimeException("Can't parse data as class: " + clazz);
  }

  /**
   * Serializes an object following the given schema.
   * Does not handle <code>array/map</code> if it is not inside a <code>record</code>
   * @param o Utf8|ByteBuffer|Integer|Long|Float|Double|Boolean|Enum|Persistent
   * @param schema The schema describing the object (or a compatible description)
   * @return array of bytes of the serialized object
   * @throws IOException
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static byte[] toBytes(Object o, Schema schema) throws IOException {
    Type type = schema.getType();
    switch (type) {
    case STRING:  return Bytes.toBytes(((Utf8)o).toString()); // TODO: maybe ((Utf8)o).getBytes(); ?
    case BYTES:   return ((ByteBuffer)o).array();
    case INT:     return Bytes.toBytes((Integer)o);
    case LONG:    return Bytes.toBytes((Long)o);
    case FLOAT:   return Bytes.toBytes((Float)o);
    case DOUBLE:  return Bytes.toBytes((Double)o);
    case BOOLEAN: return (Boolean)o ? new byte[] {1} : new byte[] {0};
    case ENUM:    return new byte[] { (byte)((Enum<?>) o).ordinal() };
    case UNION:
      // XXX Special case: When writing the top-level field of a record we must handle the
      // special case ["null","type"] definitions: this will be written as if it was ["type"]
      // if not in a special case, will execute "case RECORD".
      
      if (schema.getTypes().size() == 2) {
        
        // schema [type0, type1]
        Type type0 = schema.getTypes().get(0).getType() ;
        Type type1 = schema.getTypes().get(1).getType() ;
        
        // Check if types are different and there's a "null", like ["null","type"] or ["type","null"]
        if (!type0.equals(type1)
            && (   type0.equals(Schema.Type.NULL)
                || type1.equals(Schema.Type.NULL))) {

          if (o == null) return null ;
          
          int index = GenericData.get().resolveUnion(schema, o);
          schema = schema.getTypes().get(index) ;
          
          return toBytes(o, schema) ; // Serialize as if schema was ["type"] 
        }
        
      }
      // else
      //   type = [type0,type1] where type0=type1
      // => Serialize like "case RECORD" with Avro
      
    case RECORD:
      Map<String, SpecificDatumWriter<?>> writerMap = writerMaps.get();
      PersistentDatumWriter writer = null ;
      // For UNION schemas, must use a specific PersistentDatumReader
      // from the readerMap since unions don't have own name
      // (key name in map will be "UNION-type-type-...")
      if (schema.getType().equals(Schema.Type.UNION)) {
        writer = (PersistentDatumWriter<?>) writerMap.get(String.valueOf(schema.hashCode()));
        if (writer == null) {
          writer = new PersistentDatumWriter(schema,false);// ignore dirty bits
          writerMap.put(String.valueOf(schema.hashCode()),writer);
        }
      } else {
        // ELSE use writer for Record
        writer = (PersistentDatumWriter<?>) writerMap.get(schema.getFullName());
        if (writer == null) {
          writer = new PersistentDatumWriter(schema,false);// ignore dirty bits
          writerMap.put(schema.getFullName(),writer);
        }
      }
      
      BinaryEncoderWithStream encoder = encoders.get();
      if (encoder == null) {
        encoder = new BinaryEncoderWithStream(new ByteArrayOutputStream());
        encoders.set(encoder);
      }
      //reset the buffers
      ByteArrayOutputStream os = (ByteArrayOutputStream) encoder.getOut();
      os.reset();
      
      writer.write(schema,o, encoder);
      encoder.flush();
      return os.toByteArray();
    default: throw new RuntimeException("Unknown type: "+type);
    }
  }
}
