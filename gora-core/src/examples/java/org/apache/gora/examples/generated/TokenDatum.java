/**
 *Licensed to the Apache Software Foundation (ASF) under one
 *or more contributor license agreements.  See the NOTICE file
 *distributed with this work for additional information
 *regarding copyright ownership.  The ASF licenses this file
 *to you under the Apache License, Version 2.0 (the"
 *License"); you may not use this file except in compliance
 *with the License.  You may obtain a copy of the License at
 *
  * http://www.apache.org/licenses/LICENSE-2.0
 * 
 *Unless required by applicable law or agreed to in writing, software
 *distributed under the License is distributed on an "AS IS" BASIS,
 *WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *See the License for the specific language governing permissions and
 *limitations under the License.
 */

package org.apache.gora.examples.generated;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.HashMap;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Protocol;
import org.apache.avro.util.Utf8;
import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.specific.FixedSize;
import org.apache.avro.specific.SpecificExceptionBase;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificFixed;
import org.apache.gora.persistency.StateManager;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.persistency.impl.StateManagerImpl;
import org.apache.gora.persistency.StatefulHashMap;
import org.apache.gora.persistency.ListGenericArray;

@SuppressWarnings("all")
public class TokenDatum extends PersistentBase {
  public static final Schema _SCHEMA = Schema.parse("{\"type\":\"record\",\"name\":\"TokenDatum\",\"namespace\":\"org.apache.gora.examples.generated\",\"fields\":[{\"name\":\"count\",\"type\":\"int\"}]}");
  public static enum Field {
    COUNT(0,"count"),
    ;
    private int index;
    private String name;
    Field(int index, String name) {this.index=index;this.name=name;}
    public int getIndex() {return index;}
    public String getName() {return name;}
    public String toString() {return name;}
  };
  public static final String[] _ALL_FIELDS = {"count",};
  static {
    PersistentBase.registerFields(TokenDatum.class, _ALL_FIELDS);
  }
  private int count;
  public TokenDatum() {
    this(new StateManagerImpl());
  }
  public TokenDatum(StateManager stateManager) {
    super(stateManager);
  }
  public TokenDatum newInstance(StateManager stateManager) {
    return new TokenDatum(stateManager);
  }
  public Schema getSchema() { return _SCHEMA; }
  public Object get(int _field) {
    switch (_field) {
    case 0: return count;
    default: throw new AvroRuntimeException("Bad index");
    }
  }
  @SuppressWarnings(value="unchecked")
  public void put(int _field, Object _value) {
    if(isFieldEqual(_field, _value)) return;
    getStateManager().setDirty(this, _field);
    switch (_field) {
    case 0:count = (Integer)_value; break;
    default: throw new AvroRuntimeException("Bad index");
    }
  }
  public int getCount() {
    return (Integer) get(0);
  }
  public void setCount(int value) {
    put(0, value);
  }
}
