package org.apache.gora.oracle.store;

/**
 * Author: Apostolos Giannakidis
 */
public class OracleStoreConstants {

  public static final String DURABILITY_SYNCPOLICY = "durability.syncpolicy";
  /**
   * The mapping file to create the tables from
   */
  public static final String DEFAULT_MAPPING_FILE = "gora-oracle-mapping.xml";
  public static final String DURABILITY_REPLICAACKPOLICY = "durability.replicaackpolicy";
  public static final String CONSISTENCY = "consistency";
  public static final String TIME_UNIT = "time.unit";
  public static final String REQUEST_TIMEOUT = "request.timeout";
  public static final String READ_TIMEOUT = "read.timeout";
  public static final String OPEN_TIMEOUT = "open.timeout";
  public static final String STORE_NAME = "storename";
  public static final String HOST_NAME_PORT = "hostnameport";
  public static final String PRIMARYKEY_TABLE_NAME = "primarykey_tablename";
  public static final String DEFAULT_STORE_NAME = "kvstore";
  public static final String DEFAULT_HOST_NAME_PORT = "localhost:5000";
  public static final String DEFAULT_PRIMARYKEY_TABLE_NAME = "PrimaryKeys";
  public static final String PROPERTIES_SEPARATOR = ",";
}
