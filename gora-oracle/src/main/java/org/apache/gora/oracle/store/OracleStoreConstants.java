package org.apache.gora.oracle.store;

/**
 * Author: Apostolos Giannakidis
 */
public class OracleStoreConstants {

  public static final String DURABILITY_SYNCPOLICY = "gora.oracle.durability.syncpolicy";
  /**
   * The mapping file to create the tables from
   */
  public static final String DEFAULT_MAPPING_FILE = "gora-oracle-mapping.xml";
  public static final String DURABILITY_REPLICAACKPOLICY = "gora.oracle.durability.replicaackpolicy";
  public static final String CONSISTENCY = "gora.oracle.consistency";
  public static final String TIME_UNIT = "gora.oracle.time.unit";
  public static final String REQUEST_TIMEOUT = "gora.oracle.request.timeout";
  public static final String READ_TIMEOUT = "gora.oracle.read.timeout";
  public static final String OPEN_TIMEOUT = "gora.oracle.open.timeout";
  public static final String STORE_NAME = "gora.oracle.storename";
  public static final String HOST_NAME = "gora.oracle.hostname";
  public static final String HOST_PORT = "gora.oracle.hostport";
  public static final String PRIMARYKEY_TABLE_NAME = "gora.oracle.primarykey_tablename";
  public static final String DEFAULT_STORE_NAME = "kvstore";
  public static final String DEFAULT_HOST_NAME = "localhost";
  public static final String DEFAULT_HOST_PORT = "5000";
  public static final String DEFAULT_PRIMARYKEY_TABLE_NAME = "PrimaryKeys";
}
