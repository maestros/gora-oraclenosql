package org.apache.gora.oracle;


import oracle.kv.*;
import org.apache.gora.GoraTestDriver;
import org.apache.gora.oracle.store.OracleStore;

import java.io.IOException;
import java.util.Properties;

import org.apache.gora.util.GoraException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Author: Apostolos Giannakidis
 * Date: 7/3/13
 * Driver to set up an embedded Oracle database instance for use in our
 * unit tests.
 */
public class GoraOracleTestDriver extends GoraTestDriver {

  private static Logger log = LoggerFactory.getLogger(GoraOracleTestDriver.class);
  private static String storeName = "kvstore";
  private static String hostName = "localhost";
  private static String hostPort = "5000";

  //milliseconds to sleep after the server process executes
  private final long MILLISTOSLEEP = 8000;

  //number of times to retry to start the service, in case it fails to start
  private final int TIMESTORETRY = 3;

  private static KVStore kvstore;    // reference to the kvstore

  Process proc;   // reference to the kvstore process

  /**
   * Constructor
   */
  public GoraOracleTestDriver() {
    super(OracleStore.class);
  }

  @Override
  public void setUpClass() throws Exception {
    super.setUpClass();
    log.info("Initializing Oracle NoSQL driver.");
    initOracleNoSQLSever();
    createKVStore();
  }

  @Override
  public void tearDownClass() throws Exception {
    super.tearDownClass();

    if (proc != null) {
      proc.destroy();
      proc = null;
      log.info("Process killed");
    }

    log.info("Finished Oracle NoSQL driver.");
  }

  /**
   * Initiate the Oracle NoSQL server on the default port.
   * Waits MILLISTOSLEEP seconds in order for the service to start.
   * @return
   * @throws IOException
   */
  private void initOracleNoSQLSever() throws IOException {
    log.info("initOracleNoSQLSever started");

    if (proc != null)
      proc.destroy();

    proc = null;
    /* Spawn a new process in order to start the Oracle NoSQL service. */
    proc = Runtime.getRuntime().exec(new String[]{"java","-jar","lib-ext/kv-2.0.39/kvstore.jar", "kvlite"});

    try {
      Thread.sleep(MILLISTOSLEEP); // sleep for MILLISTOSLEEP in order for the service to be started.
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    if (proc == null)
      log.info("Server not started");
    else
      log.info("Server started");

    log.info("initOracleNoSQLSever finished");
  }


  /**
   * Creates the Oracle NoSQL store and returns a specific object
   * @return
   * @throws IOException
   */
  private KVStore createKVStore() throws IOException {
    log.info("createKVStore started");

    if (kvstore!=null){
      log.info("kvstore was not null. Closing the kvstore...");
      kvstore.close();
      kvstore=null;
    }

    log.info("storeName:"+storeName+", host:"+hostName+":"+hostPort);

    boolean started = false;
    for(int i=1;i<=TIMESTORETRY;i++){

      try {
        kvstore = KVStoreFactory.getStore  // create the kv store
                (new KVStoreConfig(storeName, hostName + ":" + hostPort));

        started = true;
      }catch (FaultException fe){
        log.info("Service seems to be down: "+fe.getMessage());
        log.info("Trying to restart the service. Retry:"+i);
        initOracleNoSQLSever(); // start the service
        continue; // and loop again to try to connect
      }

      if (started)
        break;
    }

    if (kvstore == null){
      log.error("KVStore was not opened.");
      log.error("Terminated because Oracle NoSQL service cannot be started.");
      System.exit(-1);
    }
    else
      log.info("KVStore opened: "+kvstore.toString());

    log.info("kvstore returned");
    return kvstore;
  }

  @Override
  protected void setProperties(Properties properties) {
    super.setProperties(properties);
  }

  public byte[] get(Key myKey){
    ValueVersion vv = null;

    vv = kvstore.get(myKey);

    if (vv != null) {
      Value value = vv.getValue();
      return value.getValue();
    }
    else
      return null;
  }

  public KVStore getKvstore(){
    return kvstore;
  }

}
