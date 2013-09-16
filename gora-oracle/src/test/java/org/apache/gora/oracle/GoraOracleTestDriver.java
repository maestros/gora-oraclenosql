package org.apache.gora.oracle;

import oracle.kv.*;
import org.apache.gora.GoraTestDriver;
import org.apache.gora.oracle.store.OracleStore;

import org.apache.commons.io.FileUtils;

import java.io.*;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Driver to set up an embedded Oracle database instance for use in our
 * unit tests.
 * @author Apostolos Giannakidis
 */
public class GoraOracleTestDriver extends GoraTestDriver {

  private static Logger log = LoggerFactory.getLogger(GoraOracleTestDriver.class);
  private static String storeName = "kvstore";  //the default kvstore name
  private static String serverHostName = "localhost"; //the default hostname to connect to
  private static String serverHostPort = "5000";  //the default port to connect to

  //milliseconds to sleep after the server process executes
  private final long MILLISTOSLEEP = 11000;

  //number of times to retry to start the service, in case it fails to start
  private final int TIMESTORETRY = 4;

  private static KVStore kvstore;    // reference to the kvstore

  Process proc; // reference to the kvstore process

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
    initOracleNoSQLSever(); // start the Oracle NoSQL server
    createKVStore();  //connect to the Oracle NoSQL server
  }

  @Override
  public void tearDownClass() throws Exception {
    super.tearDownClass();

    if (proc != null) {
      proc.destroy();
      proc = null;
      log.info("Process killed");
    }

    cleanupDirectoriesFailover();
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

    InputStream stdin = proc.getInputStream();
    InputStreamReader isr = new InputStreamReader(stdin);
    BufferedReader br = new BufferedReader(isr);

    try {
      Thread.sleep(MILLISTOSLEEP);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    if ( (proc == null) )
      log.info("Server not started");
    else
      log.info("Server started");

    log.info("initOracleNoSQLSever finished");
  }


  /**
   * Creates the Oracle NoSQL store and returns a specific object
   * @return the kvstore handle
   * @throws IOException
   */
  private KVStore createKVStore() throws IOException {
    log.info("createKVStore started");

    if (kvstore!=null){
      log.info("kvstore was not null. Closing the kvstore...");
      kvstore.close();
      kvstore=null;
    }

    log.info("storeName:"+storeName+", host:"+ serverHostName +":"+ serverHostPort);

    boolean started = false;
    for(int i=1;i<=TIMESTORETRY;i++){

      try {
        kvstore = KVStoreFactory.getStore  // create the kv store
                (new KVStoreConfig(storeName, serverHostName + ":" + serverHostPort));

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
      try {
        tearDownClass();
      } catch (Exception e) {
        e.printStackTrace();
      }
      System.exit(-1);
    }
    else
      log.info("KVStore opened: "+kvstore.toString());

    log.info("kvstore returned");
    return kvstore;
  }

  /**
   * Setter for Properties. Required by the Gora API.
   * @param properties  the properties object to be set
   */
  @Override
  protected void setProperties(Properties properties) {
    super.setProperties(properties);
  }

  /**
   * Helper method to get the serialised value directly from the database
   * @param myKey the key of the key/value pair
   * @return the fetched, serialised value
   */
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

  /**
   * Helper method to get the kvstore handle
   * @return the kvstore handle
   */
  public KVStore getKvstore(){
    return kvstore;
  }

  /**
   * Simply cleans up Oracle NoSQL's output from the Unit tests.
   * In the case of a failure, it waits 250 msecs and tries again, 3 times in total.
   */
  public void cleanupDirectoriesFailover() {
    int tries = 3;
    while (tries-- > 0) {
      try {
        cleanupDirectories();
        break;
      } catch (Exception e) {
        //ignore exception
        try {
          Thread.sleep(250);
        } catch (InterruptedException e1) {
          //ignore exception
        }
      }
    }
  }

  /**
   * Cleans up Oracle NoSQL's temp base directory.
   * @throws Exception
   *    if an error occurs
   */
  public void cleanupDirectories() throws Exception {
    File dirFile = new File("kvroot");
    if (dirFile.exists()) {
      FileUtils.deleteDirectory(dirFile);
    }
  }

}
