package org.apache.gora.oracle;

import org.apache.gora.GoraTestDriver;
import org.apache.gora.persistency.Persistent;
import org.apache.gora.store.DataStore;
import org.apache.gora.util.GoraException;
import org.apache.gora.oracle.store.OracleStore;

import java.io.IOException;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.kv.KVStore;
import oracle.kv.KVStoreFactory;
import oracle.kv.KVStoreConfig;

/**
 * Author: Apostolos Giannakidis
 * Date: 7/3/13
 * Time: 6:16 AM
 */
public class GoraOracleTestDriver extends GoraTestDriver {

    private static Logger log = LoggerFactory.getLogger(GoraOracleTestDriver.class);
    private static String storeName = "kvstore";
    private static String hostName = "localhost";
    private static String hostPort = "5000";

    private KVStore kvstore;    // reference to the kvstore

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
        createDataStore();
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
     * Creates the Oracle NoSQL store and returns a specific object
     * @return
     * @throws IOException
     */
    protected KVStore createDataStore() throws IOException {
        log.info("createDataStore");

        /* Spawn a new process in order to start the Oracle NoSQL service. */
        proc = Runtime.getRuntime().exec(new String[]{"java","-jar","lib-ext/kv-2.0.39/kvstore.jar", "kvlite"});

        try {
            Thread.sleep(7000); // sleep for a few seconds in order for the service to be started.
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        kvstore = KVStoreFactory.getStore  // create the data store
                (new KVStoreConfig(storeName, hostName + ":" + hostPort));

        if (kvstore == null)
            log.info("KVStore is null");
        else
            log.info("KVStore Opened: "+kvstore.toString());

        return kvstore;
    }

    @Override
    protected void setProperties(Properties properties) {
        super.setProperties(properties);
    }

}
