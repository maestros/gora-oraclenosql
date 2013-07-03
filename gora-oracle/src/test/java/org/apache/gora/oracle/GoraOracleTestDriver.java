package org.apache.gora.oracle;

import org.apache.gora.GoraTestDriver;
import org.apache.gora.persistency.Persistent;
import org.apache.gora.store.DataStore;
import org.apache.gora.util.GoraException;
import org.apache.gora.oracle.store.OracleStore;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User: apostolosgiannakidis
 * Date: 7/3/13
 * Time: 6:16 AM
 * To change this template use File | Settings | File Templates.
 */
public class GoraOracleTestDriver extends GoraTestDriver {

    private static Logger log = LoggerFactory.getLogger(GoraOracleTestDriver.class);

    /**
     * Constructor for this class.
     */
    public GoraOracleTestDriver() {
        super(OracleStore.class);
    }

    @Override
    public void setUpClass() throws Exception {
        super.setUpClass();
    }

    @Override
    public void tearDownClass() throws Exception {
        super.tearDownClass();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Override
    protected void setProperties(Properties properties) {
        super.setProperties(properties);
    }

    @Override
    public <K, T extends Persistent> DataStore<K, T> createDataStore(Class<K> keyClass, Class<T> persistentClass) throws GoraException {
        return super.createDataStore(keyClass, persistentClass);
    }

    @Override
    public Class<?> getDataStoreClass() {
        return super.getDataStoreClass();
    }
}
