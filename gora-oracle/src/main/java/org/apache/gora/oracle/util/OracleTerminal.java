package org.apache.gora.oracle.util;

import oracle.kv.*;
import org.apache.gora.oracle.query.OracleQuery;
import org.apache.gora.oracle.store.OracleStore;
import org.apache.gora.query.Query;

import java.lang.String;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Utility application that provides a "terminal" to the Oracle NoSQL server.
 * It provides specific capabilities to the user.
 * Also, it is aware of the Gora-Oracle data model, in order to retrieve
 * the collection of key/value pairs of a specific persistent object.
 * @author Apostolos Giannakidis
 */
public class OracleTerminal {
  private final KVStore store;  //the kvstore handle

  private static final String USAGE = "OracleTerminal \n"+
          "           -dumpDB\n" +
          "           -get <key>\n" +
          "           -truncate \n" +
          "           -countKeys [<PrimaryKeys>]\n";

  /**
   * Runs the OracleTerminal command line program.
   */
  public static void main(String args[]) {

    if(args.length < 1) {
      System.err.println(USAGE);
      System.exit(1);
    }

    try {
      OracleTerminal example = new OracleTerminal(args);

      if (args[0].equals("dumpDB")){
        int total = example.dumpDB(null);
        System.out.println("Total keys: "+total);
      }
      else if (args[0].equals("get")){
        System.out.println("Total keys: "+example.get(args[1]));
      }
      else if (args[0].equals("countKeys")){
        if (args.length<2)
          System.out.println("Total keys: "+example.dumpDB(null));
        else
          System.out.println("Total keys: "+example.dumpDB(args[1]));
      }
      else if (args[0].equals("truncate")){
        example.deleteAll();
      }
      else {
        System.err.println(USAGE);
        System.exit(1);
      }

      example.close();
    } catch (RuntimeException e) {
      e.printStackTrace();
    }
  }

  /**
   * Parses command line args and opens the KVStore.
   */
  OracleTerminal(String[] argv) {
    String storeName = "kvstore";
    String hostName = "localhost";
    String hostPort = "5000";
    store = KVStoreFactory.getStore
            (new KVStoreConfig(storeName, hostName + ":" + hostPort));
  }

  /**
   * Creates an Oracle NoSQL Key based on the String full key path.
   * @param fullKey the full key based on which the Oracle NoSQL Key is created
   * @return the Oracle NoSQL Key
   */
  public static Key createKey(String fullKey){

    if (fullKey==null){
      System.err.println("Invalid fullKey: fullKey was null.");
      return null;
    }

    Key returnKey;
    List<String> majorComponents = new ArrayList<String>();

    if (fullKey.contains("/")){
      String majorComponentStrings[] = fullKey.split("/");

      String persistenseKey = majorComponentStrings[majorComponentStrings.length-1];
      majorComponentStrings[majorComponentStrings.length-1] = OracleUtil.encodeKey(persistenseKey);

      Collections.addAll(majorComponents, majorComponentStrings);
      returnKey = Key.createKey(majorComponents);
    }
    else {
      returnKey = Key.createKey(fullKey);
    }

    return returnKey;
  }

  /**
   * Wrapper method that closes the kv store.
   */
  void close(){
    store.close();
  }

  /**
   * Prints all the key/value pairs that are descendants of the parent key.
   * If the parentKey is null, then the whole database is dumped.
   * @param parentKey the parent key
   * @return the number of key/value pairs that were retrieved.
   */
  int dumpDB(String parentKey) {
    int count=0;
    Key OracleParentKey = null;

    if (parentKey!=null){
      OracleParentKey = createKey(parentKey);
      System.out.println(OracleParentKey.toString());
    }

    try {
      Iterator iterator = store.storeIterator (Direction.UNORDERED,
              0, OracleParentKey, null, Depth.PARENT_AND_DESCENDANTS);
      while (iterator.hasNext ()) {
        Object object = iterator.next ();
        KeyValueVersion keyValueVersion = (KeyValueVersion) object;
        String key =  new String (keyValueVersion.getKey().toString());
        String value = new String (keyValueVersion.getValue().getValue());
        System.out.println (key + " : "  + value);
        count++;
      }

    } catch (Exception ex) {
      System.err.println("Error: " + ex.toString());
    }
    return count;
  }

  /**
   * Retrieves and prints all the key/value pairs
   * that are related to the given persistent key.
   * Note that the values are displayed as is.
   * Which means that they are not deserialised.
   * @param key
   * @return
   */
  int get(String key) {
    int count=0;
    Key myKey = createKey(key);
    System.out.println(myKey);
    Iterator<KeyValueVersion> i =
            store.multiGetIterator(Direction.FORWARD, 0, myKey, null, null);
    while (i.hasNext()) {
      KeyValueVersion obj = i.next();
      Key k = obj.getKey();
      String v = new String(obj.getValue().getValue());
      System.out.println(k.toString()+" : "+v);
      count++;
    }
    return count;
  }

  /**
   * Iterates the kv store using the multiGetIterator and
   * unconditionally deletes each record based on the key.
   */
  public void deleteAll() {
    System.out.println("Begin deleting");

    try {
      while (true){
        Iterator<Key> i = store.storeKeysIterator
                (Direction.UNORDERED, 1, null,
                        null, Depth.DESCENDANTS_ONLY);
        if (!i.hasNext()) {
          break;
        }
        Key descendant = Key.createKey(i.next().getMajorPath());

        store.multiDelete(descendant, null,
                Depth.PARENT_AND_DESCENDANTS);
      }

    } catch (DurabilityException de) {
      de.printStackTrace();
    } catch (RequestTimeoutException rte) {
      rte.printStackTrace();
    } catch (FaultException fe) {
      fe.printStackTrace();
    }

    System.out.println("Finished deleting");
  }
}