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
package org.apache.gora.solr;

import java.io.File;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.gora.GoraTestDriver;
import org.apache.gora.solr.store.SolrStore;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;

public class GoraSolrTestDriver extends GoraTestDriver {
  JettySolrRunner solr;

  public GoraSolrTestDriver() {
    super(SolrStore.class);
  }

  @Override
  public void setUpClass() throws Exception {
    super.setUpClass();
    solr = new JettySolrRunner("solr","/solr", 9876);
    solr.start();
  }

  @Override
  public void tearDownClass() throws Exception {
    super.tearDownClass();
    if (solr != null) {
      solr.stop();
      solr = null;
    }
    cleanupDirectoriesFailover();
  }

  /**
   * Simply cleans up Solr's output from the Unit tests.
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
   * Cleans up Solr's temp base directory.
   *
   * @throws Exception
   *    if an error occurs
   */
  public void cleanupDirectories() throws Exception {
    File dirFile = new File("solr");
    if (dirFile.exists()) {
      FileUtils.deleteDirectory(dirFile);
    }
  }


  @Override
  protected void setProperties(Properties properties) {
    // TODO Auto-generated method stub
    super.setProperties(properties);
  }

}
