/*
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

package org.apache.gora.util;

import org.apache.gora.GoraVersionAnnotation;

/**
 * This class finds the package info for Gora and the GoraVersionAnnotation
 * information.
 */
public class VersionInfo {
  private static Package myPackage;
  private static GoraVersionAnnotation version;
  
  static {
    myPackage = GoraVersionAnnotation.class.getPackage();
    version = myPackage.getAnnotation(GoraVersionAnnotation.class);
  }

  /**
   * Get the meta-data for the Gora package.
   * @return
   */
  static Package getPackage() {
    return myPackage;
  }
  
  /**
   * Get the Gora version.
   * @return the Gora version string, eg. "X.Y"
   */
  public static String getVersion() {
    return version != null ? version.version() : "Unknown";
  }
  
  /**
   * Get the subversion revision number for the root directory
   * @return the revision number, eg. "451451"
   */
  public static String getRevision() {
    return version != null ? version.revision() : "Unknown";
  }
  
  /**
   * The date that Gora was compiled.
   * @return the compilation date in unix date format
   */
  public static String getDate() {
    return version != null ? version.date() : "Unknown";
  }
  
  /**
   * The user that compiled Gora.
   * @return the username of the user
   */
  public static String getUser() {
    return version != null ? version.user() : "Unknown";
  }
  
  /**
   * Get the subversion URL for the root Gora directory.
   */
  public static String getUrl() {
    return version != null ? version.url() : "Unknown";
  }

  /**
   * Get the checksum of the source files from which Gora was
   * built.
   **/
  public static String getSrcChecksum() {
    return version != null ? version.srcChecksum() : "Unknown";
  }
  
  /**
   * Returns the buildVersion which includes version, 
   * revision, user and date. 
   */
  public static String getBuildVersion(){
    return VersionInfo.getVersion() + 
    " from " + VersionInfo.getRevision() +
    " by " + VersionInfo.getUser() + 
    " source checksum " + VersionInfo.getSrcChecksum();
  }
  
  public static void main(String[] args) {
    System.out.println("Gora " + getVersion());
    System.out.println("Subversion " + getUrl() + " -r " + getRevision());
    System.out.println("Compiled by " + getUser() + " on " + getDate());
    System.out.println("From source with checksum " + getSrcChecksum());

  }
}
