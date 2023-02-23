/*
 * Copyright 2023 Babak Farhang
 */
package io.crums.sldg.logs.text;

import java.lang.System.Logger;

/**
 * 
 */
public class LogledgeConstants {

  // never
  private LogledgeConstants() {  }
  
  
  public final static String LOGNAME = "sldg.logs";
  
  public static Logger sysLogger() {
    return System.getLogger(LOGNAME);
  }
  

  public final static int UNUSED_PAD = 2;

  
  public final static String DIR_PREFIX = ".sldg.";

  
  public final static String STATE_PREFIX = "_";
  public final static String STATE_POSTFIX = ".fstate";
  

  public final static String FRONTIERS_FILE = "fhash";
  public final static String OFFSETS_FILE = "eor";


  public final static String FRONTIERS_MAGIC = FRONTIERS_FILE;
  public final static String OFFSETS_MAGIC = OFFSETS_FILE;
  public final static String STATE_MAGIC = "fstate";
  
}
