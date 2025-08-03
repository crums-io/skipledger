/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.morsel;

import java.lang.System.Logger;

/**
 * 
 */
public class MorselConstants {
  
  public final static String FILENAME_EXT = ".mrsl";
  
  public final static String LOG_NAME = "io.crums.sldg.morsel";
  
  public static Logger getLogger() {
    return System.getLogger(LOG_NAME);
  }

  // never
  private MorselConstants() {  }

}
