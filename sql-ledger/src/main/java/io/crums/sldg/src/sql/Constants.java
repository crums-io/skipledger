/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.src.sql;


import java.lang.System.Logger.Level;

/**
 * 
 */
public class Constants {
  // no-one calls
  private Constants() {  }
  
  
  public final static String LOG_NAME = "io.crums.sql-ledger";
  
  private static void log(Level level, String message) {
    System.getLogger(LOG_NAME).log(level, message);
  }
  
  public static void logWarning(String message) {
    log(Level.WARNING, message);
  }
  
  public static void logError(String message) {
    log(Level.ERROR, message);
  }
  
  
}
