/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.mc.mgmt;


import java.lang.System.Logger;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * Module constants and package-private helper methods. 
 */
public class McMgmtConstants {

  // no one calls
  private McMgmtConstants() {  }
  
  
  /**
   * By default, microchain-specific table names begin with this prefix.
   */
  public final static String SLDG_PREFIX = "sldg_";
  
  public final static String COMMIT_TABLE_PRE_SUBFIX = "cc_";
  
  
  public final static String LOG_NAME = "microchain-mgmt";
  
  
  static Logger getLogger() {
    return System.getLogger(LOG_NAME);
  }
  
  
  static Optional<String> normalize(Optional<String> arg) {
    return normalize(arg, true);
    
  }
  
  
  static Optional<String> normalize(Optional<String> arg, boolean trim) {
    if (arg == null)
      return Optional.empty();
    if (trim)
      arg = arg.map(String::trim);
    
    return arg.filter(Predicate.not(String::isBlank));
    
  }
  
  static void checkUri(Optional<String> uri) throws IllegalArgumentException {
    if (uri.isPresent())
      checkUri(uri.get());
  }
  
  static void checkUri(String uri) throws IllegalArgumentException {
    toUri(uri);
  }
  static URI toUri(String uri) throws IllegalArgumentException {
    try {
      return new URI(uri);
    } catch (URISyntaxException usx) {
      throw new IllegalArgumentException(
          "malformed URI '%s' -- detail: %s".formatted(uri, usx.getMessage()));
    }
  }

}



