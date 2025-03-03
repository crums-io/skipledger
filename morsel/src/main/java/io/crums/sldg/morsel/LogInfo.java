/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.morsel;

import java.net.URI;

/**
 * 
 */
public class LogInfo extends LedgerInfo {
  
  

  /**
   * 
   * 
   * @param alias       locally unique name
   * @param uri         optional (may be {@code null})
   * @param desc        optional description ({@code null} or blank counts
   *                    for naught)
   */
  public LogInfo(String alias, URI uri, String desc) {
    super(LedgerType.LOG, alias, uri, desc);
  }

}
