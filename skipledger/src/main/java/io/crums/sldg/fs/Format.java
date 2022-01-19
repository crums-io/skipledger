/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg.fs;


/**
 * File formats for external storage of entities such as ledger
 * paths and nuggets.
 */
public enum Format {
  
  /**
   * Binary format. This is the most compact representation (least compressible).
   */
  BINARY,
  /**
   * JSON format. A bit bloated, but human readable.
   */
  JSON;

  
  
  public boolean isJson() {
    return this == JSON;
  }
  
  

  public boolean isBinary() {
    return this == BINARY;
  }

}
