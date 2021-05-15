/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.src;


/**
 * Data type for a column value in a row. The classification here concerns
 * how data is hashed, not the exact data type.
 */
public enum ColumnType {
  
  /**
   * Signed long. All numeric values are to be mapped to {@code long}s.
   * Floating point numbers are not supported. We want fixed precision.
   * Therefore, if a column in a source table in a database is a floating point value,
   * it must somehow be mapped to an integral value.
   */
  NUMBER(4, "L"),
  /**
   * UTF-8 string.
   */
  STRING(3, "S"),
  /**
   * Sequence of byte literals, like a blob but hopefully not too large.
   * In fact, the smaller the better.
   */
  BYTES(2, "B"),
  
  /**
   * A SHA-256 hash of some other value.
   */
  HASH(1, "H"),
  
  /**
   * A null value is marked specially.
   */
  NULL(0, "NULL");
  
  
  public static ColumnType forCode(byte code) {
    switch (code) {
    case 0:   return NULL;
    case 1:   return HASH;
    case 2:   return BYTES;
    case 3:   return STRING;
    case 4:   return NUMBER;
    default:
      throw new IllegalArgumentException("unknown code " + code);
    }
  }
  
  
  private final byte code;
  private final String symbol;
  
  private ColumnType(int code, String symbol) {
    this.code = (byte) code;
    this.symbol = symbol;
  }
  
  
  public byte code() {
    return code;
  }
  
  public String symbol() {
    return symbol;
  }
  
  
  
  public boolean isOpaque() {
    switch (this) {
    case NULL:
    case HASH:
      return true;
    default:
      return false;
    }
  }

}
