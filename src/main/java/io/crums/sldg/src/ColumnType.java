/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.src;


/**
 * Data type for a column value in a row. The classification here concerns
 * how data is hashed, not the exact data type.
 * 
 * <h3>About the Salted Moniker</h3>
 * <p>
 * There are 2 principal ways to hash a column value: salted and unsalted. The
 * reason why a value may be salted prior to hashing is in order to guard against
 * a rainbow attacks--which if this library is successful, will be quite common.
 * Therefore, unsalted types are expected to tbe exception rather than the norm.
 * Unsalted types have a '0' (zero) appended to their symbol.
 * </p>
 */
public enum ColumnType {
  
  
  /**
   * Signed floating point value. (A bit concerned about
   * equality tests, read consistency with <code>0 +/-</code> values, that sort of thing.)
   * Including this type for now although it will likely be deprecated soon.
   */
  DOUBLE(6, "D"),
  
  /**
   * Signed long. All numeric values are to be mapped to {@code long}s.
   */
  LONG(5, "L"),
  /**
   * UTF-8 string.
   */
  STRING(4, "S"),
  /**
   * Sequence of byte literals, like a blob but hopefully not too large.
   * In fact, the smaller the better.
   */
  BYTES(3, "B"),
  
  /**
   * A SHA-256 hash of some other value.
   */
  HASH(2, "H"),
  
  /**
   * A null value is specially marked.
   */
  NULL(1, "NULL");
  
  
  public static ColumnType forCode(int code) {
    switch (code) {
    case 1:   return NULL;
    case 2:   return HASH;
    case 3:   return BYTES;
    case 4:   return STRING;
    case 5:   return LONG;
    case 6:   return DOUBLE;
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
  
  
  public boolean isNull() {
    return this == NULL;
  }
  
  public boolean isHash() {
    return this == HASH;
  }
  
  public boolean isBytes() {
    return this == BYTES;
  }
  
  public boolean isString() {
    return this == STRING;
  }
  
  
  public boolean isNumber() {
    return this == LONG;
  }

}

