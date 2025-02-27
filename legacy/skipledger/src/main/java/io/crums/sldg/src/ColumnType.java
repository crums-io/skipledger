/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.src;

import java.util.Objects;

/**
 * Data type for a column value in a row. The classification is supposed to map a broad
 * range of data types to a smaller subset. The goal here is to convey <em>some</em> semantic
 * context ({@linkplain #DATE}, {@linkplain #DOUBLE}, {@linkplain #LONG}, {@linkplain #STRING},
 * {@linkplain #HASH}), while also spelling out (elsewhere), how they're hashed.
 */
public enum ColumnType {
  
  
  /**
   * Date/time.
   */
  DATE(7, "T"),
  /**
   * Signed floating point value. (A bit concerned about
   * equality tests, read consistency with <code>0 +/-</code> values, that sort of thing.)
   * Including this type for now although it will likely be deprecated soon.
   */
  DOUBLE(6, "D"),
  
  /**
   * Signed long. All fixed precision numeric values are to be mapped to {@code long}s;
   * booleans are mapped to 0 and 1, for {@code false} and {@code true}, resp.
   */
  LONG(5, "L"),
  /**
   * UTF-8 string.
   */
  STRING(4, "S"),
  /**
   * Sequence of bytes, like a blob but hopefully not too large.
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
  NULL(1, "NUL");
  
  
  public static ColumnType forCode(int code) {
    switch (code) {
    case 1:   return NULL;
    case 2:   return HASH;
    case 3:   return BYTES;
    case 4:   return STRING;
    case 5:   return LONG;
    case 6:   return DOUBLE;
    case 7:   return DATE;
    default:
      throw new IllegalArgumentException("unknown code " + code);
    }
  }
  
  
  public static ColumnType forSymbol(String symbol) {
    Objects.requireNonNull(symbol, "null symbol arg");
    switch (symbol) {
    case "NUL":   return NULL;
    case "H":     return HASH;
    case "B":     return BYTES;
    case "S":     return STRING;
    case "L":     return LONG;
    case "D":     return DOUBLE;
    case "T":     return DATE;
    default:
      throw new IllegalArgumentException("unknown symbol: '" + symbol + "'");
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
  
  
  public boolean isLong() {
    return this == LONG;
  }
  
  public boolean isDate() {
    return this == DATE;
  }
  
  public boolean isDouble() {
    return this == DOUBLE;
  }
  
  /**
   * Determines whether a {@linkplain ColumnValue} with this
   * {@linkplain ColumnValue#getType() type} returns a
   * {@code java.lang.Number} for its {@linkplain ColumnValue#getValue()}.
   * @return
   */
  public boolean isNumber() {
    switch (this) {
    case LONG:
    case DOUBLE:
    case DATE:
      return true;
    default:
      return false;
    }
  }

}

