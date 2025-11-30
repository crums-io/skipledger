/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.mc.fx;

import java.sql.Types;

/**
 * Classification of SQL {@linkplain Types}. This is used for
 * "what-to-do" (branching) on SQL type discovery.
 */
public enum SqlGenType {
  
  /**
   * String (or string-like) value. 
   */
  STRING,
  /**
   * Whole number.
   */
  INTEGRAL,
  /**
   * Fixed-precision, fractional.
   */
  FRACTIONAL,
  /**
   * Floating point (inexact) precision.
   */
  FLOATING,
  /**
   * Bit or boolean.
   */
  BOOL,
  /**
   * Date (or date-like).
   */
  DATE,
  /**
   * Binary data guaranteed to fit in memory.
   */
  BYTES,
  /**
   * Large objects (binary or char data).
   */
  LOB,
  /**
   * Not dealt with.
   */
  OTHER;
  
  
  public static SqlGenType forSqlType(int sqlType) {
    switch (sqlType) {
    
    case Types.CHAR:
    case Types.VARCHAR:
    case Types.LONGVARCHAR:
    case Types.NCHAR:
    case Types.NVARCHAR:
    case Types.LONGNVARCHAR:
      
      return STRING;
    
    case Types.TINYINT:
    case Types.SMALLINT:
    case Types.INTEGER:
    case Types.BIGINT:
      return INTEGRAL;

    case Types.DECIMAL:
    case Types.NUMERIC:
      return FRACTIONAL;
      
    case Types.REAL:
    case Types.FLOAT:
    case Types.DOUBLE:
      return FLOATING;
      
    case Types.BINARY:
    case Types.VARBINARY:
    case Types.LONGVARBINARY:
      return BYTES;
      
    case Types.DATE:
    case Types.TIME:
    case Types.TIMESTAMP:
    case Types.TIMESTAMP_WITH_TIMEZONE:
    case Types.TIME_WITH_TIMEZONE:
      return DATE;
      
    case Types.BOOLEAN:
    case Types.BIT:
      return BOOL;
      

    case Types.BLOB:
    case Types.CLOB:
    case Types.NCLOB:
      return LOB;
      
    default:
      return OTHER;
    }
  }
  
  
  public boolean isIntegral() {
    return this == INTEGRAL;
  }
  
  
  public boolean isString() {
    return this == STRING;
  }
  
  
  public boolean isFractional() {
    return this == FRACTIONAL;
  }
  
  
  public boolean isDate() {
    return this == DATE;
  }
  
  
  public boolean isBool() {
    return this == BOOL;
  }
  
  
  public boolean isNumber() {
    switch (this) {
    case INTEGRAL:
    case FRACTIONAL:
    case FLOATING:
      return true;
      
    default:
      return false;
    }
  }

}

















