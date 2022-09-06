/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.func;


import java.util.function.BinaryOperator;

/**
 * Binary operations for boxed number primitives. {@code Short}s and {@code Byte}s
 * are treated as {@code Integer}s.
 * 
 * @see #apply(Number, Number)
 * @see #negate(Number)
 */
public enum NumOp implements BinaryOperator<Number> {
  
  /** Symbol: {@code +} */
  ADD("+"),
  /** Symbol: {@code -} */
  SUBTRACT("-"),
  /** Symbol: {@code *} */
  MULTIPLY("*"),
  /** Symbol: {@code /} */
  DIVIDE("/");
  
  
  
  private final String symbol;
  
  private NumOp(String symbol) {
    this.symbol = symbol;
  }
  
  
  /**
   * Returns the symbol. Used in JSON.
   */
  public String symbol() {
    return symbol;
  }
  
  /**
   * Applies the operation and returns the result. The returned result uses
   * the same widening conversion rules as the primitives.
   * 
   * @return a {@code Double}, {@code Float}, {@code Long}, or {@code Integer}.
   */
  @Override
  public Number apply(Number a, Number b) throws ArithmeticException {
    switch (this) {
    case ADD: return add(a, b, 1);
    case SUBTRACT:  return add(a, b, -1);
    case MULTIPLY:  return multiply(a, b);
    case DIVIDE:    return divide(a, b);
    default:
      throw new RuntimeException("unaccounted enum: " + this);
    }
  }
  
  
  
  
  
  
  
  
  
  /**
   * Determines whether this operation is associative. Recall only an associative
   * operation should be used in a stream <em>reduction</em>.
   * 
   * @return {@code this == ADD || this == MULTIPLY}
   */
  public boolean isAssociative() {
    return this == ADD || this == MULTIPLY;
  }
  
  
  private final static Number ZERO = 0;
  private final static Number ONE = 1;
  
  
  /**
   * Returns the identity value for this operation.
   * @return {@code 0} if additive; {@code 1} if multiplicative
   */
  public Number identity() {
    switch (this) {
    case ADD:
    case SUBTRACT:    return ZERO;
    case MULTIPLY:
    case DIVIDE:      return ONE;
    default:
      throw new RuntimeException("unaccounted enum: " + this);
    }
  }
  
  
  
  /**
   * 
   * @return {@linkplain #MULTIPLY}.{@code apply(number, -1)}
   */
  public static Number negate(Number number) {
    return multiply(number, -1);
  }
  
  
  public static NumOp forSymbol(String symbol) {
    for (var op : values())
      if (op.symbol().equals(symbol))
        return op;
    throw new IllegalArgumentException("symbol: " + symbol);
  }



  private Number add(Number a, Number b, int sign) {
    
    // if there's a double, promote and return
    if (a instanceof Double || b instanceof Double)
      return a.doubleValue() + sign * b.doubleValue();
    
    // no doubles.. 
    if (a instanceof Float || b instanceof Float)
      return a.floatValue() + sign * b.floatValue();
    
    // fixed precision (integral)
    
    if (a instanceof Long || b instanceof Long)
      return a.longValue() + sign * b.longValue();
    else
      return a.intValue() + sign * b.intValue();
  }
  
  
  
  private static Number multiply(Number a, Number b) {

    // if there's a double, promote and return
    if (a instanceof Double || b instanceof Double)
      return a.doubleValue() * b.doubleValue();
    
    // no doubles.. 
    if (a instanceof Float || b instanceof Float)
      return a.floatValue() * b.floatValue();
    
    // fixed precision (integral)
    
    if (a instanceof Long || b instanceof Long)
      return a.longValue() * b.longValue();
    
    return a.intValue() * b.intValue();
  }
  
  
  
  private Number divide(Number a, Number b) {

    // if there's a double, promote and return
    if (a instanceof Double || b instanceof Double)
      return a.doubleValue() / b.doubleValue();
    
    // no doubles.. 
    if (a instanceof Float || b instanceof Float)
      return a.floatValue() / b.floatValue();
    
    // fixed precision (integral)
    
    if (a instanceof Long || b instanceof Long)
      return a.longValue() / b.longValue();
    
    return a.intValue() / b.intValue();
  }
  
  

}
