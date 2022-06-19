/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.reports.pdf.model;


import java.util.Objects;

import io.crums.util.PrimitiveComparator;

/**
 * A <em>primitive</em> type number, annotated with a name.
 * 
 * @see #param()
 */
@SuppressWarnings("serial")
public class NumberArg extends Number {
  
  private final Param<Number> argName;
  private final Number value;
  
  /**
   * 
   * @param argName
   * @param value
   */
  public NumberArg(Param<Number> argName, Number value) {
    this(argName, value, true);
    boolean ok =
        value instanceof Long ||
        value instanceof Integer ||
        value instanceof Double ||
        value instanceof Float ||
        value instanceof Short ||
        value instanceof Byte;
    if (!ok)
      throw new IllegalArgumentException(
          "unsupported Number type (class: " + value.getClass() + "): " + value);
  }
  
  
  /** @throws NullPointerException if an argument is {@code null} */
  public NumberArg(Param<Number> argName, Long value) {
    this(argName, value, true);
  }

  /** @throws NullPointerException if an argument is {@code null} */
  public NumberArg(Param<Number> argName, Integer value) {
    this(argName, value, true);
  }

  /** @throws NullPointerException if an argument is {@code null} */
  public NumberArg(Param<Number> argName, Short value) {
    this(argName, value, true);
  }

  /** @throws NullPointerException if an argument is {@code null} */
  public NumberArg(Param<Number> argName, Byte value) {
    this(argName, value, true);
  }

  /** @throws NullPointerException if an argument is {@code null} */
  public NumberArg(Param<Number> argName, Double value) {
    this(argName, value, true);
  }

  /** @throws NullPointerException if an argument is {@code null} */
  public NumberArg(Param<Number> argName, Float value) {
    this(argName, value, true);
  }
  
  
  
  /** @param ignore (signature disambiguation hack) */
  private NumberArg(Param<Number> argName, Number value, boolean ignore) {
    this.argName = Objects.requireNonNull(argName, "null argName");
    this.value = Objects.requireNonNull(value, "null value");
  }

  @Override
  public int intValue() {
    return value.intValue();
  }

  @Override
  public long longValue() {
    return value.longValue();
  }

  @Override
  public float floatValue() {
    return value.floatValue();
  }

  @Override
  public double doubleValue() {
    return value.doubleValue();
  }
  
  
  /**
   * Returns the paramater (name and such).
   * 
   * @return not null
   */
  public Param<Number> param() {
    return argName;
  }
  
  
  /**
   * Returns the wrapped value.
   * 
   * @return not null
   */
  public Number getValue() {
    return value;
  }
  
  
  /**
   * Not a good idea to compare instances. There will be edge cases
   * where 2 numbers compare equal but still fail this test. Uses
   * {@linkplain PrimitiveComparator#equal(Number, Number)} and
   * {@linkplain PrimitiveComparator#hashCode(Number)}.
   */
  @Override
  public final boolean equals(Object o) {
    return o == this ||
        o instanceof NumberArg arg &&
        PrimitiveComparator.equal(value, arg.value) &&
        hashCode() == arg.hashCode();
  }
  

  /** See {@linkplain PrimitiveComparator#hashCode(Number)}. */
  @Override
  public final int hashCode() {
    return PrimitiveComparator.hashCode(value);
  }

}













