/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.input;


import java.util.Objects;

import io.crums.util.PrimitiveComparator;

/**
 * A <em>primitive</em> type number, annotated with a name.
 * 
 * @see #param()
 */
@SuppressWarnings("serial")
public class NumberArg extends Number {
  
  private final Param<Number> param;
  private final Number value;
  
  /**
   * 
   * @param param
   * @param value
   */
  /**
   * @throws IllegalArgumentException if {@code value} is {@code null} and {@code param} has no default */
  public NumberArg(Param<Number> param, Number value) {
    this(param, value, true);
    boolean ok =
        value == null ||            // (base constructor has checked *param* has default value)
        value instanceof Integer ||
        value instanceof Double ||
        value instanceof Long ||
        value instanceof Float ||
        value instanceof Short ||
        value instanceof Byte;
    if (!ok)
      throw new IllegalArgumentException(
          "unsupported Number type (class: " + value.getClass() + "): " + value);
  }
  
  
  /** @throws IllegalArgumentException if {@code value} is {@code null} and {@code param} has no default */
  public NumberArg(Param<Number> param, Long value) {
    this(param, value, true);
  }

  /** @throws IllegalArgumentException if {@code value} is {@code null} and {@code param} has no default */
  public NumberArg(Param<Number> param, Integer value) {
    this(param, value, true);
  }

  /** @throws IllegalArgumentException if {@code value} is {@code null} and {@code param} has no default */
  public NumberArg(Param<Number> param, Short value) {
    this(param, value, true);
  }

  /** @throws IllegalArgumentException if {@code value} is {@code null} and {@code param} has no default */
  public NumberArg(Param<Number> param, Byte value) {
    this(param, value, true);
  }

  /** @throws IllegalArgumentException if {@code value} is {@code null} and {@code param} has no default */
  public NumberArg(Param<Number> param, Double value) {
    this(param, value, true);
  }

  /** @throws IllegalArgumentException if {@code value} is {@code null} and {@code param} has no default */
  public NumberArg(Param<Number> param, Float value) {
    this(param, value, true);
  }
  
  
  
  /** @param ignore (signature disambiguation hack) */
  private NumberArg(Param<Number> argName, Number value, boolean ignore) {
    this.param = Objects.requireNonNull(argName, "null argName");
    this.value = value;
    if (value == null && argName.defaultValue() == null)
      throw new IllegalArgumentException("missing value for non-default param '" + argName + "'");
  }

  @Override
  public int intValue() {
    return getValue().intValue();
  }

  @Override
  public long longValue() {
    return getValue().longValue();
  }

  @Override
  public float floatValue() {
    return getValue().floatValue();
  }

  @Override
  public double doubleValue() {
    return getValue().doubleValue();
  }
  
  
  /**
   * Returns the parameter (contains name and such).
   * 
   * @return not null
   */
  public Param<Number> param() {
    return param;
  }
  
  
  /**
   * Returns the wrapped primitive, or defaulted value. Note a
   * defaulted value comes from {@code param().}{@linkplain Param#defaultValue() defaultValue()}.
   * 
   * @return not null
   * @see #isDefaulted()
   */
  public Number getValue() {
    return value == null ? param.defaultValue() : value;
  }
  
  
  
  /**
   * Indicates whether the {@linkplain #getValue() value} comes from
   * the param default.
   * 
   * @return {@code false} <b>iff</b> if instance was constructed with a {@code null} value 
   */
  public boolean isDefaulted() {
    return value == null;
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
  
  
  /**
   * Returns the decimal representation of the number. Delegates to the
   * underlying primitive value.
   * 
   * @return {@code getValue().toString()}
   */
  @Override
  public final String toString() {
    return getValue().toString();
  }

}













