/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.input;


import java.util.Collection;
import java.util.Objects;
import java.util.function.Supplier;

import io.crums.util.PrimitiveComparator;

/**
 * A <em>primitive</em> type number, annotated with a name.
 * 
 * @see #param()
 */
@SuppressWarnings("serial")
public class NumberArg extends Number implements Supplier<Number> {
  
  private final static int CH = NumberArg.class.hashCode();
  
  /**
   * A {@code NumberArg} collector. {@linkplain NumberArg}s are designed to be
   * sprinkled about constructs and expressions the same way regular numbers are.
   * This interface standardizes how components gather there user-supplied arguments.
   */
  public interface Collector {
    /** Adds an instance's arguments to the given collection. */
    void collectNumberArgs(Collection<NumberArg> collector);
  }
  
  
  
  
  
  private final Param<Number> param;
  private Number value;
  
  /**
   * @param param   not null
   * @param value   an {@code int, long, float}, or {@code double}, may be {@code null}
   *                if {@code param} {@linkplain #isDefaulted()}
   * @throws IllegalArgumentException if {@code value} is {@code null} and {@code param} has no default
   */
  public NumberArg(Param<Number> param, Number value) {
    this.param = Objects.requireNonNull(param, "null param");
    set(value);
  }

  /**
   * Creates a new instance whose value is either the {@code param}'s
   * default, if any; {@code 0}, otherwise.
   * 
   * @param param not null
   */
  public NumberArg(Param<Number> param) {
    this.param = Objects.requireNonNull(param, "null param");
    set(param.getDefaultValue().isEmpty() ? 0 : null);
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
  public NumberArg(Param<Number> param, Double value) {
    this(param, value, true);
  }

  /** @throws IllegalArgumentException if {@code value} is {@code null} and {@code param} has no default */
  public NumberArg(Param<Number> param, Float value) {
    this(param, value, true);
  }
  
  
  
  
  
  /** @param ignore (signature disambiguation hack) */
  private NumberArg(Param<Number> param, Number value, boolean ignore) {
    this.param = Objects.requireNonNull(param, "null param");
    this.value = value;
    
    if (value == null && !param.hasDefault())
      throw new IllegalArgumentException(
          "missing value for non-default param '" + param + "'");
  }
  
  
  
  
  

  @Override
  public int intValue() {
    return get().intValue();
  }

  @Override
  public long longValue() {
    return get().longValue();
  }

  @Override
  public float floatValue() {
    return get().floatValue();
  }

  @Override
  public double doubleValue() {
    return get().doubleValue();
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
  @Override
  public Number get() {
    return value == null ? param.defaultValue() : value;
  }
  
  public void set(Number value) {
    if (value == null && !param.hasDefault())
      throw new IllegalArgumentException("attempt to set non defaulted to null");
    boolean ok =
        value == null ||            // (we checked *param* has default value)
        value instanceof Integer ||
        value instanceof Double ||
        value instanceof Long ||
        value instanceof Float;
    if (!ok)
      throw new IllegalArgumentException("unsupported Number type: " + value.getClass());
  }
  
  
  /**
   * Indicates whether the {@linkplain #get() value} comes from
   * the param default.
   * 
   * @return {@code false} <b>iff</b> if instance was constructed with a {@code null} value 
   */
  public boolean isDefaulted() {
    return value == null;
  }
  
  
  /**
   * Equality is based on param name.
   * 
   * @see #param()
   * @see Param#name()
   */
  @Override
  public final boolean equals(Object o) {
    return o == this ||
        o instanceof NumberArg arg &&
        arg.param().name().equals(param.name());
//        PrimitiveComparator.equal(value, arg.value);
  }
  

  /** Consistent with {@linkplain #equals(Object)}. */
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
    return get().toString();
  }

}













