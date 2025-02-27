/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.input;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A <em>primitive</em> type number, annotated with a name.
 * 
 * @see #param()
 */
@SuppressWarnings("serial")
public class NumberArg extends Number implements Supplier<Number> {
  
  
  /**
   * Returns a [param] <em>name</em> to number arg mapping (convenience method
   * for user input).
   * 
   * @throws IllegalArgumentException if {@code args} list has duplicate param names
   * @see NumberArg#param()
   * @see Param#name()
   */
  public static Map<String, NumberArg> toArgMap(List<NumberArg> args)
      throws IllegalArgumentException {
    final int size = args.size();
    switch (size) {
    case 0: return Map.of();
    case 1: return Map.of(args.get(0).param().name(), args.get(0));
    }
    var map = new HashMap<String, NumberArg>(size);
    args.forEach(a -> map.put(a.param().name(), a));
    
    // verify no dups
    if (map.size() != size) {
      var dups = new ArrayList<String>();
      for (var arg : args) {
        var name = arg.param().name();
        boolean dup = map.remove(name) == null;
        if (dup)
          dups.add(name);
      }
      throw new IllegalArgumentException("Duplicate arg names: " + dups);
    }
    return Collections.unmodifiableMap(map);
  }
  
  
  
  /**
   * Binds the given {@code values} to the given list of number arguments
   * {@code args}. <em>All required arguments in the given list must be bound by the given
   * values</em>; otherwise an {@code IllegalArgumentException} is thrown.
   * 
   * @param args    settable arguments (some of which may be required)
   * @param values  map of values set
   */
  public static void bindValues(List<NumberArg> args, Map<String, Number> values) {
    Objects.requireNonNull(args, "null number args list");
    Objects.requireNonNull(values, "null param-name to number map");
    
    var toSet = new HashMap<>(values);
    
    Map<Boolean, List<NumberArg>> reqOptPartition =
        args.stream().collect(Collectors.groupingBy(NumberArg::isRequired));
    
    for (var requiredArg : reqOptPartition.getOrDefault(true, List.of())) {
      var name = requiredArg.param().name();
      var number = toSet.remove(name);
      if (number == null)
        throw new IllegalArgumentException(
            "missing value for required argument '" + name + "'");
      requiredArg.set(number);
    }
    
    if (toSet.isEmpty())
      return;
    
    var optArgMap = toArgMap(reqOptPartition.getOrDefault(false, List.of()));
    for (var entry : toSet.entrySet()) {
      var numArg = optArgMap.get(entry.getKey());
      if (numArg == null)
        throw new IllegalArgumentException(
            "no number argument found with the named value '" + entry.getKey() + "'");
      numArg.set(entry.getValue());
    }
  }
  
  
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
    if (param.getDefaultValue().isEmpty())
      this.value = 0;
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
   * @see Param
   */
  public Param<Number> param() {
    return param;
  }
  
  
  /** @return {@code param().name()} */
  public String name() {
    return param.name();
  }
  
  
  /**
   * Returns the wrapped primitive, or defaulted value. Note a
   * defaulted value comes from {@code param().}{@linkplain Param#defaultValue() defaultValue()}.
   * 
   * @return not null, boxed primitive
   * @see #isDefaulted()
   */
  @Override
  public Number get() {
    return value == null ? param.defaultValue() : value;
  }
  
  /**
   * Sets the instance's value.
   * 
   * @param value   boxed primitive, may only be {@code null} if {@linkplain #param() param} provides default
   * @throws IllegalArgumentException if {@code value} violates above constraints
   */
  public void set(Number value) {
    if (value == null && !param.hasDefault())
      throw new IllegalArgumentException(
          "attempt to set not-defaulted number (named '" + param.name() + "') to null");
    boolean ok =
        value == null ||            // (we checked *param* has default value)
        value instanceof Integer ||
        value instanceof Double ||
        value instanceof Long ||
        value instanceof Float;
    if (!ok)
      throw new IllegalArgumentException("unsupported Number type: " + value.getClass());
    
    this.value = value;
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
  
  /** @return {@code param().isRequired()} */
  public boolean isRequired() {
    return param.isRequired();
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
  }
  

  
  private final static int CH = NumberArg.class.hashCode();

  /** Consistent with {@linkplain #equals(Object)}. */
  @Override
  public final int hashCode() {
    return CH ^ param.name().hashCode();
  }
  
  
  /**
   * Returns the decimal representation of the number. Delegates to the
   * underlying primitive value.
   * 
   * @return {@code getValue().toString()}
   */
  @Override
  public final String toString() {
    return get().toString() + " (param=" + param.name() + ")";
  }

}













