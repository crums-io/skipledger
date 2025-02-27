/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.func;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import io.crums.sldg.reports.pdf.input.NumberArg;

/**
 * Abstraction of number functions with zero or many arguments.
 * In the event the function takes zero arguments, it also doubles
 * as a number {@linkplain Supplier supplier}.
 */
public interface NumFunc extends Supplier<Number>, NumberArg.Collector {
  
  
  public static NumFunc of(Number value) {
    Objects.requireNonNull(value, "null value");
    return new NumFunc() {
      
      @Override
      public int getArgCount() {
        return 0;
      }
      
      @Override
      public Number eval(List<Number> args) throws IllegalArgumentException {
        assertArgCount(args);
        return value;
      }
      
      @Override
      public Number eval() {
        return value;
      }
    };
  }
  
  
  
  
  
  

  /**
   * Returns the number of arguments this instance's {@linkplain #eval(List)} takes.
   * 
   * @return &ge; 0
   */
  int getArgCount();
  
  /**
   * Determines whether the argument count is zero. If this
   * method returns {@code true}, then either {@linkplain #get()} or
   * {@linkplain #eval()} be safely
   * be invoked (w/o raising an {@code UnsupportedOperationException}).
   * 
   * @return {@code getArgCount() == 0}
   */
  default boolean isSupplied() {
    return getArgCount() == 0;
  }
  
  @Override
  default void collectNumberArgs(Collection<NumberArg> collector) {  }
  
  
  /**
   * Returns the result of the computation.
   * 
   * @param args of size {@linkplain #getArgCount()}
   * @return not null
   * @throws IllegalArgumentException if {@code getArgCount() != args.size()}
   */
  Number eval(List<Number> args) throws IllegalArgumentException;
  
  /**
   * Returns the result of the computation with no arguments.
   * 
   * @return {@code eval(List.of())}
   * @throws UnsupportedOperationException if {@code !isSupplied()}
   * @see #isSupplied()
   */
  default Number eval() throws UnsupportedOperationException {
    if (!isSupplied())
      throw new UnsupportedOperationException(
          "func takes arguments (" + getArgCount() + ")");
    return eval(List.of());
  }
  
  /**
   * Returns the result of the computation with no arguments.
   * 
   * @return {@code eval()}
   * @throws UnsupportedOperationException if {@code !isSupplied()}
   * @see #isSupplied()
   */
  @Override
  default Number get() throws UnsupportedOperationException {
    return eval();
  }
  
  
  
  /**
   * Convenience method for both implementor and user.
   * 
   * @param args    the arguments (invoked on {@linkplain #eval(List)}
   * 
   * @throws IllegalArgumentException if {@code args.size() != getArgCount()}
   */
  default void assertArgCount(List<Number> args) throws IllegalArgumentException {
    if (args.size() != getArgCount())
      throw new IllegalArgumentException (
          "arg count mismatch: expected " + getArgCount() + "; given " + args.size());
  }

}
