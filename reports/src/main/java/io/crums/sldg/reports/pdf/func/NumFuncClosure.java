/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.func;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import io.crums.sldg.reports.pdf.input.NumberArg;
import io.crums.util.Lists;

/**
 * 
 */
public class NumFuncClosure implements NumFunc {
  
  
  /**
   * Value bound to a {@linkplain NumFunc}'s indexed argument.
   */
  public record Value(int index, Number value) implements Comparable<Value> {
    public Value {
      if (index < 0)
        throw new IndexOutOfBoundsException("negative index: " + index);
      Objects.requireNonNull(value, "null value");
    }
    /** Ordered by index. */
    @Override
    public int compareTo(Value other) {
      return Integer.compare(index, other.index);
    }
    
    public boolean hasNumberArg() {
      return value instanceof NumberArg;
    }
    
    public NumberArg numberArg() throws ClassCastException {
      return (NumberArg) value;
    }
  }
  
  
  
  
  
  
  
  private final NumFunc baseFunc;
  private final List<Value> values;
  
  

  /**
   * 
   */
  public NumFuncClosure(NumFunc baseFunc, List<Value>  values) {
    this.baseFunc = Objects.requireNonNull(baseFunc, "null base func");
    Objects.requireNonNull(values, "null values");
    if (values.isEmpty())
      throw new IllegalArgumentException("empty closure values");
    this.values = List.copyOf(values);
    checkArgs();
  }
  
  private void checkArgs() {
    if (!Lists.isSortedNoDups(values))
      throw new IllegalArgumentException(
          "values must be sorted with no duplicate indexes: " + values);
    int lastIndex = values.get(values.size() - 1).index();
    if (lastIndex >= baseFunc.getArgCount())
      throw new IllegalArgumentException(
          "index (" + lastIndex + ") out of bounds (>= " + baseFunc.getArgCount() + ")");
    if (getArgCount() < 0)
      throw new IllegalArgumentException(
          "too many (" + values.size() + ") values. Max: " + baseFunc.getArgCount());
  }


  public List<Value> getBoundValues() {
    return values;
  }
  
  /**
   * Returns the (undecorated) base function.
   */
  public NumFunc getBaseFunc() {
    return baseFunc;
  }

  @Override
  public final int getArgCount() {
    return baseFunc.getArgCount() - values.size();
  }
  
  @Override
  public void collectNumberArgs(Collection<NumberArg> collector) {
    values.stream().filter(Value::hasNumberArg).map(Value::numberArg).forEach(collector::add);
    baseFunc.collectNumberArgs(collector);
  }



  @Override
  public Number eval(List<Number> args) throws IllegalArgumentException {
    assertArgCount(args);
    var filledArgs = new Number[baseFunc.getArgCount()];
    for (
        int ai, vi, index = ai = vi = 0;
        index < filledArgs.length;
        ++index) {
      
      filledArgs[index] =
          isSuppliedIndex(index, vi) ?
              values.get(vi++).value() : args.get(ai++);
    }
    
    return baseFunc.eval(Lists.asReadOnlyList(filledArgs));
  }
  
  
  private boolean isSuppliedIndex(int index, int vIndex) {
    return vIndex < values.size() && values.get(vIndex).index() == index;
  }

}










