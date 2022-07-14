/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.model.func;


import static io.crums.util.PrimitiveNumber.isSettable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

import io.crums.util.PrimitiveNumber;
import io.crums.util.PrimitiveNumber.Settable;

/**
 * A numeric function "specification" using a composition of a tree of {@linkplain NumNode}s
 * and one or more {@linkplain PrimitiveNumber.Settable settable number}s as the value of
 * leaves in that tree.
 * 
 * <h3>Argument Order and Evaluation</h3>
 * <p>
 * Arguments are ordered (left to right) by the order of discovery in pre-order traversal.
 * Evaluation, too, is in pre-order, but that only matters for debugging.
 * </p>
 * <p>
 * <em>Not thread safe!</em>
 * </p>
 */
public class NumberFunc  {
  
  
  private NumNode root;
  private List<Settable> arguments; 
  
  /**
   * On construction, an instance <em>discovers</em> is arguments by examining
   * the {@code Number} type in the leaves of the tree.
   */
  public NumberFunc(NumNode root) {
    this.root = Objects.requireNonNull(root, "null root");
    this.arguments = 
        Collections.unmodifiableList(
            collectArgs(new ArrayList<>(4), root));
    if (arguments.isEmpty())
      throw new IllegalArgumentException("no settable numbers found in evaluation tree");
  }
  
  
  /**
   * Collects the <em>settable</em> numbers in a pre-order traversal
   * of the evaluation tree.
   */
  private List<Settable> collectArgs(List<Settable> collected, NumNode cursor) {
    if (cursor.isLeaf()) {
      Number value = cursor.value();
      if (isSettable(value))
        collected.add((Settable) value);
    } else {
      for (var n : cursor.asBranch().children())
        collectArgs(collected, n);
    }
    return collected;
  }
  
  
  /**
   * Returns the evaluation tree. Used to write JSON (or maybe some other format in the future).
   *
   * @return not null
   */
  public NumNode evaluationTree() {
    return root;
  }
  
  
  /**
   * Returns the number of argument this instance takes.
   * 
   * @return &ge; 1
   */
  public int getArgCount() {
    return arguments.size();
  }
  
  
  /**
   * Returns this instance as a single argument number function.
   * 
   * @throws UnsupportedOperationException if {@linkplain #getArgCount()}{@code != 1}
   */
  public Function<Number, Number> asFunc() throws UnsupportedOperationException {
    assertArgCount(1);
    return this::evalImpl1;
  }
  

  /**
   * Returns this instance as a 2-argument number function.
   * 
   * @throws UnsupportedOperationException if {@linkplain #getArgCount()}{@code != 2}
   */
  public BiFunction<Number, Number, Number> asBiFunc() throws UnsupportedOperationException {
    assertArgCount(2);
    return this::evalImpl2;
  }
  
  
  /**
   *
   * @throws UnsupportedOperationException if {@linkplain #getArgCount()}{@code != 1}
   */
  public Number eval(Number arg) throws UnsupportedOperationException {
    assertArgCount(1);
    return evalImpl1(arg);
  }
  
  
  private Number evalImpl1(Number arg) {
    arguments.get(0).setValue(arg);
    return root.value();
  }
  

  /**
   * 
   * @throws UnsupportedOperationException if {@linkplain #getArgCount()}{@code != 2}
   */
  public Number eval(Number a, Number b) {
    assertArgCount(2);
    return evalImpl2(a, b);
  }
  
  private Number evalImpl2(Number a, Number b) {
    arguments.get(0).setValue(a);
    arguments.get(1).setValue(a);
    return root.value();
  }
  
  
  
  public Number eval(Number...numbers) throws UnsupportedOperationException {
    assertArgCount(numbers.length);
    for (int index = 0; index < numbers.length; ++index)
      arguments.get(index).setValue(numbers[index]);
    return root.value();
  }
  
  
  
  
  
  
  
  private void assertArgCount(int expected) {
    int count = arguments.size();
    if (count != expected)
      throw new UnsupportedOperationException(
          "arg count mismatch: expected " + expected + "; given " + count);
  }

}




























