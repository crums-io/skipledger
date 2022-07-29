/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.func;


import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;


/**
 * A numeric function "specification" using a composition of a tree of {@linkplain NumNode}s.
 * At least one the nodes in the tree must be an {@linkplain NumNode.ArgNode ArgNode}
 * instance.
 * 
 * <h3>Quirk</h3>
 * <p>
 * This class does not recognize {@code NumberArg} instances as a special {@code Number}
 * type, so its {@linkplain NumFunc#collectNumberArgs(Collection)} does nothing. This was (not)
 * done to reduce cognitive load (on the author) while refactoing stuff. So this'll likely
 * change. (TODO)
 * </p>
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
public class BaseNumFunc implements NumFunc {
  
  
  public static BaseNumFunc divideBy(Number number) {
    return
        new BaseNumFunc(
            NumNode.newBranch(
                NumOp.DIVIDE,
                List.of(NumNode.newArgLeaf(), NumNode.newLeaf(number))));
        
  }
  
  public static BaseNumFunc biFunction(NumOp op) {
    return
        new BaseNumFunc(
            NumNode.newBranch(
                op,
                List.of(NumNode.newArgLeaf(), NumNode.newArgLeaf())));
        
  }
  
  
  
  
  
  
  
  
  private final NumNode root;
  private final List<NumNode.Leaf> arguments; 
  
  /**
   * On construction, an instance <em>discovers</em> its arguments by inspecting
   * and collecting {@linkplain NumNode.Leaf#isArgument() settable} leaf nodes.
   */
  public BaseNumFunc(NumNode root) {
    this.root = Objects.requireNonNull(root, "null root");
    this.arguments = root.getArguments();
    if (arguments.isEmpty())
      throw new IllegalArgumentException("no arguments (settable nodes) found in evaluation tree");
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
  public Function<Number, Number> asFunction() throws UnsupportedOperationException {
    assertArgCount(1);
    return this::evalImpl1;
  }
  

  /**
   * Returns this instance as a 2-argument number function.
   * 
   * @throws UnsupportedOperationException if {@linkplain #getArgCount()}{@code != 2}
   */
  public BiFunction<Number, Number, Number> asBiFunction() throws UnsupportedOperationException {
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
  
  
  
  public Number eval(Number...numbers) {
    assertArgCount(numbers.length);
    for (int index = 0; index < numbers.length; ++index)
      arguments.get(index).setValue(numbers[index]);
    return root.value();
  }
  
  
  public Number eval(List<Number> numbers) {
    final int count = numbers.size();
    assertArgCount(count);
    for (int index = 0; index < count; ++index)
      arguments.get(index).setValue(numbers.get(index));
    return root.value();
  }
  
  
  
  
  @Override
  public final boolean equals(Object o) {
    return o instanceof BaseNumFunc func && root.equals(func.root);
  }
  
  
  @Override
  public final int hashCode() {
    return 0x10000 + root.hashCode();
  }
  
  
  
  
  
  private void assertArgCount(int actual) {
    int expected = arguments.size();
    if (actual != expected)
      throw new UnsupportedOperationException(
          "arg count mismatch: expected " + expected + "; given " + actual);
  }
  
  
  

}




























