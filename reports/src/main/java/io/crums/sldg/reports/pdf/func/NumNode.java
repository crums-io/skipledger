/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.func;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
//import java.util.function.Supplier;

import io.crums.util.PrimitiveComparator;

/**
 * Arithmetic evaluation tree. The objective is to compose / model functions declaratively
 * and be able to serialize their structure in some format, say JSON.
 * 
 * <h3>Basic Design</h3>
 * <p>
 * A function's return value is represented by a tree's root node {@linkplain #value()}.
 * {@code NumNode}s come in 2 flavors, {@linkplain Leaf} and {@linkplain Branch}. {@code Leaf} instances
 * represent a "plain" number (thru their {@code value()} method), while {@code Branch}
 * types represent an arithmetic operation {@linkplain NumOp} on 2 or more child nodes.
 * If the operation {@linkplain NumOp#isAssociative() is associative} then the branch
 * node can operate on (have) more than 2 children.
 * </p><p>
 * From a design perspecitve, these operations needn't be restricted to the basic four types
 * ({@code +, -, *, /}). For example, we could add exponentiation. For now, this minimal feature
 * set suffices to express most polynomial expressions not too verbosely.
 * </p>
 * <h3>Non Goals</h3>
 * <p>
 * (Listed here so I don't forget and avoid wasting time on these.)
 * <ul>
 * <li>Spec'ing a new DSL parser. (Rabbit hole.)</li>
 * <li>Efficiency. (It's aimed at <em>entry points</em> in the script / program.)</li>
 * </ul>
 * </p>
 * <h3>Fuzziness</h3>
 * <p>
 * Remember number representations on computers generally don't have mutlipicative inverses
 * (they're not proper fields). Nor are they truly associative (floats and doubles) even under
 * addition. The abstractions used here are no different.
 * </p>
 * 
 * @see BaseNumFunc
 */
public abstract class NumNode {
  
  
  
  /**
   * Returns a <em>mutable</em> value leaf instance.
   */
  public static NumNode newArgLeaf() {
    return new ArgLeaf();
  }
  
  // Backing out..
//  /**
//   * Returns a {@linkplain SuppliedLeaf} instance.
//   * 
//   * @param input non-null, with constant equality semantics and hash code
//   */
//  public static NumNode suppliedLeaf(Supplier<Number> input) {
//    return new SuppliedLeaf(input);
//  }
  
  /**
   * Returns a <em>fixed</em> value leaf instance.
   * 
   * @param value not null
   */
  public static NumNode newLeaf(Number value) {
    return new FixedLeaf(value);
  }
  
  /**
   * Returns a branch instance.
   * 
   * @param op        not null
   * @param children  at least 2; <em>exactly</em> 2, if {@code op} is not
   *                  {@linkplain NumOp#isAssociative() associative}
   * @return a branch node
   */
  public static NumNode newBranch(NumOp op, List<NumNode> children) {
    return new Branch(op, children);
  }
  
  /**
   * Returns the value. Often, this is calculated on demand. If it's a branch node,
   * then the values of the child nodes are calculated from left to right (first to last).
   * This shouldn't matter in principle if (like you should) the values are calculated
   * without any side-effects.
   * 
   * @return not null
   */
  public abstract Number value();
  
  
  /** @see #asLeaf() */
  public abstract boolean isLeaf();
  

  /** @see #asBranch() */
  public final boolean isBranch() {
    return !isLeaf();
  }
  
  
  /** Returns this instance as a leaf. */
  public Leaf asLeaf() throws ClassCastException {
    return (Leaf) this;
  }
  

  /** Returns this instance as a branch. */
  public Branch asBranch() throws ClassCastException {
    return (Branch) this;
  }
  
  

  /**
   * Collects and returns the <em>settable</em> leaf nodes in a pre-order traversal
   * of tree.
   */
  public List<Leaf> getArguments() {
    if (isLeaf()) {
      var leaf = asLeaf();
      return leaf.isArgument() ? List.of(leaf) : List.of();
    }
    
    var settables = collectArgs(new ArrayList<>(4));
    
    return settables.isEmpty() ?
        List.of() : Collections.unmodifiableList(settables);
  }
  
  
  private List<Leaf> collectArgs(List<NumNode.Leaf> collected) {
    if (isLeaf()) {
      var leaf = asLeaf();
      if (leaf.isArgument())
        collected.add(leaf);
    } else {
      for (var sub : asBranch().children())
        sub.collectArgs(collected);
    }
    return collected;
  }
  
  
  
  
  /**
   * A leaf node has no operations. (Unary operations are composed using
   * binary operations.) It represents a direct value.
   */
  public static abstract class Leaf extends NumNode {
    
    


    @Override
    public final boolean isLeaf() {
      return true;
    }
    
    
    public abstract boolean isArgument();
    
//    public Supplier<Number> supplier() {
//      return null;
//    }
    
    
//    public final void setValue(Long value) {
//      setValueImpl(value);
//    }
//    
//    public final void setValue(Double value) {
//      setValueImpl(value);
//    }
//    
//    public final void setValue(Integer value) {
//      setValueImpl(value);
//    }
//    
//    public final void setValue(Float value) {
//      setValueImpl(value);
//    }
    
    public final void setValue(Number value) {
      boolean ok =
          value instanceof Long ||
          value instanceof Double ||
          value instanceof Integer ||
          value instanceof Float;
      if (!ok)
        throw new IllegalArgumentException(
            "not one of the top 4 boxed primitives: " + value.getClass());
      setValueImpl(value);
    }
    
    protected void setValueImpl(Number value) throws UnsupportedOperationException {
      throw new UnsupportedOperationException();
    }
    
  }
  
  
  
  /** An immutable leaf value. */
  public static class FixedLeaf extends Leaf {
    
    private final Number value;
    
    /**
     * Constructs an instance.
     * 
     * @param value not null. This is sometimes the special type {@code Primitive.Settable}
     * @see BaseNumFunc
     */
    public FixedLeaf(Number value) {
      this.value = Objects.requireNonNull(value, "null value");
    }



    @Override
    public final Number value() {
      return value;
    }
    
    
    public final boolean isArgument() {
      return false;
    }
    

    
    @Override
    public final boolean equals(Object o) {
      return o instanceof FixedLeaf fixed && PrimitiveComparator.equal(value, fixed.value);
    }
    
    
        @Override
    public final int hashCode() {
      return value.intValue();
    }
    
  }
  
  /** Mutable leaf node. I.e. an argument.
   */
  public static class ArgLeaf extends Leaf {
    
    private Number value = 0;
    

    @Override
    public boolean isArgument() {
      return true;
    }
    
    
    protected void setValueImpl(Number value) {
      this.value = Objects.requireNonNull(value, "null value");
    }

    @Override
    public Number value() {
      return value;
    }
    
    
    
    
    
    @Override
    public final boolean equals(Object o) {
      return o instanceof ArgLeaf;
    }
    

    @Override
    public final int hashCode() {
      return Integer.MIN_VALUE;
    }
    
  }
  
  
  // Backing out.. not worth the complication
  
//  public static class SuppliedLeaf extends Leaf {
//    
//    private final static int CH = SuppliedLeaf.class.hashCode();
//
//    private Supplier<Number> supplier;
//    
//    
//    /**
//     * @param supplier non-null, with constant equality semantics and hash code
//     */
//    public SuppliedLeaf(Supplier<Number> supplier) {
//      this.supplier = Objects.requireNonNull(supplier, "null supplier");
//    }
//
//    @Override
//    public boolean isSupplied() {
//      return true;
//    }
//    
//    public Number value() {
//      return supplier.get();
//    }
//    
//
//    @Override
//    public Supplier<Number> supplier() {
//      return supplier;
//    }
//    
//    
//    @Override
//    public final boolean equals(Object o) {
//      return o instanceof SuppliedLeaf;
//    }
//    
//
//    @Override
//    public final int hashCode() {
//      return CH ^ supplier.hashCode();
//    }
//  }
  
  
  
  
  
  /**
   * A binary operation on multiple sub-branches (children). If
   * the operation is associative, it can be chained with more than 2 sub-branches.
   */
  public static class Branch extends NumNode {
    
    private final NumOp op;
    private final List<NumNode> children;
    
    /**
     * Creates a branch node with 2 or more children.
     * 
     * @param op        not null
     * @param children  2; more only if {@code op} is {@linkplain NumOp#isAssociative() is associative}
     */
    public Branch(NumOp op, List<NumNode> children) {
      this.op = Objects.requireNonNull(op, "null op");
      this.children = Objects.requireNonNull(children, "null children");
      int size = children.size();
      if (size < 2)
        throw new IllegalArgumentException("too few children: " + children);
      if (children.size() > 2 && !op.isAssociative())
        throw new IllegalArgumentException(
            "too many children (" + size + ") for non-associative op " +
            op + ": " + children);
    }

    
    /**
     * Computes and returns the value of this branch by computing the
     * value of each of its children, left to right, applying the binary
     * operation {@linkplain #op()} at each step.
     */
    @Override
    public Number value() {
      if (children.size() == 2)
        return op.apply(children.get(0).value(), children.get(1).value());
      var iter = children.iterator();
      Number out = iter.next().value();
      while (iter.hasNext())
        out = op.apply(out, iter.next().value());
      return out;
    }

    @Override
    public final boolean isLeaf() {
      return false;
    }
    
    
    /** Returns the branch op. (Only branches have ops).   */
    public final NumOp op() {
      return op;
    }
    
    
    /**
     * Return the branch's children. A branch has 2 children, at minimum;
     * if the {@linkplain #op() op} is <em>associative</em>, however, then it may
     * have more than 2.
     */
    public final List<NumNode> children() {
      return Collections.unmodifiableList(children);
    }
    

    
    @Override
    public final boolean equals(Object o) {
      return o instanceof Branch branch && op == branch.op && children.equals(branch.children);
    }
    

    @Override
    public final int hashCode() {
      return op.hashCode() ^ children.hashCode();
    }
    
  }

}
