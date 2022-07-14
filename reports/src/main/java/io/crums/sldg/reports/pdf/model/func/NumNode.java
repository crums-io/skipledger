/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.model.func;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Arithmetic evaluation tree. The objective is to compose / model functions declaratively
 * and be able to serialize their structure in some format, say JSON.
 * 
 * <h3>Basic Design</h3>
 * <p>
 * A function's return value is represented by a tree's root node {@linkplain #value()}.
 * {@code NumNode}s come in 2 flavors, {@linkplain Leaf} and {@linkplain Branch}. {@code Leaf} instances
 * represent a "plain" number (thru their {@code value()} method), while {@code Branch}
 * types represent an arithmetic operation {@linkplain NumberOp} on 2 or more child nodes.
 * If the operation {@linkplain NumberOp#isAssociative() is associative} then the branch
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
 * @see NumberFunc
 */
public abstract class NumNode {
  
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
   * A leaf node has no operations. (Unary operations are composed using
   * binary operations.) It represents a direct value.
   */
  public static class Leaf extends NumNode {
    
    private final Number value;
    
    /**
     * Constructs an instance.
     * 
     * @param value not null. This is sometimes the special type {@code Primitive.Settable}
     * @see NumberFunc
     */
    public Leaf(Number value) {
      this.value = Objects.requireNonNull(value, "null value");
    }


    @Override
    public Number value() {
      return value;
    }


    @Override
    public final boolean isLeaf() {
      return true;
    }
    
  }
  
  
  /**
   * 
   */
  public static class Branch extends NumNode {
    
    private NumberOp op;
    private List<NumNode> children;
    
    /**
     * Creates a branch node with 2 or more children.
     * 
     * @param op        not null
     * @param children  2; more only if {@code op} is {@linkplain NumberOp#isAssociative() is associative}
     */
    public Branch(NumberOp op, List<NumNode> children) {
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
    public final NumberOp op() {
      return op;
    }
    
    
    /**
     * Return the branch's children. A branch has 2 children, at minimum;
     * if the {@linkplain #op() op} is <em>associative</em>, however, then it may
     * have more than 2.
     */
    public List<NumNode> children() {
      return Collections.unmodifiableList(children);
    }
    
  }

}