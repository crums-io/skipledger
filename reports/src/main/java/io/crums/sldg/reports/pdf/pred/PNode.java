/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.pred;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

import io.crums.util.Lists;

/**
 * A predicate tree composed of child sub predicates, either {@linkplain Op#AND AND}-ed
 * or {@linkplain Op#OR OR}-ed together. As usual, there are 2 types of instances:
 * {@linkplain PNode.Branch Branch} and {@linkplain PNode.Leaf Leaf} which inherit
 * from the abstract base type.
 * <p>
 * We have 2 use cases for this:
 * <ol>
 * <li>{@code PNode<SourceRow, ColumnPredicate>}: a predicate tree on 1 or more columns, and</li>
 * <li>{@code PNode<ColumnValue, CellPredicate>}: a predicate tree on an individual column value. </li>
 * </ol>
 * The 2 are related: each instance of the latter tree lives in a leaf instance
 * of the former.
 * </p>
 * <h3>Motivation</h3>
 * <p>
 * This predicate tree is designed to be serializable to JSON. To do that, it needs to
 * expose information about its internal tree structure. If we were only concerned with
 * reading predicate trees from JSON (not writing), the design could be simplified (we
 * could do away with the {@code <P>} type paramater, for eg).
 * </p>
 * 
 * @param <T>   the type the predicate tests
 * @param <P>   predicate implementation class or type (for leaf instances)
 */
public abstract class PNode<T, P extends Predicate<T>> implements Predicate<T> {
  
  // Base instance methods are at the bottom of file
  
  /** Creates and return a leaf node using the given predicate. */
  public static <T, U extends Predicate<T>> PNode<T, U> leaf(U predicate) {
    return new Leaf<>(predicate);
  }
  
  /**
   * Creates and returns a branch node using the given list of predicates and
   * operation. As a special case, when the given list is a singleton, then a
   * <em>leaf node</em> is returned.
   */
  public static <T, P extends Predicate<T>> PNode<T, P> branchLeaves(List<P> predicates, Op op) {
    if (Objects.requireNonNull(predicates, "null predicates").isEmpty())
      throw new IllegalArgumentException("empty predicates list");
    List<PNode<T,P>> leaves = predicates.stream().map(PNode::leaf).toList();
    if (leaves.size() == 1) {
      // enforce arg rule, even tho we don't need it here..
      Objects.requireNonNull(op, "null op");
      return leaves.get(0);
    }
    return new PNode.Branch<>(leaves, op);
  }
  
  
  /**
   * Creates and returns a branch node.
   * 
   * @param predicates  a list of size &ge; 2
   * @param op          the boolean operation
   */
  public static <T, P extends Predicate<T>> PNode<T, P> branch(List<PNode<T, P>> predicates, Op op) {
    return new Branch<>(predicates, op);
  }
  
  
  /**
   * 
   * @param <T>
   * @param <P>
   * @param predicates
   * @return
   */
  public static <T, P extends Predicate<T>> PNode<T, P> or(List<PNode<T, P>> predicates) {
    return branch(predicates, Op.OR);
  }
  
  
  public static <T, P extends Predicate<T>> PNode<T, P> and(List<PNode<T, P>> predicates) {
    return branch(predicates, Op.AND);
  }
  
  
  
  
  
  /**
   * Logical operation on 2 or more sub predicates.
   * 
   * @see #test(List, Op, Object)
   */
  public enum Op {
    AND,
    OR;
    
    public boolean isAnd() {
      return this == AND;
    }
    
    public boolean isOr() {
      return this == OR;
    }
  }

  /**
   * Tests the given given value {@code o} against a list of predicates
   * ({@code branches}) using the given operation ({@code op}). Evaluation of
   * predicates is short-circuited in the usual way: on first failure for
   * {@code AND}; on first success for {@code OR}.
   */
  public static <T> boolean test(List<Predicate<T>> branches, Op op, T o) {
    final int size = branches.size();
    int index = 0;
    boolean pass;
    if (op.isAnd()) {
      for (; index < size && branches.get(index).test(o); ++index);
      pass = index == size;
    } else { // or
      for (; index < size && !branches.get(index).test(o); ++index);
      pass = index != size;
    }
    return pass;
  }
  
  
  /** Branch instances have at least 2 children. */
  public static class Branch<T, P extends Predicate<T>> extends PNode<T, P> {
    
    private final List<PNode<T, P>> branches;
    private final Op op;
    
    /**
     * @param branches   of size &ge; 2
     * @param op         not null
     */
    public Branch(List<PNode<T, P>> branches, Op op) {
      this.branches = Objects.requireNonNull(branches, "null branches");
      this.op = Objects.requireNonNull(op, "null op");
      if (branches.size() < 2)
        throw new IllegalArgumentException("too few branches: " + branches);
    }

    /**
     * Tests the given value. Evaluation of predicates short-circuits
     * in the usual way one expects (AND, on first failure; OR on first success).
     */
    @Override
    public boolean test(T t) {
      return test(Lists.downcast(branches), op, t);
    }

    /** @return {@code false} */
    @Override
    public final boolean isLeaf() {
      return false;
    }
    
    @Override
    public final Branch<T, P> asBranch() {
      return this;
    }
    
    
    /** Returns the child predicate nodes. */
    public List<PNode<T, P>> getChildren() {
      return Collections.unmodifiableList(branches);
    }
    
    /** Returns the operation performed on the child nodes. */
    public final Op op() {
      return op;
    }
    
  }
  
  /** A leaf node in the predicate tree. */
  public static class Leaf<T, P extends Predicate<T>> extends PNode<T, P> {
    
    private final P predicate;
    
    
    public Leaf(P predicate) {
      this.predicate = Objects.requireNonNull(predicate, "null predicate");
    }

    @Override
    public boolean test(T t) {
      return predicate.test(t);
    }

    /** @return {@code true} */
    @Override
    public final boolean isLeaf() {
      return true;
    }
    
    @Override
    public final Leaf<T, P> asLeaf() {
      return this;
    }
    
    /** Returns the leaf predicate. */
    public P getPredicate() {
      return predicate;
    }
    
  }
  
  
  
  
  
  
  private PNode() { }
  
  
  /**
   * Returns {@code true} if this node is a combination of child predicates.
   * 
   * @return {@code !isLeaf()}
   */
  public final boolean isBranch() {
    return !isLeaf();
  }
  
  
  /**
   * Returns this instance as a branch instance.
   * 
   * @throws ClassCastException if this is not a branch
   * @see #isBranch()
   */
  public PNode.Branch<T, P> asBranch() throws ClassCastException {
    return (PNode.Branch<T, P>) this;
  }
  
  
  /**
   * Returns {@code true} if this node is <em>not</em> a combination of other predicates.
   */
  public abstract boolean isLeaf();
  

  /**
   * Returns this instance as a leaf instance.
   * 
   * @throws ClassCastException if this is not a branch
   * @see #isLeaf()
   */
  public PNode.Leaf<T, P> asLeaf() throws ClassCastException {
    return (PNode.Leaf<T, P>) this;
  }
  
  
  
  public List<PNode.Leaf<T, P>> leaves() {
    if (isLeaf())
      return List.of(asLeaf());
    var out = new ArrayList<PNode.Leaf<T, P>>(8);
    collectLeaves(out);
    return Collections.unmodifiableList(out);
  }
  
  
  
  private void collectLeaves(List<PNode.Leaf<T, P>> list) {
    if (isLeaf())
      list.add(asLeaf());
    else
      asBranch().getChildren().forEach(n -> n.collectLeaves(list));
  }
  
  

}






















