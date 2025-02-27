/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.pred;


import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;

import io.crums.sldg.reports.pdf.input.NumberArg;
import io.crums.sldg.reports.pdf.func.SourceRowNumFunc.Supplied;
import io.crums.sldg.src.SourceRow;

/**
 * 
 */
public class SourceRowPredicate implements Predicate<SourceRow>, NumberArg.Collector {
  
  
  private record Operand(Number num, Supplied func) {
    
    Operand {
      if (num == null && func == null)
        throw new IllegalArgumentException("missing number or function");
      if (num != null && func != null)
        throw new RuntimeException("Report this bug.");
    }
    
    Operand(Number num)     { this(num, null); }
    
    Operand(Supplied func)  { this(null, func); }
    
    
    
    boolean test(SourceRow row) {
      return func == null || func.test(row);
    }
    
    
    Number eval(SourceRow row) {
      return func == null ? num : func.eval(row);
    }
    
    void collectArgs(Collection<NumberArg> collector) {
      if (func != null)
        func.collectNumberArgs(collector);
      else if (num instanceof NumberArg arg)
        collector.add(arg);
    }
    
    Optional<Number> getNum() {
      return Optional.ofNullable(num);
    }
    
    Optional<Supplied> getFunc() {
      return Optional.ofNullable(func);
    }
    

//    final static String CN = Operand.class.getSimpleName();
    @Override
    public String toString() {
      return Operand.class.getSimpleName() + (num == null ? ("[func=" + func) : ("[num=" + num)) + "]";
    }
  }
  
  
  
  
  
  private Operand left;
  private BoolComp op;
  private Operand right;
  
  
  public SourceRowPredicate(Number left, BoolComp op, Supplied right) {
    this(new Operand(left), op, new Operand(right));
  }
  
  public SourceRowPredicate(Supplied left, BoolComp op, Supplied right) {
    this(new Operand(left), op, new Operand(right));
  }
  
  public SourceRowPredicate(Supplied left, BoolComp op, Number right) {
    this(new Operand(left), op, new Operand(right));
  }
  
  private SourceRowPredicate(Operand left, BoolComp op, Operand right) {
    this.left = left;
    this.op = Objects.requireNonNull(op, "null op");
    this.right = right;
  }
  

  @Override
  public boolean test(SourceRow row) {
    return
        left.test(row) &&
        right.test(row) &&
        op.test( left.eval(row), right.eval(row) );
  }

  @Override
  public void collectNumberArgs(Collection<NumberArg> collector) {
    left.collectArgs(collector);
    right.collectArgs(collector);
  }
  
  
  public Optional<Number> leftNum() {
    return left.getNum();
  }
  
  public Optional<Supplied> leftFunc() {
    return left.getFunc();
  }
  
  
  public BoolComp op() {
    return op;
  }
  
  
  public Optional<Number> rightNum() {
    return right.getNum();
  }
  
  public Optional<Supplied> rightFunc() {
    return right.getFunc();
  }
  
  
  
  @Override
  public final int hashCode() {
    return (left.hashCode() * 31 + op.hashCode()) * 31 + right.hashCode();
  }

  @Override
  public final boolean equals(Object o) {
    return
        o instanceof SourceRowPredicate s &&
        s.op == op &&
        s.left.equals(left) &&
        s.right.equals(right);
  }
  
  
  final static String CN = SourceRowPredicate.class.getSimpleName();

  @Override
  public String toString() {
    return CN + "[" + left + " " + op.symbol() + " " + right + "]";
  }

}

















