/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.input;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

import io.crums.sldg.reports.pdf.pred.PNode;
import io.crums.sldg.reports.pdf.pred.SourceRowPredicate;
import io.crums.sldg.src.SourceRow;
import io.crums.util.Lists;

/**
 * 
 */
public class Query implements Predicate<SourceRow> {

  
  
  
  private final PNode<SourceRow, SourceRowPredicate> pTree;
  
  private final List<NumberArg> numberArgs;
  
  
  public Query(SourceRowPredicate predicate) {
    this(PNode.leaf(predicate));
  }
  
  public Query(PNode<SourceRow, SourceRowPredicate> tree) {
    this.pTree = Objects.requireNonNull(tree, "null predicate tree");
    this.numberArgs = numArgs();
  }
  
  @SuppressWarnings("serial")
  private List<NumberArg> numArgs() {
    List<NumberArg> out;
    {
      var argset = new HashMap<String, NumberArg>();
      out = new ArrayList<NumberArg>() {
        @Override public boolean add(NumberArg arg) {
          var prev = argset.putIfAbsent(arg.param().name(), arg);
          if (prev == null) 
            return super.add(arg);
          if (prev == arg)
            return false;
          throw new IllegalArgumentException("duplicate number arg instances: " + arg);
        }
      };
    }
    
    pTree.leaves().forEach(leaf -> leaf.getPredicate().collectNumberArgs(out));
    return Lists.readOnlyCopy(out);
  }
  
  
  public List<NumberArg> getNumberArgs() {
    return numberArgs;
  }
  
  
  public PNode<SourceRow, SourceRowPredicate> predicateTree() {
    return pTree;
  }

  /** Tests the given {@code row} againt the {@linkplain #predicateTree() predicate tree}. */
  @Override
  public boolean test(SourceRow row) {
    return pTree.test(row);
  }
  
  
  public List<SourceRow> selectFrom(List<SourceRow> candidates) {
    return candidates.stream().filter(this).toList();
  }
  
  
  private final static int CH = Query.class.hashCode();
  
  @Override
  public final int hashCode() {
    return CH ^ pTree.hashCode();
  }
  

  @Override
  public final boolean equals(Object o) {
    return
        o instanceof Query q &&
        q.pTree.equals(pTree);
  }
  
}

























