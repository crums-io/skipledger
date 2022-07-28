/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.input;


import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;

import io.crums.sldg.reports.pdf.pred.PNode;
import io.crums.sldg.reports.pdf.pred.SourceRowPredicate;
import io.crums.sldg.src.SourceRow;
import io.crums.util.Lists;

/**
 * 
 */
public class Query {

  
  
  
  private final PNode<SourceRow, SourceRowPredicate> tree;
  
  
  public Query(SourceRowPredicate predicate) {
    this(PNode.leaf(predicate));
  }
  
  public Query(PNode<SourceRow, SourceRowPredicate> tree) {
    this.tree = Objects.requireNonNull(tree, "null predicate tree");
  }
  
  
  public List<NumberArg> getNumberArgs() {
    var out = new LinkedHashSet<NumberArg>();
    tree.leaves().forEach(leaf -> leaf.getPredicate().collectNumberArgs(out));
    var array = out.toArray(new NumberArg[out.size()]);
    return Lists.asReadOnlyList(array);
  }
  
  
  public PNode<SourceRow, SourceRowPredicate> tree() {
    return tree;
  }
  
}
