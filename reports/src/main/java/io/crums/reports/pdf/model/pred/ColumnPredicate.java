/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.reports.pdf.model.pred;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

import io.crums.reports.pdf.model.NumberArg;
import io.crums.sldg.src.ColumnValue;
import io.crums.sldg.src.SourceRow;

/**
 * A source row predicate based on column value.
 * <p>
 * Each instance of this class selects a column from a {@linkplain SourceRow}
 * (defined by {@linkplain #columnNumber() column number}) and tests the
 * column value against a predicate tree for that value.
 * </p>
 */
public class ColumnPredicate implements Predicate<SourceRow> {
  
  private final int colIndex;
  
  private final PNode<ColumnValue, CellPredicate> cellPredicate;
  
  /**
   * 
   * @param colNumber &ge; 1 (first column is 1)
   * @param predicate column value predicate tree
   */
  public ColumnPredicate(int colNumber, PNode<ColumnValue, CellPredicate> predicate) {
    this.colIndex = colNumber - 1;
    this.cellPredicate = Objects.requireNonNull(predicate, "null cell predicate");
    if (colIndex < 0)
      throw new IllegalArgumentException(
          "colNumber (" + colNumber + ") < 1");
  }
  
  /**
   * Returns the column number tested.
   * 
   *  @return &ge; 1
   */
  public final int columnNumber() {
    return colIndex + 1;
  }

  @Override
  public boolean test(SourceRow row) {
    var cols = row.getColumns();
    return cols.size() > colIndex && cellPredicate.test(cols.get(colIndex));
  }
  
  
  public PNode<ColumnValue, CellPredicate> getCellPredicate() {
    return cellPredicate;
  }
  
  
  public List<NumberArg> getNumberArgs() {
    var out = collectNumberArgs(cellPredicate, new ArrayList<>(8));
    return out.isEmpty() ? List.of() : Collections.unmodifiableList(out);
  }
  
  
  private List<NumberArg> collectNumberArgs(PNode<ColumnValue, CellPredicate> pNode, List<NumberArg> bag) {
    if (pNode.isLeaf())
      pNode.asLeaf().getPredicate().rhs().ifPresent(
          rhs -> { if (rhs instanceof NumberArg arg) bag.add(arg); });
    else
      pNode.asBranch().getChildren().forEach(n -> collectNumberArgs(n, bag));
    return bag;
  }

}
