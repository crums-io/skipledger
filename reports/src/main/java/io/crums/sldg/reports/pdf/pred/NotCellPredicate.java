/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.pred;

import java.util.Objects;
import java.util.Optional;

import io.crums.sldg.src.ColumnType;

/**
 * Base class for &ge; and &le; cell predicates.
 */
non-sealed class NotCellPredicate implements ColumnValuePredicate {
  
  private final ColumnValuePredicate cellPredicate;
  
  NotCellPredicate(ColumnValuePredicate cellPredicate) {
    this.cellPredicate = Objects.requireNonNull(cellPredicate, "null cell predicate");
  }

  @Override
  public boolean acceptType(ColumnType type) {
    return cellPredicate.acceptType(type);
  }

  @Override
  public boolean acceptValue(Object value) {
    return !cellPredicate.acceptValue(value);
  }
  
  /** @return the RHS of the predicate passed in at construction time, if any */
  public Optional<?> rhs() {
    return cellPredicate.rhs();
  }

}
