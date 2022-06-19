/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.reports.pdf.model.pred;

import java.util.Objects;
import java.util.Optional;

import io.crums.sldg.src.ColumnType;

/**
 * Base class for &ge; and &le; cell predicates.
 */
non-sealed class NotCellPredicate implements CellPredicate {
  
  private final CellPredicate cellPredicate;
  
  NotCellPredicate(CellPredicate cellPredicate) {
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
