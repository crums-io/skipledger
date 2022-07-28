/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.pred.dep;

import java.util.Objects;
import java.util.Optional;

import io.crums.sldg.src.ColumnType;

/**
 * Base class for &ge; and &le; cell predicates.
 */
non-sealed class NotColumnValuePredicate implements ColumnValuePredicate {
  
  private final ColumnValuePredicate colPredicate;
  
  NotColumnValuePredicate(ColumnValuePredicate colPredicate) {
    this.colPredicate = Objects.requireNonNull(colPredicate, "null column predicate");
  }

  @Override
  public boolean acceptType(ColumnType type) {
    return colPredicate.acceptType(type);
  }

  @Override
  public boolean acceptValue(Object value) {
    return !colPredicate.acceptValue(value);
  }
  
  /** @return the RHS of the predicate passed in at construction time, if any */
  public Optional<?> rhs() {
    return colPredicate.rhs();
  }

}
