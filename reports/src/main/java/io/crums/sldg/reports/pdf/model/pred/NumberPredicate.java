/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.model.pred;


import java.util.Objects;
import java.util.Optional;

import io.crums.sldg.src.ColumnType;
import io.crums.util.PrimitiveComparator;

/**
 * Base class {@linkplain Number}-based {@linkplain CellPredicate}s.
 */
public non-sealed class NumberPredicate implements CellPredicate {
  
  protected final Number rhs;
  protected final int expectedSign;
  
  /**
   * 
   * @param rhs           not null (checked)
   * @param expectedSign  -1, 0, or 1 (not checked, since subclasses don't expose this)
   */
  NumberPredicate(Number rhs, int expectedSign) {
    this.rhs = Objects.requireNonNull(rhs, "null RHS argument");
    this.expectedSign = expectedSign;
  }

  /**
   * @return {@code type.isNumber()}
   */
  @Override
  public boolean acceptType(ColumnType type) {
    return type.isNumber();
  }

  /**
   * {@inheritDoc}
   * {@code null} is properly handled.
   */
  @Override
  public boolean acceptValue(Object value) {
    return
        value instanceof Number num &&
        expectedSign == Integer.signum(
            PrimitiveComparator.INSTANCE.compare(num, rhs));
  }
  
  
  /** @return always present */
  @Override
  public final Optional<Number> rhs() {
    return Optional.of(rhs);
  }
}
