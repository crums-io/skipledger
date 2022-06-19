/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.reports.pdf.model;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;

import io.crums.sldg.src.SourceRow;

/**
 * The input row numbers. Introducing a layer of indirection
 * (which I might remove later).
 */
public interface InputRows {
  
  public static InputRows of(List<Long> rows) {
    if (Objects.requireNonNull(rows, "null rows").isEmpty())
      throw new IllegalArgumentException("empty rows");
    return new InputRows() {
      @Override  public List<Long> rowNumbers() { return rows; }
    };
  }
  
  
  public static Optional<InputRows> compose(List<SourceRow> sources, Predicate<SourceRow> constraint) {
    if (Objects.requireNonNull(sources, "null sources").isEmpty())
      throw new IllegalArgumentException("empty sources");
    var rowNums = sources.stream().filter(constraint).map(r -> r.rowNumber()).toList();
    return rowNums.isEmpty() ? Optional.empty() : Optional.of(of(rowNums));
  }
  
  
  
  /**
   * Returns the input row numbers. Implementations currently return this
   * in ascending order.
   * 
   * @return not-empty list
   */
  List<Long> rowNumbers();
  

  /**
   * @return {@code rowNumbers().size()}
   */
  default int size() {
    return rowNumbers().size();
  }

}
