/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.model;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;

import io.crums.sldg.src.SourceRow;
import io.crums.util.Lists;

/**
 * The input rows. Introducing a layer of indirection
 * (which I might remove later).
 */
public interface InputRows {
  
  public static InputRows of(List<SourceRow> rows) {
    if (Objects.requireNonNull(rows, "null rows").isEmpty())
      throw new IllegalArgumentException("empty rows");
    return new InputRows() {
      @Override  public List<SourceRow> rows() { return rows; }
    };
  }
  
  /**
   * 
   * @param sources
   * @param constraint
   * @return
   */
  public static Optional<InputRows> compose(List<SourceRow> sources, Predicate<SourceRow> constraint) {
    if (Objects.requireNonNull(sources, "null sources").isEmpty())
      throw new IllegalArgumentException("empty sources");
    List<SourceRow> rows = sources.stream().filter(constraint).toList();
    return rows.isEmpty() ? Optional.empty() : Optional.of(of(rows));
  }
  
  
  
//  public enum NumberOp {
//    SUM,
//  }
  
  
  
  /**
   * Returns the input rows in ascending order of row numbers.
   * 
   * @return not-empty list of unique, {@code SourceRows}s sorted by row number
   */
  List<SourceRow> rows();
  
  
  
//  default Number compute(NumberOp op, int colIndex) {
//    Objects.requireNonNull(op, "null op");
//    if (colIndex < 0)
//      throw new IndexOutOfBoundsException("colIndex: " + colIndex);
//    
//    return 0;
//  }
  
  
  
  default List<Long> rowNumbers() {
    return Lists.map(rows(), r -> r.rowNumber());
  }
  
  
  
  

  /**
   * Returns the number of input rows.
   * 
   * @return {@code rows().size()}
   */
  default int size() {
    return rows().size();
  }

}
