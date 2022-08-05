/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.func;


import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

import io.crums.sldg.reports.pdf.input.NumberArg;
import io.crums.sldg.src.SourceRow;
import io.crums.util.Lists;

/**
 * A {@linkplain SourceRow} to {@linkplain NumFunc} mapping.
 * 
 * @see Column
 */
public class SourceRowNumFunc implements Function<SourceRow, NumFunc>, Predicate<SourceRow>, NumberArg.Collector {
  
  
  

  /**
   * Encodes {@linkplain SourceRow} to {@linkplain NumFunc} argument mappings.
   *                  
   * @see #isRowNumber()
   */
  public record Column(int funcIndex, int srcIndex) implements Comparable<Column> {
    
    public final static int RN_INDEX = -1;
    
    /** Row number, no function instance. */
    public final static Column ROW_NUM = new Column(RN_INDEX);
    
    /**
     * @param funcIndex the {@linkplain NumFunc#eval(List) NumFunc}'s argument index
     * @param srcIndex  the source row's column index from which the number value comes from; if
     *                  equal to {@linkplain #RN_INDEX}, then the row number
     */
    public Column {
      if (funcIndex < 0)
        throw new IllegalArgumentException("negative funcIndex: " + funcIndex);
      if (srcIndex < RN_INDEX)
        throw new IllegalArgumentException("negative srcIndex: " + srcIndex);
    }
    
    /**
     * For when there's no function. {@code funcIndex} set to abitrarily to 0 (never read).
     * 
     * @param srcIndex  the source row's column index from which the number value comes from; if
     *                  equal to {@linkplain #RN_INDEX}, then the row number
     */
    public Column(int srcIndex) {
      this(0, srcIndex);
    }

    /** Ordered by {@linkplain #funcIndex()}. */
    @Override
    public int compareTo(Column other) {
      return funcIndex - other.funcIndex;
    }
    
    /** @return {@code srcIndex == RN_INDEX} */
    public boolean isRowNumber() {
      return srcIndex == RN_INDEX;
    }
    
  }
  
  
  
  
  
  
  
  
  
  
  private final List<Column> columns;
  
  private final int minColSize;
  
  private final NumFunc func;
  

  
  /**
   * Ceates an instance with the given columns specifying which numeric source column indexes
   * are bound to the given {@code func}tion argument.
   * 
   * @param columns  not empty list of column indexes (non-negative)
   * @param func     with {@code func.}{@linkplain NumFunc#getArgCount() getArgCount()}{@code  >= columns.size()}.
   *                 May only be null if {@code columns} is a singleton (in which case {@code null}
   *                 is interpreted as the identity function).
   */
  public SourceRowNumFunc(List<Column> columns, NumFunc func) {
    
    columns = List.copyOf( Objects.requireNonNull(columns) );
    if (columns.isEmpty())
      throw new IllegalArgumentException("empty colIndexes");
    if (!Lists.isSortedNoDups(columns))
      throw new IllegalArgumentException(
          "columns not sorted or contains duplicate funcIndex: " + columns);
    this.columns = columns;
    minColSize = columns.stream().map(Column::srcIndex).min(Comparator.naturalOrder()).get() + 1;
    
    this.func = func;
    if (func == null) {
      if (columns.size() != 1)
        throw new IllegalArgumentException(
            "null func with " + columns.size() + " columns");
    } else {
      int maxFuncIndex =
          columns.stream().map(Column::funcIndex).max(Comparator.naturalOrder()).get();
      if (maxFuncIndex >= func.getArgCount())
        throw new IllegalArgumentException(
            "max func index (" + maxFuncIndex + ") >= func arg count (" + func.getArgCount() + ")");
    }
  }
  
  
  public SourceRowNumFunc(SourceRowNumFunc copy) {
    this.columns = copy.columns;
    this.minColSize = copy.minColSize;
    this.func = copy.func;
  }
  
  
  
  public boolean isSupplied() {
    return columns.size() == (func == null ? 1 : func.getArgCount());
  }
  

  @Override
  public void collectNumberArgs(Collection<NumberArg> collector) {
    if (func != null)
      func.collectNumberArgs(collector);
  }
  
  

  

  
  @Override
  public boolean test(SourceRow row) {
    var rowCols = row.getColumns();
    if (rowCols.size() < minColSize)
      return false;
    for (var column : columns) {
      if (column.isRowNumber())
        continue;
      if (!rowCols.get(column.srcIndex).getType().isNumber())
        return false;
    }
    return true;
  }

  /**
   * Returns a {@code NumFunc} after by applying the given {@code row} to its
   * closure.
   * Throws {@code ClassCastException} or {@code IndexOutOfBoundsException}
   * if the predicate {@linkplain #test(SourceRow)} returns {@code false}.
   */
  @Override
  public NumFunc apply(SourceRow row) throws ClassCastException, IndexOutOfBoundsException {
    if (func == null)
      return NumFunc.of(columnValue(columns.get(0), row));
    
    var boundValues = Lists.map(columns, col -> bindValue(col, row));
    return new NumFuncClosure(func, boundValues);
  }
  
  
  private NumFuncClosure.Value bindValue(Column col, SourceRow row) {
    return new NumFuncClosure.Value(
        col.funcIndex,
        columnValue(col, row));
  }
  
  
  private Number columnValue(Column col, SourceRow row) {
    return
        col.isRowNumber() ?
            row.rowNumber() :
              (Number) row.getColumns().get(col.srcIndex).getValue();
  }
  
  
  public final int getArgCount() {
    return func == null ? 0 : func.getArgCount() - columns.size();
  }
  
  
  
  public final List<Column> getColumns() {
    return columns;
  }
  
  
  
  public final Optional<NumFunc> getFunc() {
    return Optional.ofNullable(func);
  }
  
  
  private final static int CH = SourceRowNumFunc.class.hashCode();
  
  @Override
  public final int hashCode() {
    int hash = columns.hashCode();
    if (func != null) {
      hash *= 31;
      hash += func.hashCode();
    }
    return CH ^ hash;
  }
  
  
  @Override
  public final boolean equals(Object o) {
    return
        o instanceof SourceRowNumFunc f &&
        f.columns.equals(columns) &&
        Objects.equals(f.func, func);
  }
  
 
  
  public static class Supplied extends SourceRowNumFunc {
    
    public Supplied(List<Column> columns, NumFunc func) {
      super(columns, func);
      if (!isSupplied())
        throw new IllegalArgumentException(
            "func " + func + " has remaining args (" + getArgCount() + ")");
    }
    
    public Supplied(SourceRowNumFunc promo) {
      super(promo);
      if (!isSupplied())
        throw new IllegalArgumentException(
            "attempt to create instance with remaining args (" + getArgCount() + ")");
      
    }
    
    
    public Number eval(SourceRow row) {
      return apply(row).eval();
    }
    
  }

}
