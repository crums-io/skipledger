/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

import com.lowagie.text.Rectangle;
import com.lowagie.text.pdf.PdfPTable;

import io.crums.sldg.reports.pdf.model.CellDataProvider;
import io.crums.sldg.reports.pdf.model.CellDataProvider.DateProvider;
import io.crums.sldg.reports.pdf.model.CellDataProvider.ImageProvider;
import io.crums.sldg.reports.pdf.model.CellDataProvider.NumberProvider;
import io.crums.sldg.reports.pdf.model.CellDataProvider.StringProvider;
import io.crums.sldg.reports.pdf.model.func.NumberFunc;
import io.crums.sldg.src.ColumnType;
import io.crums.sldg.src.SourceRow;

/**
 * A cell whose data is sourced to one or more {@linkplain SourceRow}s.
 * Instances must first be initialized thru either <ol>
 * <li>{@linkplain #init(SourceRow)}</li>
 * or if <em>{@linkplain #isCompound() compound}</em>,
 * <li>{@linkplain #init(List)}</li>
 * <ol>
 * <p>
 * Uninitialized instances are equivalent to blank cells. Instances can
 * be intialized (and reused) multiple times, so in this sense they double as
 * factories.
 * </p>
 */
public abstract class SourcedCell extends CellData {
  
  
  final CellDataProvider<?> provider;
  
  CellData generatedCell;
  
  
  
  
  SourcedCell(CellDataProvider<?> provider, CellFormat format) {
    super(format);
    this.provider = Objects.requireNonNull(provider, "null cell data provider");
  }
  

  /** Initializes the instance with the given {@code sourceRow} and returns the generated
   * cell.
   * @see #isCompound()
   * @see #isInitialized()
   * @see #generatedCell */
  public abstract CellData init(SourceRow sourceRow) throws UnsupportedOperationException;

  
  
  /** @return {@code generatedCell != null} */
  public boolean isInitialized() {
    return generatedCell != null;
  }
  
  /**
   * Determines the instance's initialization method. If this method returns
   * {@code true}, then the instance is initialized via {@linkplain #init(List)};
   * o.w. the instance is initialized via {@linkplain #init(SourceRow)}. Invoking
   * the wrong {@code init(..)} method results in an {@code UnsupportedOperationException}
   * being thrown.
   */
  public boolean isCompound() {
    return false;
  }
  
  /**
   * Initializes the instance with the given {@code rowset} and returns the generated cell.
   * @see #isCompound()
   * @see #isInitialized()
   * @see #generatedCell */
  public CellData init(List<SourceRow> rowset) throws UnsupportedOperationException {
    throw new UnsupportedOperationException();
  }
  
  
  public CellDataProvider<?> provider() {
    return provider;
  }


  /**
   * Appends a cell to the given PDF {@code table} using the instance's
   * {@linkplain #generatedCell}, or a blank cell, if not initialized.
   */
  @Override
  public void appendTable(Rectangle borders, CellFormat format, PdfPTable table) {
    CellData delegate = generatedCell == null ? TextCell.BLANK : generatedCell;
    delegate.appendTable(borders, format, table);
  }


  
  
  public CellData effectiveCell() {
    return generatedCell == null ? TextCell.BLANK : generatedCell;
  }
  
  
  
  
  
  
  

  static abstract class ColumnCell extends SourcedCell {

    final int columnIndex;
    
    
    ColumnCell(int columnIndex, CellDataProvider<?> provider) {
      this(columnIndex, provider, null);
    }
    
    ColumnCell(int columnIndex, CellDataProvider<?> provider, CellFormat format) {
      super(provider, format);
      this.columnIndex = columnIndex;
      if (columnIndex < 0)
        throw new IndexOutOfBoundsException("columnIndex " + columnIndex);
    }
    
    
    public final int columnIndex() {
      return columnIndex;
    }

    
    boolean hasColumn(SourceRow sourceRow) {
      return sourceRow.getColumns().size() > columnIndex;
    }
    
    boolean hasColumn(SourceRow sourceRow, ColumnType type) {
      return hasColumn(sourceRow) && hasColumnType(sourceRow, type);
    }
    
    boolean hasColumnType(SourceRow sourceRow, ColumnType type) throws IndexOutOfBoundsException {
      return sourceRow.getColumns().get(columnIndex).getType() == type;
    }
    
    
    Object columnValue(SourceRow sourceRow) throws IndexOutOfBoundsException {
      return sourceRow.getColumns().get(columnIndex).getValue();
    }
    
  }
  
  
  public static class DateCell extends ColumnCell {
    
    
    
    public DateCell(int columnIndex, DateProvider dateProvider) {
      this(columnIndex, dateProvider, null);
    }
    
    public DateCell(int columnIndex, DateProvider dateProvider, CellFormat format) {
      super(columnIndex, dateProvider, format);
    }

    
    
    @Override
    public CellData init(SourceRow sourceRow) {
      generatedCell = null;
      if (hasColumn(sourceRow, ColumnType.DATE))
        generatedCell =
          ((DateProvider) provider).getCellData((Long) columnValue(sourceRow), format);
      return effectiveCell();
    }
  }
  
  
  
  public static class NumberCell extends ColumnCell {

    final NumberFunc func;

    public NumberCell(
        int columnIndex, NumberProvider provider, NumberFunc func, CellFormat format) {
      super(columnIndex, provider, format);
      this.func = func;
      if (func != null && func.getArgCount() != 1)
        throw new IllegalArgumentException(
            "expected 1-arg function; actual is " + func.getArgCount() + "-arg");
    }
    
    
    boolean hasNumberType(SourceRow sourceRow) {
      return
          hasColumn(sourceRow) && (
            hasColumnType(sourceRow, ColumnType.LONG) ||
            hasColumnType(sourceRow, ColumnType.DOUBLE) );
    }

    @Override
    public CellData init(SourceRow sourceRow) {
      generatedCell = null;
      if (hasNumberType(sourceRow))
        generateCell((Number) columnValue(sourceRow));
      return effectiveCell();
    }
    
    
    /** Applies the function (if any) to the given value and then generates a cell. */
    void generateCell(Number value) {
      if (func != null)
        value = func.asFunc().apply(value);
      generatedCell = ((NumberProvider) provider).getCellData(value, format);
    }
    
    /** @return may be {@code null} */
    public NumberFunc func() {
      return func;
    }
    
  }
  
  
  public static class Sum extends NumberCell {

    public Sum(int columnIndex, NumberProvider provider, NumberFunc func, CellFormat format) {
      super(columnIndex, provider, func, format);
    }
    

    /** Not supported.
     * @see #init(List) */
    @Override
    public CellData init(SourceRow sourceRow) throws UnsupportedOperationException {
      throw new UnsupportedOperationException();
    }
    
    /** @return {@code true} */
    @Override
    public boolean isCompound() {
      return true;
    }
    
    
    /**
     * Sums numeric column values, applies the {@linkplain #func()}, if any,
     * to the sum and then generates a cell.
     */
    public CellData init(List<SourceRow> rowset) {
      generatedCell = null;
      var type = columnType(rowset);
      if (type == null)
        return effectiveCell();
      Number total;
      if (type.isLong()) {
        long sum = 0;
        for (var row : rowset) {
          if (hasNumberType(row))
            sum += ((Number) columnValue(row)).longValue();
        }
        total = sum;
      } else {
        double sum = 0;
        for (var row : rowset) {
          if (hasNumberType(row))
            sum += ((Number) columnValue(row)).doubleValue();
        }
        total = sum;
      }
      generateCell(total);
      return effectiveCell();
    }
    
    
    private ColumnType columnType(List<SourceRow> rowset) {
      for (var row : rowset) {
        if (!hasColumn(row))
          continue;
        var type = row.getColumns().get(columnIndex).getType();
        if (type.isLong() || type.isDouble())
          return type;
      }
      return null;
    }
    
    
  }
  
  
  public static class SourcedImage extends ColumnCell {
    
    public SourcedImage(
        int columnIndex, ImageProvider provider, CellFormat format) {
      super(columnIndex, provider, format);
    }

    @Override
    public CellData init(SourceRow sourceRow) {
      generatedCell = null;
      if (hasColumn(sourceRow, ColumnType.BYTES)) {
        var bytes = (ByteBuffer) columnValue(sourceRow);
        generatedCell = ((ImageProvider) provider).getCellData(bytes, format);
      }
      return effectiveCell();
    }
    
  }
  
  
  
  public static class StringCell extends ColumnCell {

    StringCell(int columnIndex, StringProvider provider, CellFormat format) {
      super(columnIndex, provider, format);
    }

    @Override
    public CellData init(SourceRow sourceRow) {
      generatedCell = null;
      if (hasColumn(sourceRow))
        generatedCell = ((StringProvider) provider).getCellData(columnValue(sourceRow), format);
      return effectiveCell();
    }
    
  }
  
  
  
  
}
