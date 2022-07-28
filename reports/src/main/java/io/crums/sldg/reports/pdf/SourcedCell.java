/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;

import com.lowagie.text.Rectangle;
import com.lowagie.text.pdf.PdfPTable;

import io.crums.sldg.reports.pdf.CellDataProvider.DateProvider;
import io.crums.sldg.reports.pdf.CellDataProvider.ImageProvider;
import io.crums.sldg.reports.pdf.CellDataProvider.NumberProvider;
import io.crums.sldg.reports.pdf.CellDataProvider.StringProvider;
import io.crums.sldg.reports.pdf.func.BaseNumFunc;
import io.crums.sldg.reports.pdf.func.NumOp;
import io.crums.sldg.src.ColumnType;
import io.crums.sldg.src.SourceRow;
import io.crums.util.Lists;

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
 * </p><p>
 * TODO: make {@linkplain NumberCell} like {@linkplain Sum}, so that it takes
 * multiple columns.
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
  
  
  
  
  
  
  
  

  public static abstract class ColumnCell extends SourcedCell {

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
    
    
    public final int getColumnIndex() {
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
    
    
    final boolean baseEquals(ColumnCell cell) {
      return
          columnIndex == cell.columnIndex &&
          provider.equals(cell.provider) &&
          Objects.equals(format, cell.format);
    }
    
    final int baseHashCode() {
      return provider.hashCode() * 31 + columnIndex;
    }
    
  }
  
  
  public static class DateCell extends ColumnCell {
    
    private final static int CH = DateCell.class.hashCode();
    
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
          provider().getCellData((Long) columnValue(sourceRow), format);
      return effectiveCell();
    }
    
    
    @Override
    public DateProvider provider() {
      return (DateProvider) provider;
    }
    
    
    @Override
    public final boolean equals(Object o) {
      return o instanceof DateCell d && baseEquals(d);
    }
    
    @Override
    public final int hashCode() {
      return baseHashCode() ^ CH;
    }
  }
  
  
  static abstract class BaseNumber extends ColumnCell {


    final BaseNumFunc func;
    
    BaseNumber(int columnIndex, NumberProvider provider, BaseNumFunc func, CellFormat format) {
      super(columnIndex, provider, format);
      this.func = func;
      if (func != null && func.getArgCount() != 1)
        throw new IllegalArgumentException(
            "expected 1-arg function; actual is " + func.getArgCount() + "-arg");
    }


    
    
    @Override
    public NumberProvider provider() {
      return (NumberProvider) provider;
    }

    
    
    /** @return may be {@code null} */
    public BaseNumFunc func() {
      return func;
    }
    
    
    /** Applies the function (if any) to the given value and then generates a cell. */
    void generateCell(Number value) {
      if (func != null)
        value = func.asFunction().apply(value);
      generatedCell = provider().getCellData(value, format);
    }
    
    
    boolean hasNumberType(SourceRow sourceRow) {
      return
          hasColumn(sourceRow) && (
            hasColumnType(sourceRow, ColumnType.LONG) ||
            hasColumnType(sourceRow, ColumnType.DOUBLE) );
    }
    
    boolean baseNumberEquals(BaseNumber other) {
      return baseEquals(other) && Objects.equals(func, other.func);
    }
    
    
    int baseNumberHashCode() {
      int base = baseHashCode();
      return func == null ? base : base * 31 + func.hashCode();
    }
    
  }
  
  
  public static class NumberCell extends BaseNumber {
    
    private final static int CH = NumberCell.class.hashCode();

    public NumberCell(
        int columnIndex, NumberProvider provider, BaseNumFunc func, CellFormat format) {
      super(columnIndex, provider, func, format);
    }

    @Override
    public CellData init(SourceRow sourceRow) {
      generatedCell = null;
      if (hasNumberType(sourceRow))
        generateCell((Number) columnValue(sourceRow));
      return effectiveCell();
    }
    

    
    @Override
    public final boolean equals(Object o) {
      return o instanceof NumberCell d && baseNumberEquals(d);
    }
    
    @Override
    public final int hashCode() {
      return baseNumberHashCode() ^ CH;
    }
    
  }
  
  
  public static class Sum extends BaseNumber {
    
    private final static int CH = Sum.class.hashCode();
    
    private static int first(List<Integer> colIndexes) {
      Objects.requireNonNull(colIndexes, "null column indexes");
      if (colIndexes.isEmpty())
        throw new IllegalArgumentException("empty column indexes");
      return colIndexes.get(0);
    }
    
    private final List<Integer> colIndexes;
    private final BaseNumFunc columnsFunc;
    

    /**
     * Base constructor.
     * 
     * @param colIndexes  source row column indices in ascending order. At least one
     * @param columnsFunc  optionally {@code null} only if there is only one column.
     *                    Must have the same {@linkplain BaseNumFunc#getArgCount() arg count}
     *                    as the column count ({@code colIndexes.size()})
     * @param provider    not null
     * @param func        optional (may be null)
     * @param format      optional (may be null)
     */
    public Sum(
        List<Integer> colIndexes,
        BaseNumFunc columnsFunc,
        NumberProvider provider,
        BaseNumFunc func,
        CellFormat format) {
      super(first(colIndexes), provider, func, format);
      this.colIndexes = colIndexes;
      this.columnsFunc = columnsFunc;
      if (colIndexes.size() > 1) {
        var colSet = new TreeSet<>(colIndexes);
        if (colSet.size() != colIndexes.size())
          throw new IllegalArgumentException("duplicated column indexes: " + colIndexes);
        if (colSet.first() < 0)
          throw new IllegalArgumentException("negative column index: " + colSet.first());
      }
      final int cc = colIndexes.size();
      if (cc > 1) {
        Objects.requireNonNull(columnsFunc, "null columns func with " + cc + " columns");
        int ac = columnsFunc.getArgCount();
        if (ac != cc)
          throw new IllegalArgumentException(
              "expected column func with " + cc + " arguments; given func has " + ac + " arguments");
      }
    }
    
    
    
    public List<Integer> getColumnIndexes() {
      return Collections.unmodifiableList(colIndexes);
    }
    
    
    /** @return may be null (!) */
    public BaseNumFunc getColumnsFunc() {
      return columnsFunc;
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
      
      Number total = ZERO;
      for (var row : rowset)
        total = NumOp.ADD.apply(total, computeRowValue(row));
      generateCell(total);
      return effectiveCell();
    }
    
    
    private final static Number ZERO = 0;
    
    private Number computeRowValue(SourceRow row) {
      if (columnsFunc == null)
        return hasNumberType(row) ? ((Number) columnValue(row)).longValue() : ZERO;
      
      final int cc = colIndexes.size();
      if (row.getColumns().size() <= colIndexes.get(cc - 1))
        return ZERO;
      
      var arguments = new ArrayList<Number>(cc);
      for (var colIndex : colIndexes) {
        var column = row.getColumns().get(colIndex);
        var type = column.getType();
        if (type.isLong() || type.isDouble())
          arguments.add((Number) column.getValue());
        else
          return ZERO;
      }
      
      return columnsFunc.eval(arguments);
    }
    
    
    private ColumnType columnType(List<SourceRow> rowset) {
      for (var colIndex : colIndexes) {
        var type = columnType(rowset, colIndex);
        if (type == null || type.isDouble())
          return type;
      }
      return ColumnType.LONG;
    }
    
    private ColumnType columnType(List<SourceRow> rowset, int colIndex) {
      for (var row : rowset) {
        if (!hasColumn(row))
          continue;
        var type = row.getColumns().get(colIndex).getType();
        if (type.isLong() || type.isDouble())
          return type;
      }
      return null;
    }


    
    @Override
    public final boolean equals(Object o) {
      return
          o instanceof Sum s && baseNumberEquals(s) &&
          colIndexes.equals(s.colIndexes) &&
          Objects.equals(columnsFunc, s.columnsFunc);
    }
    
    @Override
    public final int hashCode() {
      int base = baseNumberHashCode() ^ CH;
      return columnsFunc == null ? base : base * 31 + columnsFunc.hashCode();
    }
    
  }
  
  
  public static class SourcedImage extends ColumnCell {
    
    private final static int CH = SourcedImage.class.hashCode();
    
    public SourcedImage(
        int columnIndex, ImageProvider provider, CellFormat format) {
      super(columnIndex, provider, format);
    }

    @Override
    public CellData init(SourceRow sourceRow) {
      generatedCell = null;
      if (hasColumn(sourceRow, ColumnType.BYTES)) {
        var bytes = (ByteBuffer) columnValue(sourceRow);
        generatedCell = provider().getCellData(bytes, format);
      }
      return effectiveCell();
    }
    
    @Override
    public ImageProvider provider() {
      return (ImageProvider) provider;
    }

    
    @Override
    public final boolean equals(Object o) {
      return o instanceof SourcedImage s && baseEquals(s);
    }
    
    @Override
    public final int hashCode() {
      return baseHashCode() ^ CH;
    }
    
  }
  
  
  
  public static class StringCell extends ColumnCell {
    
    private final static int CH = StringCell.class.hashCode();

    public StringCell(int columnIndex, StringProvider provider, CellFormat format) {
      super(columnIndex, provider, format);
    }

    @Override
    public CellData init(SourceRow sourceRow) {
      generatedCell = null;
      if (hasColumn(sourceRow))
        generatedCell = provider().getCellData(columnValue(sourceRow), format);
      return effectiveCell();
    }
    
    @Override
    public StringProvider provider() {
      return (StringProvider) provider;
    }
    

    @Override
    public final int hashCode() {
      return baseHashCode() ^ CH;
    }
    
    @Override
    public final boolean equals(Object o) {
      return o instanceof StringCell s && baseEquals(s);
    }
    
  }
  
  
  public static class MultiStringCell extends SourcedCell {
    
    private final static int CH = MultiStringCell.class.hashCode();
    
    public final static String DEFAULT_SEP = ", ";

    private final List<Integer> columns;
    
    private String separator = DEFAULT_SEP;
    
    public MultiStringCell(List<Integer> columns, StringProvider provider, CellFormat format) {
      super(provider, format);
      Objects.requireNonNull(columns, "null columns");
      this.columns = Collections.unmodifiableList(columns);
      if (columns.isEmpty())
        throw new IllegalArgumentException("empty columns");
      if (columns.get(0) < 0)
        throw new IllegalArgumentException("negative column index: " + columns.get(0));
      if (!Lists.isSortedNoDups(columns))
        throw new IllegalArgumentException("column indices not sorted or contains duplicates: " + columns);
    }
    
    
    public String getSeparator() {
      return separator;
    }
    
    public MultiStringCell setSeparator(String separator) {
      this.separator = separator == null ? DEFAULT_SEP : separator;
      return this;
    }
    
    @Override
    public StringProvider provider() {
      return (StringProvider) provider;
    }
    
    
    public List<Integer> getColumnIndexes() {
      return columns;
    }
    

    @Override
    public CellData init(SourceRow sourceRow) {
      generatedCell = null;
      var string = new StringBuilder();
      final int sc = sourceRow.getColumns().size();
      for (var col : columns) {
        if (col >= sc)
          break;
        
        var type = sourceRow.getColumns().get(col).getType();
        boolean accept = type.isString() || type.isLong() || type.isDouble();
        if (!accept)
          continue;

        if (!string.isEmpty())
          string.append(separator);
        
        string.append(sourceRow.getColumns().get(col).getValue());
      }
      generatedCell = provider().getCellData(string, format);
      return effectiveCell();
    }
    
    
    
    @Override
    public final int hashCode() {
      int hash = separator.hashCode() * 31;
      hash += provider.hashCode();
      hash *= 31;
      hash += columns.hashCode();
      if (format != null) {
        hash *= 31;
        hash += format.hashCode();
      }
      return hash ^ CH;
    }
    
    public final boolean equals(Object o) {
      return
          o instanceof MultiStringCell other &&
          other.separator.equals(separator) &&
          other.provider.equals(provider) &&
          other.columns.equals(columns) &&
          Objects.equals(format, other.format);
    }
    
  }
  
  
}











