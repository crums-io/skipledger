/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.reports.pdf;


import java.awt.Color;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.function.Consumer;

import com.lowagie.text.Rectangle;
import com.lowagie.text.pdf.PdfPTable;


/**
 * A template for creating a PDF table from a sequence (list) of values.
 * Some table cells may be pre-populated (fixed).
 * 
 * @see #setFixedCell(int, int, CellData)
 * @see #setFixedCell(int, CellData)
 * 
 * @see #createTable(List)
 */
public class TableTemplate {
  
  /**
   * Maximum number of columns allocated by this class.
   */
  public final static int MAX_COLUMNS = 512;
  
  
  public static boolean equal(TableTemplate a, TableTemplate b) {
    return
        a == b ||
        a != null && b != null &&
        a.columns.equals(b.columns) &&
        a.tableBorderH.equals(b.tableBorderH) &&
        a.tableBorderV.equals(b.tableBorderV) &&
        a.defaultLineH.equals(b.defaultLineH) &&
        a.defaultLineV.equals(b.defaultLineV) &&
        Arrays.equals(a.columnWidths, b.columnWidths);
  }
  
  
  
  private final List<CellFormat> columns;
  
  
  // pushed down from FixedTable
  protected final TreeMap<Integer, CellData> fixedCells;
  
  
  
  
  private float[] columnWidths;

  
  
  private LineSpec tableBorderH;
  private LineSpec tableBorderV;
  
  private LineSpec defaultLineH;
  private LineSpec defaultLineV;
  
  private float docPercentage = 100;
  
  
  
  
  
  /**
   * Creates a new instance with the given column formats with no border or grid lines.
   * Instances are <em>always</em> in a proper state.
   * 
   * @param columns not-empty
   */
  public TableTemplate(List<CellFormat> columns) {
    this.columns = List.copyOf(Objects.requireNonNull(columns, "null columns"));
    if (columns.isEmpty())  // how about MAX_COLUMNS ?
      throw new IllegalArgumentException("instance has no columns");
    fixedCells = new TreeMap<>();
    defaultLineV = defaultLineH = tableBorderV = tableBorderH = LineSpec.BLANK;
  }
  
  
  /**
   * Creates a new instance with the specified number of columns, all with a
   * common font. The generated column formats are separate instances and are
   * indepedent. No border or grid lines set.
   * 
   * @param columns &ge; 1 and &le; {@linkplain #MAX_COLUMNS}
   * @param font    not null
   */
  public TableTemplate(int columns, FontSpec font) {
    if (columns <= 0 || columns > MAX_COLUMNS)
      throw new IllegalArgumentException("columns: " + columns);
    var colArray = new CellFormat[columns];
    for (int index = 0; index < columns; ++index)
      colArray[index] = new CellFormat(font);
    this.columns = List.of(colArray);
    fixedCells = new TreeMap<>();
    defaultLineV = defaultLineH = tableBorderV = tableBorderH = LineSpec.BLANK;
  }
  
  
  /**
   * Shallow copy constructor.
   */
  protected TableTemplate(TableTemplate copy) {
    this.columns = Objects.requireNonNull(copy, "null table").columns;
    this.fixedCells = copy.fixedCells;
    this.columnWidths = copy.columnWidths;
    this.tableBorderH = copy.tableBorderH;
    this.tableBorderV = copy.tableBorderV;
    this.defaultLineH = copy.defaultLineH;
    this.defaultLineV = copy.defaultLineV;
    this.docPercentage = copy.docPercentage;
  }
  
  
  
  /**
   * Sets the column widths as <em>relative</em> values. Must match the
   * number of columns {@linkplain #getColumnCount()}.
   */
  public void setColumnWidths(List<? extends Number> relativeWidths) {
    final int count = relativeWidths.size();
    if (count != columns.size())
      throw new IllegalArgumentException(
          "wrong number of args (" + count +
          "); must match column count " + columns.size());
    var widths = new float[count];
    for (int index = count; index--> 0; ) {
      widths[index] = relativeWidths.get(index).floatValue();
      if (widths[index] <= 0)
        throw new IllegalArgumentException(
            "relativeWidth [" + index + "]: " + widths[index]);
    }
    this.columnWidths = widths;
  }


  /**
   * Sets the column widths as <em>relative</em> values. Must match the
   * number of columns {@linkplain #getColumnCount()}.
   */
  public void setColumnWidths(float...relativeWidths) {
    final int count = relativeWidths.length;
    if (count != columns.size())
      throw new IllegalArgumentException(
          "wrong number of args (" + count +
          "); must match column count " + columns.size());
    var widths = new float[count];
    for (int index = count; index--> 0; ) {
      widths[index] = relativeWidths[index];
      if (widths[index] <= 0)
        throw new IllegalArgumentException(
            "relativeWidth [" + index + "] expected positive: actual was " + widths[index]);
    
    }
    this.columnWidths = widths;
  }
  
  
  /**
   * Returns the column widths. If not present, then the columns
   * default to equal width.
   */
  public Optional<float[]> getColumnWidths() {
    if (columnWidths == null)
      return Optional.empty();
    float[] copy = new float[columnWidths.length];
    for (int index = copy.length; index-- > 0; )
      copy[index] = columnWidths[index];
    return Optional.of(copy);
  }
  
  
  public final int getColumnCount() {
    return columns.size();
  }
  
  
  public final boolean isSingleColumn() {
    return columns.size() == 1;
  }
  
  
  
  public void setGridLines(LineSpec line) {
    setTableBorders(line);
    setDefaultLines(line);
  }
  
  
  public void setTableBorders(LineSpec line) {
    setHorizontalBorder(line);
    setVerticalBorder(line);
  }
  public void setHorizontalBorder(LineSpec line) {
    this.tableBorderH = Objects.requireNonNull(line);
  }
  public void setVerticalBorder(LineSpec line) {
    this.tableBorderV = Objects.requireNonNull(line);
  }
  
  
  public void setDefaultLines(LineSpec hv) {
    setDefaultHorizontalLine(hv);
    setDefaultVerticalLine(hv);
  }
  public void setDefaultHorizontalLine(LineSpec line) {
    this.defaultLineH = Objects.requireNonNull(line);
  }
  public void setDefaultVerticalLine(LineSpec line) {
    this.defaultLineV = Objects.requireNonNull(line);
  }
  
  
  
  public final boolean sameGridLines() {
    return sameBorders() && sameDefaultLines() && tableBorderH.equals(defaultLineH);
  }
  
  
  public final boolean sameBorders() {
    return tableBorderH.equals(tableBorderV);
  }
  
  public final boolean sameDefaultLines() {
    return defaultLineH.equals(defaultLineV);
  }
  
  
  public void setDocPercentage(float docPercentage) {
    if (docPercentage > 100)
      docPercentage = 100;
    else if (docPercentage <= 0)
      throw new IllegalArgumentException("docPercentage: " + docPercentage);
    this.docPercentage = docPercentage;
  }
  
  
  public final float getDocPercentage() {
    return docPercentage;
  }
  
  
  
  
  /**
   * Returns the serial index of the given cell coordinates. This is the
   * (zero-based) serial index of a table cell in a row-by-row traversal,
   * starting from the first row and first column. Convenience method.
   * 
   * @param col zero based column index
   * @param row zero based row index
   * 
   * @return {@code row * getColumnCount() + col}
   * @throws IndexOutOfBoundsException if the indices are out of bounds or negative
   */
  public int toSerialIndex(int col, int row) throws IndexOutOfBoundsException {
    int colsPerRow = getColumnCount();
    if (col < 0 || col >= colsPerRow)
      throw new IndexOutOfBoundsException("col: " + col);
    checkNegativeIndex(row, "row");
    return row * colsPerRow + col;
  }
  
  
  /**
   * Returns the row index for the given serial index. Convenience method.
   * 
   * @param serialIndex &ge; 0
   * @return {@code serialIndex / getColumnCount()}
   */
  public int toRowIndex(int serialIndex) throws IndexOutOfBoundsException {
    checkNegativeIndex(serialIndex, "serialIndex");
    return serialIndex / getColumnCount();
  }
  

  /**
   * Returns the column index for the given serial index. Convenience method.
   * 
   * @param serialIndex &ge; 0
   * @return {@code serialIndex % getColumnCount()}
   */
  public int toColIndex(int serialIndex) throws IndexOutOfBoundsException {
    checkNegativeIndex(serialIndex, "serialIndex");
    return serialIndex % getColumnCount();
  }
  
  
  private void checkNegativeIndex(int value, String name) {
    if (value < 0)
      throw new IndexOutOfBoundsException(name + ": " + value);
  }
  
  
  

  /**
   * Creates and returns a new PDF table.
   * 
   * @param cells with size adding up to a multiple of {@linkplain #getColumnCount()} (after
   *              accounting for fixed cells)
   * 
   * @return the new PDF table
   * @throws IllegalArgumentException
   *         if the size of {@code cells} leaves a row only partially defined
   * 
   * @see #interlaceFixed(List)
   */
  public PdfPTable createTable(List<CellData> cells) throws IllegalArgumentException {
    var table = initPdfTable();
    var ic = interlaceFixed(cells);
    return appendRows(ic, table);
  }

  
  
  protected PdfPTable initPdfTable() {
    var table = new PdfPTable(getColumnCount());
    table.setWidthPercentage(docPercentage);
    if (this.columnWidths != null)
      table.setWidths(columnWidths);
    return table;
  }
  
  
  /**
   * Interlaces and returns the given list of cells as a new list with the fixed cells
   * injected. The interlacing algorithm is such that the given {@code cells} fill in
   * the gaps in the instance's fixed cells.
   * <p>
   * <em>However</em>, the algorithm is lenient in the following way: the
   * interlacing stops at the first gap (in fixed cells) that fail to be filled
   * with the given argument; the remaining fixed cells, if any, are not returned.
   * </p>
   */
  protected List<CellData> interlaceFixed(List<CellData> cells) {
    Objects.requireNonNull(cells, "null cells");
    if (fixedCells.isEmpty())
      return cells;

    var cellsPlusFixed = new ArrayList<CellData>();
    var inputIter = cells.iterator();
    var fixedIter = fixedCells.entrySet().iterator();
    
    var nextFixedEntry = nextFixedEntry(fixedIter);
    int nextFixedIndex = nextFixedIndex(nextFixedEntry);
    
    while (inputIter.hasNext()) {
      while (cellsPlusFixed.size() == nextFixedIndex) {
        cellsPlusFixed.add(nextFixedEntry.getValue());
        nextFixedEntry = nextFixedEntry(fixedIter);
        nextFixedIndex = nextFixedIndex(nextFixedEntry);
      }
      cellsPlusFixed.add(inputIter.next());
    }
    
    while (cellsPlusFixed.size() == nextFixedIndex) {
      cellsPlusFixed.add(nextFixedEntry.getValue());
      nextFixedEntry = nextFixedEntry(fixedIter);
      nextFixedIndex = nextFixedIndex(nextFixedEntry);
    }
    
    return cellsPlusFixed;
  }
  
  

  private int nextFixedIndex(Entry<Integer, CellData> fixedEntry) {
    return fixedEntry == null ? Integer.MAX_VALUE : fixedEntry.getKey();
  }
  
  
  private Entry<Integer, CellData> nextFixedEntry(Iterator<Entry<Integer, CellData>> fixedIter) {
    return fixedIter.hasNext() ? fixedIter.next() : null;
  }
  
  
  
  
  protected PdfPTable appendRows(Collection<CellData> cells, PdfPTable table) {

    final int rows;
    {
      int count = cells.size();
      if (count == 0)
        throw new IllegalArgumentException("empty cells");
      
      int cc = columns.size();
      rows = count / cc;
      if (rows * cc != count)
        throw new IllegalArgumentException(
            "cell count (" + count + ") not a multiple of column count (" + cc + ")");
    }
    
    var iter = cells.iterator();
    
    int countDown = rows;
    if (table.getRows().isEmpty()) {
      injectFirstRow(iter, table, rows == 1);
      if (--countDown == 0)
        return table;
    }
    countDown--;
    while (countDown-- > 0)
      injectMidRow(iter, table);
    
    injectLastRow(iter, table);
    
    return table;
  }
  
  
  
  
  /**
   * Injects last-row cells.
   */
  private void injectLastRow(Iterator<CellData> cells, PdfPTable table) {
    injectSingleRow(cells, table, false, true);
  }
  
  private void injectMidRow(Iterator<CellData> cells, PdfPTable table) {
    injectSingleRow(cells, table, false, false);
  }
  
  /**
   * Injects first-row cells with table outer-borders. No cell bottom border,
   * unless it's a 1-row table.
   * 
   * @param andLast if {@code true} then a 1-row table
   */
  private void injectFirstRow(Iterator<CellData> cells, PdfPTable table, boolean andLast) {
    injectSingleRow(cells, table, true, andLast);
  }
  
  
  /**
   * Injects a row's worth of cells.
   * 
   * @param first {@code true} if the first row
   * @param last  {@code true} if the last row
   */
  private void injectSingleRow(Iterator<CellData> cells, PdfPTable table, boolean first, boolean last) {
    var borders = new Rectangle(0, 0);
    setBorderTop(borders, first ? tableBorderH : defaultLineH);
    setBorderLeft(borders, tableBorderV);
    if (last)
      setBorderBottom(borders, tableBorderH);
    
    boolean single = isSingleColumn();
    
    if (single)
      setBorderRight(borders, tableBorderV);
    
    injectFirstColumnCell(cells, borders, table);
    if (!single) {
      setBorderLeft(borders, defaultLineV);
      // inject the middle columns (there may be none)
      injectMidColumns(cells, borders, table);
      setBorderRight(borders, tableBorderV);
      injectLastColumnCell(cells, borders, table);
    }
  }
  
  
  private void injectFirstColumnCell(Iterator<CellData> cells, Rectangle borders, PdfPTable table) {
    cells.next().appendTable(borders, columns.get(0), table);
  }
  
  private void injectLastColumnCell(Iterator<CellData> cells, Rectangle borders, PdfPTable table) {
    cells.next().appendTable(borders, columns.get(columns.size() - 1), table);
  }
  

  
  private void injectMidColumns(Iterator<CellData> cells, Rectangle borders, PdfPTable table) {
    final int lastIndex = columns.size() - 1;
    for (int index = 1; index < lastIndex; ++index)
      cells.next().appendTable(borders, columns.get(index), table);
  }
  
  
  
 
  
  
  private void setBorderTop(Rectangle borders, LineSpec line)  {
    setLine(borders::setBorderWidthTop, borders::setBorderColorTop, line);
  }
  private void setBorderBottom(Rectangle borders, LineSpec line)  {
    setLine(borders::setBorderWidthBottom, borders::setBorderColorBottom, line);
  }
  private void setBorderLeft(Rectangle borders, LineSpec line)  {
    setLine(borders::setBorderWidthLeft, borders::setBorderColorLeft, line);
  }
  private void setBorderRight(Rectangle borders, LineSpec line)  {
    setLine(borders::setBorderWidthRight, borders::setBorderColorRight, line);
  }
  
  private void setLine(Consumer<Float> widthFunc, Consumer<Color> colorFunc, LineSpec line) {
    if (line == null)
      return;
    widthFunc.accept(line.getWidth());
    if (line.hasColor())
      colorFunc.accept(line.getColor());
  }






  /**
   * Returns a read-only view of the columns. (The individual elements are
   * <em>not</em> read-only, unforturnately.)
   */
  public List<CellFormat> getColumns() {
    return Collections.unmodifiableList(columns);
  }






  public LineSpec getHorizontalBorder() {
    return tableBorderH;
  }






  public LineSpec getVerticalBorder() {
    return tableBorderV;
  }






  public LineSpec getDefaultHorizontalLine() {
    return defaultLineH;
  }






  public LineSpec getDefaultVerticalLine() {
    return defaultLineV;
  }
  
  
  

  
  
  /**
   * Returns a read-only view of the fixed cells, keyed by serial index.
   * 
   * @see #toSerialIndex(int, int)
   */
  public final SortedMap<Integer, CellData> getFixedCells() {
    return Collections.unmodifiableSortedMap(fixedCells);
  }
  
  
  /**
   * Returns the non-default fixed cells. (This base class knows nothing about
   * <em>default</em> cells: it's here for the benefit of the JSON parsers.)
   */
  public SortedMap<Integer, CellData> getNonDefaultFixedCells() {
    return getFixedCells();
  }

  
  /**
   * Sets a fixed value at the cell with the given <em>serial</em> index.
   * 
   * @see #toSerialIndex(int, int)
   * @see #setFixedCell(int, int, CellData)
   */
  public CellData setFixedCell(
      int serialIndex, CellData cell) throws IndexOutOfBoundsException {
    
    Objects.requireNonNull(cell, "null cell");
    if (serialIndex < 0)
      throw new IllegalArgumentException(
          "negative serial index: " + serialIndex);
    return fixedCells.put(serialIndex, cell);
  }


  /**
   * Sets the fixed value at the cell with the given (zero-based) column
   * and row coordinates. Equivalent to
   * {@code setFixedCell(toSerialIndex(col, row), cell)}.
   * 
   * @see #toSerialIndex(int, int)
   * @see #setFixedCell(int, CellData)
   */
  public void setFixedCell(
      int col, int row, CellData cell) throws IndexOutOfBoundsException {
    
    setFixedCell(toSerialIndex(col, row), cell);
  }
  
  
  /**
   * Returns the minimum number arguments this instance requires to
   * {@linkplain #createTable(List) create a PDF table}.
   * 
   * @return &ge; 0
   */
  public int getMinArgCount() {
    return fixedCells.isEmpty() ? 1 : fixedCells.lastKey() + 1 - fixedCells.size();
  }
  
  
}











