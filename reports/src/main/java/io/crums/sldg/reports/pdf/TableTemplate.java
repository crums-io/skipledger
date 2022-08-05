/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf;


import java.awt.Color;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;

import com.lowagie.text.Rectangle;
import com.lowagie.text.pdf.PdfPTable;

import io.crums.sldg.src.SourceRow;

/**
 * 
 */
public class TableTemplate {
  
  private final static Comparator<Integer> SERIAL_COMP =
      new Comparator<Integer>() {

        @Override
        public int compare(Integer a, Integer b) {
          int left = a;
          int right = b;
          if (left < 0) {
            if (right < 0)
              return Integer.compare(-left, -right);
            return 1;
          }
          if (right < 0)
            return -1;
          return Integer.compare(left, right);
        }
      };

      
  private final TreeMap<Integer, CellData> fixedCells = new TreeMap<>(SERIAL_COMP);
  
  private final List<ColumnTemplate> columns;
  private final boolean consumesSources;
  
  private float[] columnWidths;

  
  
  private LineSpec tableBorderH;
  private LineSpec tableBorderV;
  
  private LineSpec defaultLineH;
  private LineSpec defaultLineV;
  
  private float docPercentage = 100;
  
  
  private CellData defaultCell = CellData.TextCell.BLANK;
  
  
  @Override
  public final int hashCode() {
    int hash = tableBorderH.hashCode();
    hash *= 7;
    hash += tableBorderV.hashCode();
    hash *= 7;
    hash += defaultLineH.hashCode();
    hash *= 7;
    hash += defaultLineV.hashCode();
    hash *= 7;
    hash += Float.hashCode(docPercentage);
    if (columnWidths != null) for (var w : columnWidths) {
      hash *= 7;
      hash += Float.hashCode(w);
    }
    hash *= 7;
    hash += columns.hashCode();
    return hash;
  }
  
  
  @Override
  public final boolean equals(Object o) {
    return
        o instanceof TableTemplate t &&
        t.columns.equals(columns) &&
        t.tableBorderH.equals(tableBorderH) &&
        t.tableBorderV.equals(tableBorderV) &&
        t.defaultLineH.equals(defaultLineH) &&
        t.defaultLineV.equals(defaultLineV) &&
        Arrays.equals(t.columnWidths, columnWidths);
  }
  
  
  /**
   * 
   */
  public TableTemplate(List<ColumnTemplate> columns) {
    Objects.requireNonNull(columns, "null columns");
    this.columns = List.copyOf(columns);
    this.consumesSources = this.columns.stream().anyMatch(ColumnTemplate::usesSource);
    if (columns.isEmpty())  // how about MAX_COLUMNS ?
      throw new IllegalArgumentException("instance has no columns");
    defaultLineV = defaultLineH = tableBorderV = tableBorderH = LineSpec.BLANK;
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
  
  
  public final List<ColumnTemplate> getColumns() {
    return columns;
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
   * Sets a cell at the given <em>serial</em> index.
   * Note the <em>fixed</em> moniker is a reference to the cell's <em>position</em>:
   * it's value may be dynamically determined (at least, that's the plan).
   * 
   * @see #toSerialIndex(int, int)
   * @see #setFixedCell(int, int, CellData)
   */
  public CellData setFixedCell(
      int serialIndex, CellData cell) throws IndexOutOfBoundsException {
    
    Objects.requireNonNull(cell, "null cell");
    checkReasonableRow(toRowIndex(serialIndex));
    return fixedCells.put(serialIndex, cell);
  }
  
  /**
   * Sets the value at the cell with the given (zero-based) column
   * and row coordinates. Equivalent to
   * {@code setFixedCell(toSerialIndex(col, row), cell)}.
   * 
   * @param col   &ge; 0, &lt; {@linkplain #getColumnCount()}
   * @param row   zero-based, but with special sematics for negative values
   * @param cell  the cell data (possibly dynamic)
   * 
   * @see #toSerialIndex(int, int)
   * @see #setFixedCell(int, CellData)
   */
  public void setFixedCell(
      int col, int row, CellData cell) throws IndexOutOfBoundsException {
    
    setFixedCell(toSerialIndex(col, row), cell);
  }
  
  
  /**
   * Returns a read-only view of the instance's fixed cells.
   * 
   * @return not null
   */
  public SortedMap<Integer, CellData> getFixedCells() {
    return Collections.unmodifiableSortedMap(fixedCells);
  }
  
  
  /**
   * Converts the given cell coordinates to a serial index. <em>Negative</em>
   * row numbers have special semantics: they mean <em>after</em> the regular
   * (dynamic) rows: -1 means the first row after the dynamic rows, -2 the second row
   * after, and so on.
   * 
   * @param col   &ge; 0, &lt; {@linkplain #getColumnCount()}
   * @param row   may be negative (!)
   * @return {@code row * colsPerRow + (Integer.signum(row) * col)}
   * @throws IndexOutOfBoundsException if {@code col} is out-of-bounds, or {@code row} is unreasonably large
   * @see #MAX_TABLE_ROWS
   */
  public int toSerialIndex(int col, int row) throws IndexOutOfBoundsException {
    int colsPerRow = getColumnCount();
    if (col < 0 || col >= colsPerRow)
      throw new IndexOutOfBoundsException("col: " + col);
    checkReasonableRow(row);
    return row * colsPerRow + (Integer.signum(row) * col);
  }



  /**
   * Returns the row index. Note negative return values with special
   * semantics.
   * 
   * @param serialIndex may be negative (!)
   * @see #toSerialIndex(int, int)
   */
  public int toRowIndex(int serialIndex) {
    return serialIndex / getColumnCount();
  }

  /**
   * Returns the zero-based column index for the given serial index
   * 
   * @param serialIndex possibly negative (!)
   * 
   * @return &ge; 0
   * @see #toSerialIndex(int, int)
   */
  public int toColIndex(int serialIndex) {
    return Math.abs(serialIndex) % getColumnCount();
  }


  /** Ballpark max rows in PDF table. Used in sanity check. */
  public final static int MAX_TABLE_ROWS = 1_000_000;

  private void checkReasonableRow(int row) {
    if (Math.abs(row) > MAX_TABLE_ROWS)
      throw new IndexOutOfBoundsException("too many rows [" + row + "] (sanity check)");
  }
  
  
  
  
  
  
  
  
  
  

  

  
  
  /**
   * 
   * @param rowset
   * @return
   */
  public PdfPTable createTable(List<SourceRow> rowset) {
    if (Objects.requireNonNull(rowset, "null rowset").isEmpty())
      throw new IllegalArgumentException("empty rowset");
    
    var table = initPdfTable();
    
    // need the following info..
    // 1. fixed cells (fixed here means the position is 
    // 2. column sources
    // 3. final rows
    
    // first row..
    int row = 0;
    int[] sourcePtr = { 0 };
    TreeMap<Integer, CellData> fixedPending = new TreeMap<>(SERIAL_COMP);
    fixedPending.putAll(fixedCells);
    
    List<CellData> tableRow = generateRow(row, sourcePtr, rowset, fixedPending);
    ++row;
    {
      boolean finished = finished(sourcePtr[0], rowset, fixedPending);
      injectFirstRow(tableRow.iterator(), table, finished);
      if (finished)
        return table;
    }
    
    
    while (true) {
      tableRow = generateRow(row, sourcePtr, rowset, fixedPending);
      ++row;
      if (finished(sourcePtr[0], rowset, fixedPending))
        break;
      injectMidRow(tableRow.iterator(), table);
    }
    injectLastRow(tableRow.iterator(), table);
    
    return table;
  }
  
  
  
  
  private boolean finished(
      int sourceIndex,
      List<SourceRow> sources,
      TreeMap<Integer, CellData> fixedPending) {
    
    
    return
        (!consumesSources || sourceIndex >= sources.size()) &&
        fixedPending.isEmpty();
  }
  
  
  
  private List<CellData> generateRow(
      int row,
      int[] sourcePtr,    // c-like hack
      List<SourceRow> rowset,
      TreeMap<Integer, CellData> fixedPending) {
    
    final int baseIndex = toSerialIndex(0, row);      // (inc)
    
    boolean srcConsumed = false;
    
    // set the working source row, if the table consumes sources
    // (consume here means generate a row per element in rowset)
    SourceRow sourceRow =
        consumesSources && sourcePtr[0] < rowset.size() ?
            rowset.get(sourcePtr[0]) :
              null;
    

    final int cc = getColumnCount();
    List<CellData> out = new ArrayList<>(cc);
    
    for (int col = 0; col < cc; ++col) {
      CellData fixedCell = fixedPending.remove(baseIndex + col);
      if (fixedCell != null) {
        fixedCell = prepareCell(fixedCell, rowset);
        out.add(fixedCell);
        
      } else if (sourceRow != null && columns.get(col).usesSource()) {
          var cell = makeSourcedCell(col, sourceRow);
          out.add(cell);
          srcConsumed = true;
        
      } else
        out.add(defaultCell);
    }
    
    if (srcConsumed)
      sourcePtr[0]++;

    // if there are no more source rows to consume AND the remaining cells
    // are all indexed implicitly (using negative numbers to signify the
    // *last* rows), then rekey those implicit values to the actual values
    // of the occasion..
    
    boolean nomoSrcs = !consumesSources || sourcePtr[0] >= rowset.size();
    if (nomoSrcs && !fixedPending.isEmpty() && fixedPending.firstKey() < 0) {
      // rewrite the map so that its keys are anchored to the last row number
      var rekeyed = new TreeMap<Integer, CellData>();
      for (var entry : fixedPending.entrySet()) {
        int serialIndex = entry.getKey();
        int rowIndex = toRowIndex(serialIndex);
        int colIndex = toColIndex(serialIndex);
        rekeyed.put(toSerialIndex(colIndex, row - rowIndex), entry.getValue());
      }
      fixedPending.clear();
      fixedPending.putAll(rekeyed);
    }
    
    return out;
  }


  
  private CellData prepareCell(CellData cellData, List<SourceRow> rowset) {
    if (cellData instanceof SourcedCell srcCell) {
      if (srcCell.isCompound())
        srcCell.init(rowset);
      else for (var row : rowset) {
        srcCell.init(row);
        if (srcCell.isInitialized())
          break;
      }
    }
    return cellData;
  }


  private CellData makeSourcedCell(int col, SourceRow sourceRow) {
    return columns.get(col).getProtoSrc().init(sourceRow);
  }

  




  
  protected PdfPTable initPdfTable() {
    var table = new PdfPTable(getColumnCount());
    table.setWidthPercentage(docPercentage);
    if (this.columnWidths != null)
      table.setWidths(columnWidths);
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
    cells.next().appendTable(borders, columns.get(0).getFormat(), table);
  }
  
  private void injectLastColumnCell(Iterator<CellData> cells, Rectangle borders, PdfPTable table) {
    cells.next().appendTable(borders, columns.get(columns.size() - 1).getFormat(), table);
  }
  

  
  private void injectMidColumns(Iterator<CellData> cells, Rectangle borders, PdfPTable table) {
    final int lastIndex = columns.size() - 1;
    for (int index = 1; index < lastIndex; ++index)
      cells.next().appendTable(borders, columns.get(index).getFormat(), table);
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
  
}




























