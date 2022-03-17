/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.reports.pdf;


import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;

import com.lowagie.text.pdf.PdfPTable;

import io.crums.reports.pdf.CellData.TextCell;

/**
 * A table with a fixed number of rows.
 */
public class FixedTable extends TableTemplate {
  
  
  /**
   * Tests for equality. Instance equality decisions delayed this way, for now.
   */
  public static boolean equal(FixedTable a, FixedTable b) {
    return
        a == b ||
        TableTemplate.equal(a, b) &&
        a.rows == b.rows &&
        Objects.equals(a.defaultCell, b.defaultCell) &&
        a.fixedCells.equals(b.fixedCells);
  }
  
  public final static int MAX_ROWS = 0xffff;
  
  
  
  
  
  
  
  
  private final int rows;

  private TextCell defaultCell;
  
  
  
  /**
   * Constructs an instance with a fixed number of rows.
   * 
   * @param columns non null nor empty
   * @param rows &ge; 1
   */
  public FixedTable(List<CellFormat> columns, int rows) {
    super(columns);
    this.rows = checkRows(rows);
  }
  
  
  /**
   * Constructs an instance with a fixed number of rows.
   * 
   * @param cols    &ge; 1
   * @param rows    &ge; 1
   * @param font    not null
   */
  public FixedTable(int cols, int rows, FontSpec font) {
    super(cols, font);
    this.rows = checkRows(rows);
  }
  
  /**
   * Promotion constructor.
   * 
   * @param table shallow copied
   * @param rows  &ge; 1
   */
  public FixedTable(TableTemplate table, int rows) {
    super(table);
    this.rows = checkRows(rows);
  }
  
  private int checkRows(int rows) {
    if (rows < 1 || rows > MAX_ROWS)
      throw new IllegalArgumentException("rows: " + rows);
    return rows;
  }
  
  
  
  
  
  
  
  /**
   * Returns the number of rows in this table. Fixed at construction.
   */
  public final int getRowCount() {
    return rows;
  }
  
  
  /**
   * <p>Overridden to bounds check {@code serialIndex} against
   * {@linkplain #getCellCount()}.
   * </p>
   * {@inheritDoc}
   */
  public CellData setFixedCell(
      int serialIndex, CellData cell) throws IndexOutOfBoundsException {
    if (serialIndex >= getCellCount())
      throw new IndexOutOfBoundsException(
          "serial index " + serialIndex + " > fixed cell count " + getCellCount());
    return super.setFixedCell(serialIndex, cell);
  }
  
  
  /**
   * Sets the remaining unset cells to the given value. 
   * 
   * @param blank non-null, but usually a blank value.
   */
  public void setRemainingCells(CellData blank) {
    final int cc = getCellCount();
    for (int index = 0; fixedCells.size() < cc; ++index)
      fixedCells.putIfAbsent(index, blank);
  }
  

  /**
   * Sets the default value for every cell. On return the table {@linkplain #isFixed() is fixed}.
   * 
   * @param blank non-null, but usually a blank value.
   */
  public void setDefaultCell(TextCell blank) {
    setRemainingCells(blank);
    this.defaultCell = blank;
  }
  
  
  public final Optional<TextCell> getDefaultCell() {
    return Optional.ofNullable(defaultCell);
  }
  
  
  public final SortedMap<Integer, CellData> getNonDefaultFixedCells() {
    if (defaultCell == null)
      return getFixedCells();
    var out = new TreeMap<Integer, CellData>();
    for (var e : fixedCells.entrySet()) {
      if (!e.getValue().equals(defaultCell))
        out.put(e.getKey(), e.getValue());
    }
    return Collections.unmodifiableSortedMap(out);
  }
  
  
  /**
   * Determines whether no fixed cells have been set.
   */
  public final boolean isEmpty() {
    return fixedCells.isEmpty();
  }
  
  
  /**
   * Returns the total number of cells that must be input (dynamically)
   * in order to create a complete table. This is the expected size of
   * of the {@linkplain #createTable(List)} arguments.
   */
  public final int getDynamicCellCount() {
    return getCellCount() - fixedCells.size();
  }
  
  
  /**
   * If the instance is fixed, then {@linkplain #createTable(List)} takes no arguments:
   * an empty list.
   */
  public final boolean isFixed() {
    return getCellCount() == fixedCells.size();
  }
  
  
  /**
   * Returns the total number of cells in the table (fixed and dynamic).
   * 
   * @return {@code getRowCount() * getColumnCount()}
   */
  public final int getCellCount() {
    return rows * getColumnCount();
  }
  
  
  
  /**
   * Creates and returns a new PDF table with no input cells. This only works if the
   * instance {@linkplain #isFixed() is fixed}. 
   * 
   * @throws IllegalStateException if not a fixed instance.
   */
  public PdfPTable createTable() throws IllegalStateException {
    if (!isFixed())
      throw new IllegalStateException(
          "invoked while state is not fixed: " + getDynamicCellCount() + " cells expected");
    return createTable(List.of());
  }
  



  /**
   * {@inheritDoc}
   * 
   * @param cells of size {@linkplain #getDynamicCellCount()}
   */
  @Override
  public PdfPTable createTable(List<CellData> cells) throws IllegalArgumentException {
    // check arg
    if (Objects.requireNonNull(cells, "null cells").size() != getDynamicCellCount())
      throw new IllegalArgumentException(
          "input size " + cells.size() + " must be " + getDynamicCellCount());
    return super.createTable(cells);
  }





  
  

  
  
  
  

}







