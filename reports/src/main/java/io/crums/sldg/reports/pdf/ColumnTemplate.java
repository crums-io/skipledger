/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf;

import io.crums.sldg.reports.pdf.model.CellDataProvider;
import io.crums.sldg.src.ColumnType;

/**
 * A PDF table column. This describes default behavior for a column.
 */
public class ColumnTemplate {
  
  private final static int SOURCE_INDEX_NOT_SET = -2;
  private final static int SOURCE_INDEX_ROWNUM = -1;
  
  

  private CellFormat format;
  
  /** &ge; -2. -1 means a row's row-number. */
  private int sourceIndex;
  
  private ColumnType columnType;
  
//  private CellDataProvider<?> 

  /**
   * 
   */
  public ColumnTemplate() {  }
  
  
  public ColumnTemplate(CellFormat format, int columnNo) {
    this.format = format;
    this.sourceIndex = columnNo - 1;
    if (columnNo < -1)
      throw new IllegalArgumentException("column number: " + columnNo);
  }
}
