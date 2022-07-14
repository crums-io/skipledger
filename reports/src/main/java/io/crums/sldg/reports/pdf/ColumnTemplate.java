/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf;

import java.util.Objects;

import io.crums.sldg.reports.pdf.model.CellDataProvider;
import io.crums.sldg.reports.pdf.model.func.NumberFunc;
import io.crums.sldg.src.ColumnType;

/**
 * A PDF table column. This describes default behavior for a column.
 */
public class ColumnTemplate {
  
//  private final static int NO_SOURCE_INDEX = -2;
//  
//  private final static int SOURCE_INDEX_ROWNUM = -1;
  
  

  private final CellFormat format;
  private final SourcedCell protoSrc;
  
//  /** &ge; -1. -1 means a row's row-number. */
//  private final int sourceIndex;
//  
//  
//  
//  private ColumnType columnType;
//  
//  private NumberFunc func;
  
//  private CellDataProvider<?> 

  
  
  public ColumnTemplate(CellFormat format, SourcedCell protoSrc) {
    this.format = format;
    this.protoSrc = protoSrc;
  }
  
  
  
  
  
  
  
  public boolean usesSource() {
    return protoSrc != null;
  }
  
  
  public SourcedCell getProtoSrc() {
    return protoSrc;
  }
  
  
  
  public CellFormat getFormat() {
    return format;
  }
  
  
}














