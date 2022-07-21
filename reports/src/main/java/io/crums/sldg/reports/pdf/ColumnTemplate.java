/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf;


import java.util.Objects;

/**
 * A PDF table column. This describes default behavior for a column.
 */
public class ColumnTemplate {
  
  

  private final CellFormat format;
  private final SourcedCell protoSrc;
  
  
  public ColumnTemplate(CellFormat format, SourcedCell protoSrc) {
    this.format = Objects.requireNonNull(format, "null format");
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














