/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf;


import java.util.Objects;

/**
 * A PDF table column. This describes default behavior for a column.
 */
public class ColumnTemplate {
  
  private final static int CH = ColumnTemplate.class.hashCode();

  private final CellFormat format;
  private final SourcedCell protoSrc;
  
  
  public ColumnTemplate(CellFormat format, SourcedCell protoSrc) {
    this.format = Objects.requireNonNull(format, "null format");
    this.protoSrc = protoSrc;
  }
  
  
  
  
  
  
  
  public final boolean usesSource() {
    return protoSrc != null;
  }
  
  
  public SourcedCell getProtoSrc() {
    return protoSrc;
  }
  
  
  
  public CellFormat getFormat() {
    return format;
  }
  
  
  @Override
  public final int hashCode() {
    int hash = format.hashCode();
    if (usesSource())
      hash = hash * 31 + protoSrc.hashCode();
    return hash ^ CH;
  }
  
  @Override
  public final boolean equals(Object o) {
    return
        o instanceof ColumnTemplate other &&
        other.format.equals(format) &&
        Objects.equals(other.protoSrc,  protoSrc);
  }
  
}














