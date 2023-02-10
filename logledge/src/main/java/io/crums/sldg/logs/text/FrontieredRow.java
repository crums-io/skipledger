/*
 * Copyright 2023 Babak Farhang
 */
package io.crums.sldg.logs.text;


import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

import io.crums.sldg.src.ColumnValue;
import io.crums.sldg.src.SourceRow;
import io.crums.sldg.Row;
import io.crums.sldg.cache.HashFrontier;

/**
 * 
 */
public record FrontieredRow(
    List<ColumnValue> columns,
    HashFrontier preFrontier,
    long eolOffset) {

  public FrontieredRow {
    Objects.requireNonNull(preFrontier, "null preFrontier");
    if (columns.isEmpty())
      throw new IllegalArgumentException("empty columns");
    if (eolOffset <= 0) {
      eolOffset = -1L;
    }
  }
  
  
  public FrontieredRow(List<ColumnValue> columns, HashFrontier preFrontier) {
    this(columns, preFrontier, -1L);
  }
  
  public SourceRow sourceRow() {
    return new SourceRow(rowNumber(), columns);
  }
  
  
  public long rowNumber() {
    return preFrontier.rowNumber() + 1;
  }
  
  
  public ByteBuffer rowHash() {
    return frontier().frontierHash();
  }
  
  
  public ByteBuffer inputHash() {
    return SourceRow.rowHash(columns, null, null);
  }
  
  
  
  public HashFrontier frontier() {
    return preFrontier.nextFrontier(inputHash());
  }
  
  
  FrontieredRow eolOffset(long eol) {
    return new FrontieredRow(columns, preFrontier, eol);
  }

}
