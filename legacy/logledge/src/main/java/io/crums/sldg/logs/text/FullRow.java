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
 * A row with backing data. Each instance contains sufficient information
 * to create the source row, the [skip] ledgered row, the hash frontier
 * both before the source row was added and after. The line no. and EOL
 * (end-of-row) offset are mostly informational and are not self-validating.
 * 
 * 
 * @param columns     not null, not empty row columns
 * @param preFrontier the hash frontier <em>before</em> the row with the given
 *                    columns
 * @param eolOffset   <em>ending</em> offset of row in the source (exclusive);
 *                    {@code -1}, if not set.
 * @param lineNo      the row's line number in the source
 * 
 * @see #rowNumber()
 * @see #sourceRow()
 * @see #frontier()
 * @see #inputHash()
 * @see #rowHash()
 * @see #row()
 */
public record FullRow(
    List<ColumnValue> columns,
    HashFrontier preFrontier,
    long eolOffset,
    long lineNo) {

  public FullRow {
    Objects.requireNonNull(preFrontier, "null preFrontier");
    if (columns.isEmpty())
      throw new IllegalArgumentException("empty columns");
    if (eolOffset <= 0)
      throw new IllegalArgumentException("EOL offset " + eolOffset);
    
    long rn = preFrontier.rowNumber() + 1;
    if (lineNo < rn)
      lineNo = rn;
    if (eolOffset < lineNo)
      throw new IllegalArgumentException(
          "EOL offset %d < line no. %d".formatted(eolOffset, lineNo));
  }
  
  
  
  /**
   * The source row.
   * 
   * @return {@code new SourceRow(rowNumber(), columns)}
   */
  public SourceRow sourceRow() {
    return new SourceRow(rowNumber(), columns);
  }
  
  /**
   * The row number.
   * 
   * @return {@code preFrontier.rowNumber() + 1}
   */
  public long rowNumber() {
    return preFrontier.rowNumber() + 1;
  }
  
  /**
   * The row hash.
   * 
   * @return {@code frontier().frontierHash()}
   */
  public ByteBuffer rowHash() {
    return frontier().frontierHash();
  }
  
  
  /**
   * The input hash for this row number.
   * 
   * @return {@code }
   */
  public ByteBuffer inputHash() {
    return SourceRow.rowHash(columns);
  }
  
  
  /**
   * The hash frontier at this row number
   * 
   * @return {@code preFrontier.nextFrontier(inputHash())}
   */
  public HashFrontier frontier() {
    return preFrontier.nextFrontier(inputHash());
  }
  
  
  /** Sets and returns a copy of this instance with the given EOL offset. */
  FullRow eolOffset(long eol) {
    return new FullRow(columns, preFrontier, eol, lineNo);
  }
  
  
  /**
   * The full skipledger row.
   * 
   * @return {@code preFrontier.nextRow(inputHash())}
   */
  public Row row() {
    return preFrontier.nextRow(inputHash());
  }
  
  
  /**
   * The state of the log at this row.
   * 
   * @return {@code new State(frontier(), eolOffset, lineNo)}
   */
  public State toState() {
    return new State(frontier(), eolOffset, lineNo);
  }

}





