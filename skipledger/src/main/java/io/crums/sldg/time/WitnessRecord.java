/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.time;


import io.crums.model.CrumRecord;
import io.crums.sldg.HashConflictException;
import io.crums.sldg.RowHash;


/**
 * A {@linkplain #row() row} and its witness {@linkplain #record() record}.
 */
public final class WitnessRecord {
  
  private final RowHash row;
  private final CrumRecord record;
  
  public WitnessRecord(RowHash row, CrumRecord record) {
    this.row = row;
    this.record = record;
    if (!record.crum().hash().equals(row.hash()))
      throw new HashConflictException("crum hash mismatch for row " + row);
  }
  
  
  
  public long utc() {
    return record.crum().utc();
  }
  
  public long rowNum() {
    return row.no();
  }
  
  public boolean isTrailed() {
    return record.isTrailed();
  }
  
  
  public RowHash row() {
    return row;
  }
  
  public CrumRecord record() {
    return record;
  }
  
  /**
   * Equality semantics decided solely by {@linkplain #row}.
   */
  public boolean equals(Object o) {
    return o == this || (o instanceof WitnessRecord) && ((WitnessRecord) o).row.equals(row);
  }
  
  public int hashCode() {
    return row.hashCode();
  }
  
}
