/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.util.ledger;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

import io.crums.util.Lists;
import io.crums.util.hash.Digest;

/**
 * 
 */
public class LedgerNavigator implements Digest {
  
  private final SkipLedger ledger;
  
  
  
  public LedgerNavigator(SkipLedger ledger) {
    this.ledger = Objects.requireNonNull(ledger, "null ledger");
  }
  
  
  
  public Row addEntry(ByteBuffer entryHash) {
    long rowNumber = ledger.appendRow(entryHash);
    ByteBuffer rowData = ledger.getRow(rowNumber);
    return new Row(rowNumber, rowData, false);
  }
  
  
  
  public Row getEntry(long rowNumber) {
    ByteBuffer row = ledger.getRow(rowNumber);
    return new Row(rowNumber, row, false);
  }
  
  
  /**
   * Returns the number of rows in the ledger.
   * 
   * @see SkipLedger#size()
   */
  public long size() {
    return ledger.size();
  }
  
  
  
  public SkipPath vForm(long lo, long hi) {
    if (hi > size())
      throw new IllegalArgumentException("hi " + hi + " > size " + size());
    if (lo < 1)
      throw new IllegalArgumentException("lo " + lo + " < 1");
    
    List<Long> rowNumPath = SkipLedger.skipPathNumbers(lo, hi);
    int length = rowNumPath.size();
    Row[] rows = new Row[length];
    for (int index = length; index-- > 0; )
      rows[index] = getEntry(rowNumPath.get(index));
    
    return new SkipPath(Lists.asReadOnlyList(rows), newDigest());
  }
  
  
  
  public SkipPath vForm() {
    long size = size();
    return size == 0 ? null : vForm(1, size);
  }



  @Override
  public int hashWidth() {
    return ledger.hashWidth();
  }



  @Override
  public String hashAlgo() {
    return ledger.hashAlgo();
  }
  
  
  
  

}
