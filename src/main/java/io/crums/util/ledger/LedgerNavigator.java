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
 * A dressed up <tt>SkipLedger</tt>.
 * 
 * <h3>Design Note</h3>
 * 
 * <p>This could have easily been made into a <tt>SkipLedger</tt> subclass
 * (by making it a {@linkplain FilterLedger} subclass, for example), but this
 * lack of inheritance is to separate high level design choices from lower
 * level ones. 
 * </p>
 */
public class LedgerNavigator implements Digest {
  
  
  private final SkipLedger ledger;
  
  
  
  public LedgerNavigator(SkipLedger ledger) {
    this.ledger = Objects.requireNonNull(ledger, "null ledger");
  }
  
  
  /**
   * Appends the given hash.
   * 
   * @param entryHash the <em>hash</em> of the entry
   * 
   * @return     the added entry's row number
   * 
   * @see SkipLedger#appendRow(ByteBuffer)
   */
  public synchronized long addEntry(ByteBuffer entryHash) {
    return ledger.appendRow(entryHash);
  }
  
  
  
  /**
   * 
   * @param entryHashes
   * 
   * @return the new size of the ledger, or equivalently, the row number of the last entry added
   */
  public synchronized long addEntries(ByteBuffer entryHashes) {
    return ledger.appendRowsEnBloc(entryHashes);
  }
  
  
  
  /**
   * Returns the underlying ledger.
   */
  public SkipLedger getLedger() {
    return ledger;
  }
  
  
  /**
   * Returns entry at the given row number as a full row containing both the
   * entry hash and the hash pointers to previous rows.
   * 
   * @param rowNumber 1-based row number &ge; 1 and &le; {@linkplain #size()}
   * 
   * @return not null
   */
  public Row getRow(long rowNumber) {
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
  
  
  
  public SkipPath skipPath(long lo, long hi) {
    if (hi > size())
      throw new IllegalArgumentException("hi " + hi + " > size " + size());
    if (lo < 1)
      throw new IllegalArgumentException("lo " + lo + " < 1");
    
    List<Long> rowNumPath = SkipLedger.skipPathNumbers(lo, hi);
    int length = rowNumPath.size();
    Row[] rows = new Row[length];
    for (int index = length; index-- > 0; )
      rows[index] = getRow(rowNumPath.get(index));
    
    return new SkipPath(Lists.asReadOnlyList(rows), newDigest());
  }
  
  
  
  public SkipPath skipPath() {
    long size = size();
    return size == 0 ? null : skipPath(1, size);
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
