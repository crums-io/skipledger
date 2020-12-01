/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.util.ledger;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Objects;

/**
 * Filter (or decorator) pattern for a <tt>SkipLedger</tt>.
 */
public class FilterLedger extends SkipLedger {
  
  protected final SkipLedger ledger;

  /**
   * Creates an instance whose I/O is delegated to the underlying given <tt>ledger</tt>.
   */
  public FilterLedger(SkipLedger ledger) {
    this.ledger = Objects.requireNonNull(ledger, "null ledger");
  }

  @Override
  public long size() {
    return ledger.size();
  }
  

  @Override
  public MessageDigest newDigest() {
    return ledger.newDigest();
  }

  @Override
  public int hashWidth() {
    return ledger.hashWidth();
  }

  @Override
  public String hashAlgo() {
    return ledger.hashAlgo();
  }
  
  
  @Override
  public ByteBuffer sentinelHash() {
    return ledger.sentinelHash();
  }
  
  
  
  

  @Override
  protected ByteBuffer getCells(long index, int count) {
    return ledger.getCells(index, count);
  }

  @Override
  protected void putCells(long index, ByteBuffer cells) {
    ledger.putCells(index, cells);
  }

}
