/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg;

import java.nio.ByteBuffer;
import java.util.Objects;

import io.crums.io.buffer.ReadWriteBuffer;

/**
 * Single transaction view over another table. Used when <em>multiple</em>
 * input-hashes are to be committed to the skipledger chain.
 * 
 * <h2>Raison D'Etre</h2>
 * <p>
 * The computation of row-hashes is dependent on the row-hashes of previous
 * rows. Some of these are recorded on the chain; others (the new rows), are
 * recorded in this class's "transaction buffer".
 * Writes ({@linkplain #writeRows(ByteBuffer, long)}) get written to the
 * instance's "transaction buffer";
 * reads ({@linkplain #readRow(long)}), depending on {@code index} argument,
 * come from either the chain, or the transaction buffer.
 * {@linkplain CompactSkipLedger} uses instances of this class in order to write
 * rows <em>en bloc</em> to underlying storage (also a
 * {@linkplain SkipTable}), instead of one-by-one.
 * </p>
 */
final class TxnTable implements SkipTable {
  
  
  private final SkipTable primary;
  private final long sizeSnapshot;
  
  private ReadWriteBuffer txnBuffer;

  /**
   * 
   */
  public TxnTable(SkipTable primary, long sizeSnapshot, int capacity) {
    this.primary = Objects.requireNonNull(primary, "null primary");
    this.sizeSnapshot = sizeSnapshot;
    if (capacity <= 0)
      throw new IllegalArgumentException("capacity " + capacity);
    
    this.txnBuffer = new ReadWriteBuffer(capacity * ROW_WIDTH);
  }

  
  
  
  
  
  @Override
  public long writeRows(ByteBuffer row, long index) {
    
    if (row.remaining() != ROW_WIDTH)
      throw new IllegalArgumentException(
          "expected to write one row at-a-time. row [%d]: %s"
          .formatted(index + 1, row));
    
    if (remaining() == 0)
      throw new IllegalStateException(
          "capacity breached on row [%d]".formatted(index + 1));
    
    long sz = size();
    if (index != sz)
      throw new SldgException("on index " + index + "; size() " + sz);
    
    txnBuffer.put(row);
    
    return sz + 1;
  }
  
  

  @Override
  public ByteBuffer readRow(long index) {
    
    if (index < sizeSnapshot)
      return primary.readRow(index);
    
    if (index >= size())
      throw new IllegalArgumentException("index " + index + " > size " + size());
    
    int pos = (int) (index - sizeSnapshot) * ROW_WIDTH;
    int limit = pos + ROW_WIDTH;
    
    return txnBuffer.readBuffer().limit(limit).position(pos);
  }
  
  
  @Override
  public long size() {
    return sizeSnapshot + txnBuffer.readableBytes() / ROW_WIDTH;
  }
  
  
  public int capacity() {
    return txnBuffer.capacity() / ROW_WIDTH;
  }
  
  
  /**
   * Returns the remaining capacity.
   */
  public int remaining() {
    return txnBuffer.writeableBytes() / ROW_WIDTH;
  }



  public long commit() {
    return primary.writeRows(txnBuffer.readBuffer(), sizeSnapshot);
  }

}
