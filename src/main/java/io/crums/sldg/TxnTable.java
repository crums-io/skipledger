/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg;

import java.nio.ByteBuffer;
import java.util.ConcurrentModificationException;
import java.util.Objects;

import io.crums.io.buffer.ReadWriteBuffer;

/**
 * Single transaction view over another table.
 */
final class TxnTable implements Table {
  
  
  private final Table primary;
  private final long sizeSnapshot;
  
  private ReadWriteBuffer txnBuffer;

  /**
   * 
   */
  public TxnTable(Table primary, int capacity) {
    this.primary = Objects.requireNonNull(primary, "null primary");
    this.sizeSnapshot = primary.size();
    
    if (capacity <= 0)
      throw new IllegalArgumentException("capacity " + capacity);
    this.txnBuffer = new ReadWriteBuffer(capacity * ROW_WIDTH);
  }

  
  
  @Override
  public long addRows(ByteBuffer row, long index) {
    
    int rowCount = row.remaining() / ROW_WIDTH;
    
    if (rowCount > remaining())
      throw new IllegalStateException(
          "row count (" + rowCount + ") greater than remaining capacity " + remaining());
    
    // internal code expects better use, so we only assert
    assert rowCount > 0 && row.remaining() % ROW_WIDTH == 0;
    
    if (index != size())
      throw new ConcurrentModificationException("on index " + index + "; size() " + size());
    
    txnBuffer.put(row);
    
    long sz = size();
    
    assert sz == index + rowCount;
    
    return sz;
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
    return primary.addRows(txnBuffer.readBuffer(), sizeSnapshot);
  }

}
