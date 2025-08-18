/*
 * Copyright 2020-2021 Babak Farhang
 */
package io.crums.sldg;


import java.nio.ByteBuffer;
import java.util.ConcurrentModificationException;

import io.crums.io.buffer.ReadWriteBuffer;

/**
 * In-memory storage for a {@linkplain CompactSkipLedger}.
 * 
 */
public class VolatileTable implements SkipTable {
  
  
  private double expansionFactor = 1.5;
  
  private volatile ReadWriteBuffer mem;

  
  
  /**
   * Creates an instance with initial capacity of 16 rows.
   */
  public VolatileTable() {
    this(16);
  }

  /**
   * Creates an instance with the specified initial capacity.
   * 
   * @param initCapacity &ge; 1
   */
  public VolatileTable(int initCapacity) {
    if (initCapacity < 1)
      throw new IllegalArgumentException("initCapacity " + initCapacity);
    mem = new ReadWriteBuffer(initCapacity * ROW_WIDTH);
  }




  @Override
  public long writeRows(ByteBuffer rows, long index) {
    
    ReadWriteBuffer mem = this.mem;
    if (index != size(mem))
      throw new ConcurrentModificationException("on index " + index);
    
    int count = rows.remaining() / ROW_WIDTH;
    
    if (count > remaining(mem)) {
      int min = (int) (index + count) * ROW_WIDTH;
      int capacity = Math.max(min, (int) (mem.capacity() * expansionFactor) );
      ReadWriteBuffer copy = new ReadWriteBuffer(capacity);
      copy.put(mem.readBuffer());
      mem = copy;
    }
    mem.put(rows);
    long sz = size(mem);
    this.mem = mem;
    return sz;
  }

  
  
  @Override
  public ByteBuffer readRow(long index) {
    if (index < 0)
      throw new IllegalArgumentException("index " + index);
    
    ReadWriteBuffer mem = this.mem;
    if (index >= size(mem))
      throw new IllegalArgumentException(
          "index " + index + " out of bounds; size " + size(mem));
    
    int pos = (int) index * ROW_WIDTH;
    int limit = pos + ROW_WIDTH;
    
    return mem.readBuffer().limit(limit).position(pos);
  }

  
  
  @Override
  public long size() {
    return size(mem);
  }
  
  
  /**
   * <p>Supported.</p>
   * {@inheritDoc}
   */
  @Override
  public void trimSize(long newSize) {
    final long sz = size();
    if (newSize == sz)
      return;
    if (newSize > sz)
      throw new IllegalArgumentException(
          "newSize (%d) > size() (%d)".formatted(newSize, sz));
    if (newSize < 0L)
      throw new IllegalArgumentException("newSize: " + newSize);
    mem.position((int) newSize * ROW_WIDTH);
  }

  public ByteBuffer serialize() {
    return mem.readBuffer();
  }
  
  
  private long size(ReadWriteBuffer mem) {
    return mem.readableBytes() / ROW_WIDTH;
  }
  
  
  private int remaining(ReadWriteBuffer mem) {
    return mem.writeableBytes() / ROW_WIDTH;
  }

}

