/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.util.ledger;


import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * In-memory ledger. TODO: Bridge to {@linkplain DirectLedger}.
 */
public class VolatileLedger extends SkipLedger {
  
  // Not using ReadWriteBuffer because we're allowing read-only also
  
  private final ByteBuffer cells;
  private final ByteBuffer view;
  
  
  
  public VolatileLedger(int rows) {
    this(ByteBuffer.allocate(rows * 3 * DEF_DIGEST.hashWidth()), 0);
  }
  
  
  /**
   * Creates a new empty instance.
   * 
   * @param mem as memory: positional state doesn't matter
   */
  public VolatileLedger(ByteBuffer mem) {
    this(mem, 0);
  }

  /**
   * Creates a new instance using the given backing memory. To create
   * a read-only ledger (one that cannot be appended to), use a read-only
   * buffer.
   * 
   * @param mem as memory: positional state doesn't matter
   * @param rows the number of existing rows (non-negative)
   */
  public VolatileLedger(ByteBuffer mem, int rows) {
    
    // clear positional info
    Objects.requireNonNull(mem, "null mem").clear();

    this.cells = mem;
    this.view = mem.asReadOnlyBuffer();
    
    if (rows < 0)
      throw new IllegalArgumentException("negative rows: " + rows);
    
    long nextPosition = cellNumber(rows + 1) * hashWidth();
    if (nextPosition > cells.capacity())
      throw new IllegalArgumentException(
          "mem capacity " + cells.capacity() + " with " + rows + " rows");
    
    int endOffset = (int) nextPosition;
    cells.position(endOffset);
    view.limit(endOffset);
  }


  @Override
  public long cellCount() {
    return view.remaining() / hashWidth();
  }
  
  
  public int maxRows() {
    int cellCount = cells.capacity() / hashWidth();
    return (int) maxRows(cellCount);
  }
  
  
  

  @Override
  protected ByteBuffer getCells(long index, int count) {
    int cellWidth = hashWidth();
    long offset = index * cellWidth;
    long limit = offset + (count * cellWidth);
    
    // code below performs equivalent check
    
//    if (limit < 0 || limit > cellNumber(rows + 1))
//      throw new IllegalArgumentException(
//          "attempt to access undefined cells @" + index + ":" + count);
    
    ByteBuffer snapshot;
    
    if (cells.isReadOnly())
      snapshot = view.slice();
    else synchronized (lock()) {
      snapshot = view.slice();
    }
    
    return snapshot.position((int) offset).limit((int) limit);
  }

  @Override
  protected void putCells(long index, ByteBuffer cells) {
    int cellWidth = hashWidth();
    long offset = index * cellWidth;
    synchronized (lock()) {
      if (this.cells.position() != offset)
        throw new IllegalArgumentException("index " + index + "; frontier position " + this.cells.position());
      this.cells.put(cells);
      view.limit(this.cells.position());
      
    }
  }
  
  
  
  private Object lock() {
    return view;
  }

}
