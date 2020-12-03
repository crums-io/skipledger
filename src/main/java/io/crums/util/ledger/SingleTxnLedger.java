/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.util.ledger;

import java.nio.ByteBuffer;
import java.util.ConcurrentModificationException;
import java.util.logging.Logger;

import io.crums.io.buffer.ReadWriteBuffer;

/**
 * Implements transactional semantics for {@linkplain #appendRowsEnBloc(java.nio.ByteBuffer)}.
 */
class SingleTxnLedger extends FilterLedger {
  
  private final long snapCellCount;
  
  private ReadWriteBuffer preCommitCells;

  /**
   * @param ledger
   */
  public SingleTxnLedger(SkipLedger ledger) {
    super(ledger);
    this.snapCellCount = ledger.cellCount();
  }

  
  
  
  
  @Override
  public long appendRowsEnBloc(ByteBuffer entryHashes) {
    if (preCommitCells != null)
      throw new IllegalStateException("already invoked: may only be invoked once");
    
    int cellWidth = hashWidth();
    int newRows = checkedEntryCount(entryHashes, cellWidth);
    long snapSize = maxRows(snapCellCount);
    long newSize = snapSize + newRows;
    long postCommitCellCount = cellNumber(newSize + 1);
    int newCells = (int) (postCommitCellCount - snapCellCount);
    
    this.preCommitCells = new ReadWriteBuffer(newCells * hashWidth());
    for (int i = 0; i < newRows; ++i) {
      int pos = i * cellWidth;
      int limit = pos + cellWidth;
      entryHashes.limit(limit).position(pos);
      appendRow(entryHashes);
    }
    
    assert preCommitCells.writeableBytes() == 0;
    return newSize;
  }

  
  public long newCellIndex() {
    return snapCellCount;
  }


  public ByteBuffer newCells() {
    if (preCommitCells == null || preCommitCells.writeableBytes() != 0)
      throw new IllegalStateException("invoked before append");
    
    return preCommitCells.readBuffer();
  }


  @Override
  protected ByteBuffer getCells(long index, int count) {
    
    int cellWidth = hashWidth();
    
    if (index < snapCellCount) {
      
      if (index + count <= snapCellCount)
        return super.getCells(index, count);
      
      
      
      // Following corner case should never hit under current code.
      // Handled in case we do.
      // Will drop this warning once excercised for good reason.
      //
      Logger.getLogger(getClass().getSimpleName()).warning(
          "index " + index + ", count " + count + ", snapCellCount " + snapCellCount);
      
      int subCount = (int) (snapCellCount - index);
      ByteBuffer head = super.getCells(index, subCount);
      ByteBuffer tail = preCommitCells.readBuffer().limit((count - subCount) * cellWidth);
      ByteBuffer out = ByteBuffer.allocate(head.remaining() + tail.remaining());
      return out.put(head).put(tail).flip();
      
    } else {
    
      int pos = (int) (index - snapCellCount) * cellWidth;
      int limit = pos + count * cellWidth;
      return preCommitCells.readBuffer().limit(limit).position(pos).slice();
    }
  }





  @Override
  protected void putCells(long index, ByteBuffer cells) {
    if (preCommitCells == null)
      throw new UnsupportedOperationException("appendRows invoked directly (?)");
    
    // we shouldn't be overwriting
    assert index >= snapCellCount;
    
    int cellWidth = hashWidth();
    
    int expectedPosition = (int) (index - snapCellCount) * cellWidth;
    
    if (expectedPosition != preCommitCells.readableBytes())
      throw new ConcurrentModificationException(
          "expected pos " + expectedPosition + "; actual " + preCommitCells.readableBytes());
    
    assert cells.hasRemaining() && cells.remaining() % hashWidth() == 0;
    preCommitCells.put(cells);
  }
  
  
  @Override
  public long cellCount() {
    return snapCellCount + preCommitCells.readableBytes() / hashWidth();
  }
  

}
