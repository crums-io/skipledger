/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.util.ledger;


import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import io.crums.io.channels.ChannelUtils;

/**
 * 
 */
public class DirectLedger extends SkipLedger implements Closeable {
  
  public final static String FILE_EXT = ".sldg";
  
  
  
  private final FileChannel file;
  

  // nordicanalyticaws@gmail.com / Nordic420#1
  
  /**
   * 
   */
  @SuppressWarnings("resource")
  public DirectLedger(File file) throws UncheckedIOException {
    try {
      
      if (!file.exists())
        file.createNewFile();
      
      this.file = new RandomAccessFile(file, "rw").getChannel();
    
    } catch (IOException iox) {
      throw new UncheckedIOException("on attempting " + file, iox);
    }
  }

  @Override
  public long size() throws UncheckedIOException {
    
    long cells;
    
    try {
      cells = cellCount();
    } catch (IOException iox) {
      throw new UncheckedIOException("on size()", iox);
    }
    
    return maxRows(cells);
  }

  @Override
  protected ByteBuffer getCells(long index, int count) throws UncheckedIOException {
    int cellSize = hashWidth();
    long offset = index * cellSize;
    ByteBuffer cells = ByteBuffer.allocate(cellSize * count);
    try {
      ChannelUtils.readRemaining(file, offset, cells);
    } catch (IOException iox) {
      throw new UncheckedIOException("on getCells(" + index + "," + count + ")", iox);
    }
    return cells.flip();
  }

  @Override
  protected void putCells(long index, ByteBuffer cells) throws UncheckedIOException {
    try {
      long offset = index * hashWidth();
      ChannelUtils.writeRemaining(file, offset, cells);
    } catch (IOException iox) {
      throw new UncheckedIOException("on putCells(" + index + "," + cells + ")", iox);
    }
  }

  @Override
  public void close() throws UncheckedIOException {
    try {
      file.close();
    } catch (IOException iox) {
      throw new UncheckedIOException("on closing " + this, iox);
    }
  }
  
  
  
  
  private long cellCount() throws IOException {
    return file.size() / hashWidth();
  }

}
