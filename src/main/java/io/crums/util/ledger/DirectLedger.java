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
  private final boolean readOnly;
  
  

  // 
  

  public DirectLedger(File file) throws UncheckedIOException {
    this(file, false);
  }
  
  
  /**
   * Create a new 
   * @param file
   * @param readOnly
   * @throws UncheckedIOException
   */
  @SuppressWarnings("resource")
  public DirectLedger(File file, boolean readOnly) throws UncheckedIOException {
    try {
      
      if (!file.exists())
        file.createNewFile();
      
      String mode = readOnly ? "r" : "rw";
      this.file = new RandomAccessFile(file, mode).getChannel();
      this.readOnly = readOnly;
    
    } catch (IOException iox) {
      throw new UncheckedIOException("on attempting " + file, iox);
    }
  }
  
  
  /**
   * Determines whether the ledger was opened in read-only mode.
   */
  public boolean isReadOnly() {
    return readOnly;
  }
  
  
  public void commit() throws UncheckedIOException {
    try {
      file.force(false);
    } catch (IOException iox) {
      throw new UncheckedIOException("on commit: " + this, iox);
    }
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
//      file.force(false);
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
  
  
  
  @Override
  public long cellCount() throws UncheckedIOException {
    try {
      return file.size() / hashWidth();
    } catch (IOException iox) {
      throw new UncheckedIOException("on cellCount()", iox);
    }
  }

}
