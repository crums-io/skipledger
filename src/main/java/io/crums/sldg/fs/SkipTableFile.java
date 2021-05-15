/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg.fs;


import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ConcurrentModificationException;

import io.crums.io.Opening;
import io.crums.io.channels.ChannelUtils;
import io.crums.sldg.SkipTable;

/**
 * File-based {@linkplain SkipTable} implementation.
 */
public class SkipTableFile implements SkipTable {
  
  
  
  private final FileChannel file;

  /**
   * 
   */
  public SkipTableFile(File file) throws IOException {
    this(file, Opening.CREATE_ON_DEMAND);
  }

  /**
   * 
   */
  public SkipTableFile(File file, Opening mode) throws IOException {
    
    this.file = mode.openChannel(file);
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
  public long addRows(ByteBuffer rows, long index) {
    
    final int rowCount = rows.remaining() / ROW_WIDTH;
    
    try {
      long size = file.size() / ROW_WIDTH;
      if (index != size)
        throw new ConcurrentModificationException("on index " + index);
      
      assert rowCount != 0 && rows.remaining() % ROW_WIDTH == 0;
      
      long offset = index * ROW_WIDTH;
      ChannelUtils.writeRemaining(file, offset, rows);
      
      return size + rowCount;
    } catch (IOException iox) {
      throw new UncheckedIOException(
          "on appendRows: index " + index + "; input count " + rowCount, iox);
    }
  }

  
  
  
  @Override
  public ByteBuffer readRow(long index) {
    
    if (index < 0)
      throw new IllegalArgumentException("index " + index);
    
    try {
      {
        long size = file.size() / ROW_WIDTH;
        if (index >= size)
          throw new IllegalArgumentException(
              "index " + index + " >= size " + size);
      }
      
      long offset = index * ROW_WIDTH;
      ByteBuffer data = ByteBuffer.allocate(ROW_WIDTH);
      return ChannelUtils.readRemaining(file, offset, data).flip();
      
    } catch (IOException iox) {
      throw new UncheckedIOException("on readRow(" + index + ")", iox);
    }
  }

  @Override
  public long size() {
    try {
      return file.size() / ROW_WIDTH;
    } catch (IOException iox) {
      throw new UncheckedIOException("on size() " + this, iox);
    }
  }
  

  
  
  @Override
  public void trimSize(long newSize) {
    try {
      file.truncate(newSize * ROW_WIDTH);
    } catch (IOException iox) {
      throw new UncheckedIOException("on trimSize(" + newSize + ") " + this, iox);
    }
  }

  public void commit() throws UncheckedIOException {
    try {
      file.force(false);
    } catch (IOException iox) {
      throw new UncheckedIOException("on commit: " + this, iox);
    }
  }

}
