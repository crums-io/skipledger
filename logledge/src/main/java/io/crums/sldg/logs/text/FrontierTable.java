/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.logs.text;

import java.nio.ByteBuffer;

import io.crums.sldg.SldgConstants;
import io.crums.sldg.cache.HashFrontier;

/**
 * Backing table for storing frontier row hashes. Note, not all rows'
 * hashes need be saved: if typically every 2<sup>n</sup>-th row is saved,
 * then one can construct a {@linkplain HashFrontier} instance for any
 * row recorded in the table.
 */
public interface FrontierTable extends AutoCloseable {
  
  
  public final static int HASH_WIDTH = SldgConstants.DIGEST.hashWidth();
  
  
  /** Closes the instance. No checked exceptions. */
  void close();

  /** Returns the number of rows in the table. */
  long size();
  
  /**
   * Appends the given hash.
   * 
   * @param hash 32-bytes remaining
   */
  void append(ByteBuffer hash);

  /**
   * Returns the hash at the given <em>zero-based</em> index.
   * 
   * @param index zero-based: 0 &le; {@code index} &lt; {@code size()}
   * @param out  out buffer with capacity &ge; 32
   * 
   * @return {@code out} buffer positioned at zero, with 32-bytes remaining
   */
  ByteBuffer get(long index, ByteBuffer out);
  
  /**
   * Returns the hash at the given <em>zero-based</em> index. This is just shorthand
   * for {@code get(index, ByteBuffer.allocate(32))}.
   * 
   * @param index zero-based: 0 &le; {@code index} &lt; {@code size()}
   * @return buffer with 32-bytes remaining
   * @see #get(long, ByteBuffer)
   */
  default ByteBuffer get(long index) {
    return get(index, ByteBuffer.allocate(32));
  }
  
  /**  */
  /**
   * Trims the number of entries in the table to the specified amount.
   * If the current size is less than the given amount, then no change occurs.
   * 
   * @param newSize &ge; 0
   */
  void trimSize(long newSize);
  

}
