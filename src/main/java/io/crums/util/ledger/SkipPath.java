/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.util.ledger;


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.security.MessageDigest;
import java.util.List;

import io.crums.io.channels.ChannelUtils;
import io.crums.util.hash.Digests;

/**
 * A <tt>LinkedPath</tt> in V-form. The base class also supports
 * this structure. The main value-add here is that when in V-form,
 * a skip path has a more compact serial representation, since then
 * its row numbers can be specified with only 2 numbers (lo and hi).
 */
public class SkipPath extends LinkedPath {
  
  /**
   *  sizeof 2 longs
   */
  private final static int ROW_NUMBER_DATA_LEN = 2 * Long.BYTES;
  
  
  /**
   * Loads an instance from its serial form.
   * 
   * {@link #serialize()}
   */
  public static SkipPath load(ByteBuffer source) {
    long lo = source.getLong();
    long hi = source.getLong();
    return new SkipPath(lo, hi, source);
  }
  
  
  /**
   * Creates a new instance from the given file. Since the data is self-delimiting,
   * any trailing data in the file is ignored.
   * 
   * @see #load(InputStream)
   * {@link #serialize()}
   */
  public static SkipPath load(File file) throws IOException {
    try (FileInputStream in = new FileInputStream(file)) {
      return load(in.getChannel());
    }
  }
  
  
  /**
   * Creates a new instance from serial form. Since the data is self-delimiting,
   * on return the stream is not necessarily at its end.
   * 
   * @see #load(ReadableByteChannel)
   * {@link #serialize()}
   */
  public static SkipPath load(InputStream in) throws IOException {
    return load(ChannelUtils.asChannel(in));
  }
  
  /**
   * Creates a new instance from serial form. Since the data is self-delimiting,
   * on return the stream is not necessarily at its end.
   * 
   * @see #load(InputStream)
   * {@link #serialize()}
   */
  public static SkipPath load(ReadableByteChannel in) throws IOException {
    ByteBuffer nibble = ByteBuffer.allocate(ROW_NUMBER_DATA_LEN);
    ChannelUtils.readRemaining(in, nibble);
    long lo = nibble.flip().getLong();
    long hi = nibble.getLong();
    int remainingBytes = serialByteSize(lo, hi) - ROW_NUMBER_DATA_LEN;
    ByteBuffer skipRows = ByteBuffer.allocate(remainingBytes);
    ChannelUtils.readRemaining(in, skipRows);
    return new SkipPath(lo, hi, skipRows.flip(), false);
  }
  

  
  public static int serialByteSize(long lo, long hi) {
    return serialByteSize(lo, hi, Digests.SHA_256.hashWidth());
  }
  
  public static int serialByteSize(long lo, long hi, int hashWidth) {
    if (hashWidth < 8)
      throw new IllegalArgumentException("hashWidth " + hashWidth + " < 8");
    
    int cells = SkipLedger.skipPathNumbers(lo, hi)
        .stream().mapToInt(rn -> SkipLedger.rowCells(rn)).sum();
    
    return ROW_NUMBER_DATA_LEN + cells * hashWidth;
  }

  /**
   * Creates a new instance. Makes a defensive copy.
   * 
   * @param lo the low row number (inclusive)
   * @param hi the hi row number (inclusive)
   * 
   * @param skipRows concatentation of rows
   */
  public SkipPath(long lo, long hi, ByteBuffer skipRows) {
    super(lo, hi, skipRows);
  }

  /**
   * Package-private constructor.
   * 
   * @param lo the low row number (inclusive)
   * @param hi the hi row number (inclusive)
   * 
   * @param skipRows concatentation of rows
   * @param deepCopy if <tt>true</tt> the data is defensively copied.
   */
  SkipPath(long lo, long hi, ByteBuffer skipRows, boolean deepCopy) {
    super(lo, hi, skipRows, deepCopy);
  }
  
  /**
   * @see LinkedPath#SkipPath(List, MessageDigest)
   */
  SkipPath(List<Row> path, MessageDigest digest) {
    super(path, digest);
    // TODO Auto-generated constructor stub
  }


  /**
   * @return <tt>true</tt>
   */
  @Override
  public boolean isSkipPath() {
    return true;
  }
  
  
  /**
   * Returns a serial (binary) representation of this instance's state.
   * 
   * @see #load(ByteBuffer)
   */
  public ByteBuffer serialize() {
    ByteBuffer out;
    {
      // calculate how big the buffer should be
      int cells = path().stream().mapToInt(r -> SkipLedger.rowCells(r.rowNumber())).sum();
      int bytes = ROW_NUMBER_DATA_LEN + cells * hashWidth();
      out = ByteBuffer.allocate(bytes);
    }
    
    out.putLong(loRowNumber()).putLong(hiRowNumber());
    
    path().forEach(r -> out.put(r.data()));
    
    assert !out.hasRemaining();
    return out.flip();
  }

}
