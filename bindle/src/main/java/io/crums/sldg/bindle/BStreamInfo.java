/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.bindle;

import java.net.URI;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import io.crums.io.SerialFormatException;

/**
 * Meta info about a {@linkplain LedgerType#BSTREAM BSTREAM} type ledger.
 * 
 * @see #blockSize()
 * @see LedgerInfo#type()
 */
public final class BStreamInfo extends LedgerInfo {
  
  private final int blockSize;

  
  /**
   * Constructs an instance with the specified block-size.
   * 
   * @param alias       locally unique name (trimmed)
   * @param uri         optional (may be {@code null})
   * @param desc        optional description ({@code null} or blank counts
   *                    for naught)
   * @param blockSize   byte-size of each block (row)
   */
  public BStreamInfo(String alias, URI uri, String desc, int blockSize) {
    this(new StdProps(LedgerType.BSTREAM, alias, uri, desc), blockSize);
  }
  
  
  public BStreamInfo(StdProps props, int blockSize) {
    super(props);
    this.blockSize = blockSize;
    
    if (blockSize <= 0)
      throw new IllegalArgumentException("blockSize " + blockSize);
  }
  
  
  @Override
  LedgerInfo edit(StdProps props) {
    return new BStreamInfo(props, blockSize);
  }


  /** Returns the block-size (in bytes). */
  public int blockSize() {
    return blockSize;
  }
  
  @Override
  protected Object otherProperties() {
    return blockSize;
  }


  @Override
  public int serialSize() {
    return 4 + props.serialSize();
  }


  @Override
  public ByteBuffer writeTo(ByteBuffer out) throws BufferOverflowException {
    return props.writeTo(out).putInt(blockSize);
  }
  
  
  
  static BStreamInfo load(StdProps props, ByteBuffer in) {
    int blockSize = in.getInt();
    if (blockSize <= 0)
      throw new SerialFormatException(
          "read blockSize %d at offset %d in %s"
          .formatted(blockSize, in.position() - 4, in));
    return new BStreamInfo(props, blockSize);
  }

}






















