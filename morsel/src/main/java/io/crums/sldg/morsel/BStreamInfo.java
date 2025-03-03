/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.morsel;

import java.net.URI;

/**
 * Meta info about a {@linkplain LedgerType#BSTREAM BSTREAM} type ledger.
 * 
 * @see #blockSize()
 * @see LedgerInfo#type()
 */
public class BStreamInfo extends LedgerInfo {
  
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
    super(LedgerType.BSTREAM, alias, uri, desc);
    this.blockSize = blockSize;
    
    if (blockSize <= 0)
      throw new IllegalArgumentException("blockSize " + blockSize);
  }
  
  
  /** Returns the block-size (in bytes). */
  public int blockSize() {
    return blockSize;
  }

}




