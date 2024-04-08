/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.cache;


import static io.crums.sldg.SldgConstants.HASH_WIDTH;

import java.nio.ByteBuffer;

import io.crums.sldg.RowHash;

/**
 * A numbered row and its hash (computed from the row's parts).
 * 
 * <h2>Immutable (Almost)</h2>
 * <p>
 * Instances do not modify state. However, since instances do not copy arguments
 * at construction, an instance's state may conceivably be modified from the outside.
 * (We don't worry about such programming errors here since these tend to be discovered
 * early in other self-validating structures.)
 * </p>
 */
public class HashedRow extends RowHash {
  
  private final long rowNumber;
  private final ByteBuffer hash;
  
  
  /**
   * 
   * @param rowNumber
   * @param hash      32 bytes. Not copied. (Do not modify!)
   */
  public HashedRow(long rowNumber, byte[] hash) {
    this.rowNumber = rowNumber;
    this.hash = ByteBuffer.wrap(hash);
    checkArgs();
  }
  

  /**
   * 
   * @param rowNumber 
   * @param hash      32 remaining bytes remaining. Sliced, not copied. (Do not modify the contents
   *                  of the remaining bytes!)
   */
  public HashedRow(long rowNumber, ByteBuffer hash) {
    this.rowNumber = rowNumber;
    
    this.hash = hash.slice();
    checkArgs();
  }
  
  
  private void checkArgs() {
    if (rowNumber < 1)
      throw new IllegalArgumentException(
          "rowNumber " + rowNumber);
    if (hash.remaining() != HASH_WIDTH)
      throw new IllegalArgumentException(
          "illegal remaining bytes in hash buffer: " + hash);
  }
  
  
  

  @Override
  public long rowNumber() {
    return rowNumber;
  }

  @Override
  public ByteBuffer hash() {
    return hash.asReadOnlyBuffer();
  }

}
