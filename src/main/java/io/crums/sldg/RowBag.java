/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg;


import java.nio.ByteBuffer;

import io.crums.util.hash.Digest;

/**
 * A bag of rows. The general contract is that if a row's
 * {@linkplain #inputHash(long) input hash} is known, then
 * the {@linkplain #rowHash(long) row hash}es the row links
 * to are also known. This interface does not advertise <em>
 * which</em> rows are in the bag: that can be defined elsewhere
 * in multiple ways, so we punt for now.
 */
public interface RowBag extends Digest {
  
  
  /**
   * Returns the 
   * @param rowNumber
   * @return
   */
  ByteBuffer rowHash(long rowNumber);
  
  
  ByteBuffer inputHash(long rowNumber);
  
  
  default Row getRow(long rowNumber) {
    return new BaggedRow(rowNumber, this);
  }

}
