/*
 * Copyright 2024 Babak Farhang
 */
package io.crums.sldg;

import java.nio.ByteBuffer;
import java.util.Collections;

import io.crums.io.buffer.BufferUtils;

/**
 * Speeds up look-ups in a {@code PathPack} by
 * memo-ising the hashes of full row numbers.
 */
public class MemoPathPack extends PathPack {

  private ByteBuffer memoedHashes;

  /**
   * Constructs a memo-ised version of the given pack.
   * 
   * @param promo
   */
  public MemoPathPack(PathPack promo) {
    super(promo);
    var inputRns = getFullRowNumbers();
    if (inputRns.isEmpty()) {
      this.memoedHashes = BufferUtils.NULL_BUFFER;
      return;
    }
    this.memoedHashes = ByteBuffer.allocate(inputRns.size());
    
    var view = memoedHashes.slice();
    final int count = inputRns.size();
    // compute the hash of each memo-ed row
    // using a BaggedRow.hash() taking *this*
    // instance as the RowBag. It doesn't run
    // into to trouble since we write it from
    // the bottom no.s up: the BaggedRow never
    // accesses what's not already written as
    // we build it.
    for (int index = 0; index < count; ++index) {
      long rn = inputRns.get(index);
      var hash = new BaggedRow(rn, this).hash();
      view.put(hash);
    }
    assert !view.hasRemaining();
  }
  
  
  
  @Override
  public Row getRow(long rowNumber) {

    int index = indexOfExistingFullRow(rowNumber);
    return new MemoRow(rowNumber, index);
  }
  
  
  
  private int indexOfExistingFullRow(long rn) {
    int index = Collections.binarySearch(
        getFullRowNumbers(), rn);
    if (index < 0)
      throw new IllegalArgumentException(
          "row [" + rn + "] is not in pack");
    return index;
  }
  
  
  
  @Override
  public ByteBuffer rowHash(long rowNumber) {
    if (rowNumber <= 0) {
      if (rowNumber == 0)
        return SldgConstants.DIGEST.sentinelHash();
      
      throw new IllegalArgumentException(
          "negative row number: " + rowNumber);
    }
    
    ByteBuffer refOnlyHash = refOnlyHash(rowNumber);
    if (refOnlyHash != null)
      return refOnlyHash;
    
    return getMemoedRowHash(rowNumber);
  }





  private ByteBuffer getMemoedRowHash(long rn) {
    int index = indexOfExistingFullRow(rn);
    return memoedHash(index);
  }
  
  
  private ByteBuffer memoedHash(int index) {
    return emit(memoedHashes, index);
  }
  
  
  private class MemoRow extends BaggedRow {
    
    private final int index;

    MemoRow(long rn, int index) {
      super(rn, MemoPathPack.this);
      this.index = index;
    }

    @Override
    public ByteBuffer hash() {
      return memoedHash(index);
    }
    
  }

}
