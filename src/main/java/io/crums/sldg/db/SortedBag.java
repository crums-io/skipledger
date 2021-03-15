/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.db;


import static io.crums.sldg.SldgConstants.*;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import io.crums.io.buffer.BufferUtils;
import io.crums.sldg.RowBag;

/**
 * 
 */
public class SortedBag implements RowBag {
  
  
  private final List<Long> hashRns;
  private final List<Long> inputRns;
  
  private final ByteBuffer hashes;
  private final ByteBuffer inputs;
  
  
  
  
  
  public SortedBag(
      List<Long> hashRns, List<Long> inputRns,
      ByteBuffer hashes, ByteBuffer inputs) {
    
    this.hashRns = Objects.requireNonNull(hashRns, "null hashRns");
    this.inputRns = Objects.requireNonNull(inputRns, "null inputRns");
    this.hashes = BufferUtils.readOnlySlice(Objects.requireNonNull(hashes, "null hashes"));
    this.inputs = BufferUtils.readOnlySlice(Objects.requireNonNull(inputs, "null inputs"));
    
    if (inputRns.isEmpty())
      throw new IllegalArgumentException("inputRns is empty");
    
    if (hashRns.size() * HASH_WIDTH != hashes.remaining())
      throw new IllegalArgumentException(
          "hash row number / buffer mismatch:\n" + hashRns + "\n" + hashes);

    if (inputRns.size() * HASH_WIDTH != inputs.remaining())
      throw new IllegalArgumentException(
          "input row number / buffer mismatch:\n" + inputRns + "\n" + inputs);
  }
  
  
  /**
   * Copy constructor for subclasses. Not public cuz no reason to: instances are stateless.
   * @param copy non-null
   */
  protected SortedBag(SortedBag copy) {
    this.hashRns = Objects.requireNonNull(copy, "null copy").hashRns;
    this.inputRns = copy.inputRns;
    this.hashes = copy.hashes;
    this.inputs = copy.inputs;
  }
  


  @Override
  public ByteBuffer rowHash(long rowNumber) {
    if (rowNumber <= 0) {
      if (rowNumber == 0)
        return sentinelHash();
      throw new IllegalArgumentException("negative rowNumber: " + rowNumber);
    }
    int index = Collections.binarySearch(hashRns, rowNumber);
    if (index < 0) {
      try {
        return getRow(rowNumber).hash();
      
      } catch (IllegalArgumentException internal) {
        // internal may be from a lower rowNumber (and in fact may cascade)
        throw new IllegalArgumentException("no data for rowNumber " + rowNumber, internal);
      }
    } else
      return emit(hashes, index);
  }

  @Override
  public ByteBuffer inputHash(long rowNumber) {
    if (rowNumber <= 0)
      throw new IllegalArgumentException("rowNumber: " + rowNumber);

    int index = Collections.binarySearch(inputRns, rowNumber);
    if (index < 0)
      throw new IllegalArgumentException("no data for rowNumber " + rowNumber);
    
    return emit(inputs, index);
  }
  
  
  
  private ByteBuffer emit(ByteBuffer store, int index) {
    int offset = index * HASH_WIDTH;
    return store.asReadOnlyBuffer().position(offset).limit(offset + HASH_WIDTH).slice();
  }
  
  
  
  
  
  
  
  // - -  S T A T E L E S S  - -

  @Override
  public final int hashWidth() {
    return HASH_WIDTH;
  }

  @Override
  public final String hashAlgo() {
    return DIGEST.hashAlgo();
  }

  @Override
  public final ByteBuffer sentinelHash() {
    return DIGEST.sentinelHash();
  }

}



