/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.util.ledger;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.List;
import java.util.Objects;

import io.crums.util.Lists;
import io.crums.util.hash.Digest;

/**
 * 
 */
public class SkipPath implements Digest {
  
  private final List<Row> path;
  
  
  public SkipPath(List<Row> path) {
    Objects.requireNonNull(path, "null path");
    if (path.isEmpty())
      throw new IllegalArgumentException("empth path");
    
    Row[] rows = new Row[path.size()];
    rows = path.toArray(rows);
    
    this.path = Lists.asReadOnlyList(rows);
    
    MessageDigest digest = newDigest();
    verify(digest);
  }
  
  SkipPath(List<Row> path, MessageDigest digest) {
    this.path = Objects.requireNonNull(path, "null path");
    
    if (path.isEmpty())
      throw new IllegalArgumentException("empty path");
    
    verify(digest);
  }
  
  
  public List<Row> rows() {
    return path;
  }


  private void verify(MessageDigest digest) {
    Objects.requireNonNull(digest, "null digest");
    
    for (int index = 0, nextToLast = path.size() - 2; index < nextToLast; ++index) {
      Row row = path.get(index);
      Row prev = path.get(index + 1);
      
      if (!Digest.equal(row, prev))
        throw new IllegalArgumentException(
            "digest conflict at index " + index + ": " +
                path.subList(index, index + 2));
      
      long rowNumber = row.rowNumber();
      long prevNumber = prev.rowNumber();
      long deltaNum = rowNumber - prevNumber;
      
      if (deltaNum < 1 || rowNumber % deltaNum != 0)
        throw new IllegalArgumentException(
            "row numbers at index " + index + ": " + path.subList(index, index + 2));
      if (Long.highestOneBit(deltaNum) != deltaNum)
        throw new IllegalArgumentException(
            "non-power-of-2 delta " + deltaNum + " at index " + index + ": " +
                path.subList(index, index + 2));
      
      digest.reset();
      digest.update(prev.data());
      ByteBuffer prevRowHash = ByteBuffer.wrap(digest.digest());
      
      int pointerIndex = Long.numberOfTrailingZeros(deltaNum);
      ByteBuffer hashPointer = row.skipPointer(pointerIndex);
      if (!prevRowHash.equals(hashPointer))
        throw new IllegalArgumentException(
            "hash conflict at index " + index + ": " + path.subList(index, index + 2));
    }
  }


  @Override
  public int hashWidth() {
    return path.get(0).hashWidth();
  }


  @Override
  public String hashAlgo() {
    return path.get(0).hashAlgo();
  }

}














