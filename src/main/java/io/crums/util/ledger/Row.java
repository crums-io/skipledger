/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.util.ledger;


import java.nio.ByteBuffer;
import java.util.Objects;

import io.crums.util.IntegralStrings;
import io.crums.util.hash.Digest;

/**
 * 
 */
public class Row implements Digest {

  private final long rowNumber;
  private final ByteBuffer row;
  
  
  public Row(long rowNumber, ByteBuffer row) {
    this.rowNumber = rowNumber;
    Objects.requireNonNull(row, "null row");
    if (row.remaining() != row.capacity())
      row = row.slice();
    this.row = row;
    
    int cellsInRow = 1 + SkipLedger.skipCount(rowNumber);
    int expectedBytes = cellsInRow * hashWidth();
    if (this.row.remaining() != expectedBytes)
      throw new IllegalArgumentException(
          "expected " + expectedBytes + " bytes for rowNumber " + rowNumber + "; actual given is " + row);
  }
  
  
  public final ByteBuffer data() {
    return row.asReadOnlyBuffer();
  }
  
  public final long rowNumber() {
    return rowNumber;
  }
  
  public final ByteBuffer entryHash() {
    return data().limit(hashWidth());
  }
  
  
  public final int skipCount() {
    return SkipLedger.skipCount(rowNumber);
  }
  
  
  public final ByteBuffer skipPointer(int pointerLevel) {
    Objects.checkIndex(pointerLevel, skipCount());
    int cellWidth = hashWidth();
    int pos = (1 + pointerLevel) * cellWidth;
    int limit = pos + cellWidth;
    return data().position(pos).limit(limit).slice();
  }
  
  
  public final long skipRowNumber(int pointerLevel) {
    Objects.checkIndex(pointerLevel, skipCount());
    return rowNumber - (1L << pointerLevel);
  }
  
  
//  public ByteBuffer hash(MessageDigest digest) {
//    if (digest.getDigestLength() != hashWidth())
//      throw new IllegalArgumentException("digest mismatch: " + digest);
//    digest.reset();
//    digest.update(row.asReadOnlyBuffer());
//    return ByteBuffer.wrap(digest.digest());
//  }
  
  
  
  public int hashWidth() {
    return SkipLedger.SHA256_WIDTH;
  }
  
  
  
  public String hashAlgo() {
    return SkipLedger.SHA256_ALGO;
  }
  
  
  
  /**
   * Equality semantics only depend on {@linkplain #data() data}, not on
   * {@linkplain #rowNumber() row number}.
   * 
   * @see #hashCode()
   */
  @Override
  public final boolean equals(Object o) {
    return o == this || (o instanceof Row) && ((Row) o).row.equals(row);
  }
  

  /**
   * Consistent with {@linkplain #equals(Object)}. Implemented for the Java
   * Collections classes.
   */
  @Override
  public final int hashCode() {
    return row.hashCode();
  }
  
  
  @Override
  public String toString() {
    int skipCount = skipCount();
    
    StringBuilder string;
    {
      int cellCount = skipCount + 1;
      int len = 18 + cellCount * (hashWidth() * 2 + 1);
      string = new StringBuilder(len);
    }
    
    string.append(rowNumber).append(' ');
    
    appendCellToString(entryHash(), string);
    
    for (int p = 0; p < skipCount; ++p)
      appendCellToString(skipPointer(p), string);
    
    return string.toString();
  }
  
  private void appendCellToString(ByteBuffer cell, StringBuilder string) {
    int displayBytes = 5;
    string.append("  ");
    cell.limit(cell.position() + displayBytes);
    IntegralStrings.appendHex(cell, string);
    string.append("..");
  }

}
