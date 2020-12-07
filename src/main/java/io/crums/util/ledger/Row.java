/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.util.ledger;


import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Objects;

import io.crums.util.IntegralStrings;
import io.crums.util.hash.Digest;

/**
 * A row in a ledger, dressed up with a row number and methods to access
 * its skip [hash] pointers. Instances are immutable.
 */
public class Row implements Digest {
  

  private final long rowNumber;
  private final ByteBuffer row;
  
  
  /**
   * Constructs a new instance. This does a defensive copy (since we want a runtime
   * reference to be guarantee immutability).
   * 
   * @param rowNumber the row number
   * @param row       the row's backing data (a sequence of cells representing hashes)
   */
  public Row(long rowNumber, ByteBuffer row) {
    this(rowNumber, ByteBuffer.allocate(row.remaining()).put(row).flip(), false);
  }
  
  
  
  /**
   * Package-private shallow data copy constructor.
   * 
   * @param row unless unsliced, caller must not mess with positional info; if unsliced
   *    (i.e. remaining &ne; capacity), then the constructor slices.
   */
  Row(long rowNumber, ByteBuffer row, boolean ignored) {
    this.rowNumber = rowNumber; // (bounds checked below)
    int remaining = Objects.requireNonNull(row, "null row").remaining();
    if (remaining != row.capacity())
      row = row.slice();
    this.row = row;
    
    int cellsInRow = 1 + SkipLedger.skipCount(rowNumber); // (bounds checked)
    int expectedBytes = cellsInRow * hashWidth();
    if (remaining != expectedBytes)
      throw new IllegalArgumentException(
          "expected " + expectedBytes + " bytes for rowNumber " + rowNumber + "; actual given is " + row);
    
  }
  
  
  /**
   * Returns the ledger entry in raw form. This contains 1 +
   * {@linkplain SkipLedger#skipCount(long) skipCount(rowNumber())} many hash cells, each cell
   * {@linkplain #hashWidth()} many bytes wide.
   * 
   * @return read-only [shallow] copy
   */
  public final ByteBuffer data() {
    return row.asReadOnlyBuffer();
  }
  
  
  /**
   * Returns this row's hash.
   * 
   * @return the hash the {@linkplain data}
   */
  public final ByteBuffer rowHash() {
    MessageDigest digest = newDigest();
    digest.reset();
    digest.update(data());
    return ByteBuffer.wrap(digest.digest());
  }
  
  
  
  /**
   * Returns the row number. Note this value does not figure in instance equality.
   * 
   * @see #equals(Object)
   */
  public final long rowNumber() {
    return rowNumber;
  }
  
  
  /**
   * Returns the entry (user-inputed) hash. This is the hash of the abstract object (whatever it is, we
   * don't know).
   * 
   * @return non-null, {@linkplain #hashWidth()} bytes remaining, positioned at zero (but not sliced)
   */
  public final ByteBuffer entryHash() {
    return data().limit(hashWidth());
  }
  
  
  /**
   * Returns the number of hash pointers in this row referencing previous rows.
   * 
   * @return &ge; 1
   */
  public final int skipCount() {
    return SkipLedger.skipCount(rowNumber);
  }
  
  
  /**
   * Returns the hash of the row reference by the skip pointer at the given pointer-level.
   * 
   * @param pointerLevel &ge; 0 and &lt; {@linkplain #skipCount()}
   * 
   * @return non-null, {@linkplain #hashWidth()} bytes wide
   */
  public final ByteBuffer skipPointer(int pointerLevel) {
    Objects.checkIndex(pointerLevel, skipCount());
    int cellWidth = hashWidth();
    int pos = (1 + pointerLevel) * cellWidth;
    int limit = pos + cellWidth;
    return data().position(pos).limit(limit).slice();
  }
  
  
  /**
   * Returns the row number referenced by the skip pointer at the given pointer-level.
   * 
   * @param pointerLevel &ge; 0 and &lt; {@linkplain #skipCount()}
   * 
   * @return &ge; 0
   */
  public final long skipRowNumber(int pointerLevel) {
    Objects.checkIndex(pointerLevel, skipCount());
    return rowNumber - (1L << pointerLevel);
  }
  
  
  
  
  public int hashWidth() {
    return Constants.DEF_DIGEST.hashWidth();
  }
  
  
  
  public String hashAlgo() {
    return Constants.DEF_DIGEST.hashAlgo();
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
      // estimate req builder cap
      int cellCount = skipCount + 1;
      int len = 18 /* (row number + space) */ + cellCount * CELL_DISPLAY_LENGTH;
      string = new StringBuilder(len);
    }
    
    string.append('[').append(rowNumber).append("] ");
    while(string.length() < 8)
      string.append(' ');
    
    
    appendCellToString(entryHash(), string);
    
    for (int p = 0; p < skipCount; ++p)
      appendCellToString(skipPointer(p), string);
    
    return string.toString();
  }
  
  private final static int HEX_DISPLAY_BYTES = 3;
  private final static String CELL_DISPLAY_PREFIX = "  ";
  private final static String CELL_DISPLAY_POSTFIX = "..";
  private final static int CELL_DISPLAY_LENGTH =
      2 * HEX_DISPLAY_BYTES + CELL_DISPLAY_PREFIX.length() + CELL_DISPLAY_POSTFIX.length();
  
  private void appendCellToString(ByteBuffer cell, StringBuilder string) {
    string.append(CELL_DISPLAY_PREFIX);
    cell.limit(cell.position() + HEX_DISPLAY_BYTES);
    IntegralStrings.appendHex(cell, string);
    string.append(CELL_DISPLAY_POSTFIX);
  }

}
