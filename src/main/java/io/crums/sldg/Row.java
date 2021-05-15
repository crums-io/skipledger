/*
 * Copyright 2020-2021 Babak Farhang
 */
package io.crums.sldg;


import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.Objects;
import java.util.SortedSet;

import io.crums.util.IntegralStrings;
import io.crums.util.hash.Digest;

/**
 * A row in a ledger. Instances have immutable state.
 */
public abstract class Row implements Digest {
  
  
  /**
   * Concrete (base) implementations defined only in this package.
   */
  Row() {  }
  
  
  /**
   * Returns the row number.
   * 
   * @return &ge; 1
   * 
   * @see #equals(Object)
   */
  public abstract long rowNumber();
  

  /**
   * Returns the user-inputed hash. This is the hash of the abstract object (whatever it is, we
   * don't know).
   * 
   * @return non-null, {@linkplain #hashWidth()} bytes remaining
   */
  public abstract ByteBuffer inputHash();
  

  /**
   * Returns the byte string used to compute the {@linkplain #hash() hash} of this row. This
   * contains 1 + {@linkplain SkipLedger#skipCount(long) skipCount(rowNumber())} many hash cells, each cell
   * {@linkplain #hashWidth()} many bytes wide.
   * <p>
   * <em>Note this has nothing to do with how an instance is serialized for storage or
   * over-the-wire; it's about what data contributes a row's hash.</em>
   * </p>
   * 
   * @return may be read-only
   */
  public ByteBuffer data() {
    int levels = prevLevels();
    ByteBuffer serial = ByteBuffer.allocate((1 + levels) * hashWidth());
    serial.put(inputHash());
    for (int i = 0; i < levels; ++i)
      serial.put(prevHash(i));
    return serial.flip();
  }
  
  /**
   * Returns the hash of this row.
   * 
   * @return may be read-only, {@linkplain #hashWidth()} bytes remaining
   * 
   * @see #data()
   */
  public ByteBuffer hash() {
    MessageDigest digest = newDigest();
    digest.reset();
    
    digest.update(inputHash());
    
    for (int level = 0, levels = prevLevels(); level < levels; ++level)
      digest.update(prevHash(level));

    return ByteBuffer.wrap(digest.digest());
  }
  
  
  /**
   * Returns the hash of the given row number. An instance knows the hash of not
   * just itself, but also of the rows it references thru its skip pointers.
   * 
   * @see #coveredRowNumbers()
   */
  public final ByteBuffer hash(long rowNumber) {
    final long rn = rowNumber();
    final long diff = rn - rowNumber;
    if (diff == 0)
      return hash();
    
    int referencedRows = prevLevels();
    if (diff < 0 || Long.highestOneBit(diff) != diff || diff > (1L << (referencedRows - 1)))
      throw new IllegalArgumentException(
          "rowNumber " + rowNumber + " is not covered by this row " + this);
    int ptrLevel = 63 - Long.numberOfLeadingZeros(diff);
    return prevHash(ptrLevel);
  }
  
  
  /**
   * Returns the set of row numbers covered by this row. This includes the row's
   * own row number, as well as those referenced thru its hash pointers. The hashes
   * of these referenced rows is available thru the {@linkplain #hash(long)} method.
   */
  public final SortedSet<Long> coveredRowNumbers() {
    return SkipLedger.coverage(Collections.singletonList(rowNumber()));
  }
  

  /**
   * Returns the hash of the row referenced at the given level.
   * 
   * @param level &ge; 0 and &lt; {@linkplain #prevLevels()}
   * 
   * @return non-null, {@linkplain #hashWidth()} bytes wide
   */
  public abstract ByteBuffer prevHash(int level);
  

  /**
   * Returns the number of hash pointers in this row referencing previous rows.
   * These are called levels, because each successive {@linkplain #prevHash(int) previous hash}
   * points to a row numbered twice as far away as the level before it.
   * 
   * @return &ge; 1
   */
  public final int prevLevels() {
    return SkipLedger.skipCount(rowNumber());
  }
  
  /**
   * Returns the row number linked to at the given {@code level}.
   * 
   * @param level &ge; 0 and &lt; {@linkplain #prevLevels()}
   * 
   * @return {@code rowNumber() - (1L << level)}
   * @see #hash(long)
   */
  public final long prevRowNumber(int level) {
    long rn = rowNumber();
    Objects.checkIndex(level, SkipLedger.skipCount(rn));
    return rn - (1L << level);
  }
  

  /**
   * Equality semantics depend on {@linkplain #hash() hash}, and
   * {@linkplain #rowNumber() row number}.
   * 
   * @see #hashCode()
   */
  @Override
  public final boolean equals(Object o) {
    if (o == this)
      return true;
    else if (o instanceof Row) {
      Row other = (Row) o;
      return other.rowNumber() == rowNumber() && other.hash().equals(hash());
    } else
      return false;
  }
  

  /**
   * Consistent with {@linkplain #equals(Object)}. Implemented for the Java
   * Collections classes.
   */
  @Override
  public final int hashCode() {
    return Long.hashCode(rowNumber());
  }
  
  
  @Override
  public String toString() {
    int skipCount = prevLevels();

    StringBuilder string;
    {
      // estimate req builder cap
      int cellCount = skipCount + 1;
      int len = 18 /* (row number + space) */ + cellCount * CELL_DISPLAY_LENGTH;
      string = new StringBuilder(len);
    }
    
    string.append('[').append(rowNumber()).append("] ");
    while(string.length() < 8)
      string.append(' ');
    
    appendCellToString(inputHash(), string);

    for (int p = 0; p < skipCount; ++p)
      appendCellToString(prevHash(p), string);
    
    return string.toString();
  }
  
  
  
  private void appendCellToString(ByteBuffer cell, StringBuilder string) {
    string.append(CELL_DISPLAY_PREFIX);
    cell.limit(cell.position() + HEX_DISPLAY_BYTES);
    IntegralStrings.appendHex(cell, string);
    string.append(CELL_DISPLAY_POSTFIX);
  }
  
  


  private final static int HEX_DISPLAY_BYTES = 3;
  private final static String CELL_DISPLAY_PREFIX = "  ";
  private final static String CELL_DISPLAY_POSTFIX = "..";
  private final static int CELL_DISPLAY_LENGTH =
      2 * HEX_DISPLAY_BYTES + CELL_DISPLAY_PREFIX.length() + CELL_DISPLAY_POSTFIX.length();

}


