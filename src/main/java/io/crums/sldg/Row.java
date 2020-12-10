/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg;


import java.nio.ByteBuffer;
import java.security.MessageDigest;

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
   * Returns the row number. Note this value does not figure in instance equality.
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
   * Returns the serial binary representation of the row whose {@linkplain #hash() hash} is
   * computed by hashing <em>this</em> string. This contains 1 +
   * {@linkplain Ledger#skipCount(long) skipCount(rowNumber())} many hash cells, each cell
   * {@linkplain #hashWidth()} many bytes wide.
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
    return Ledger.skipCount(rowNumber());
  }
  

  /**
   * Equality semantics only depend on {@linkplain #data() data}, not on
   * {@linkplain #rowNumber() row number}.
   * 
   * @see #hashCode()
   */
  @Override
  public final boolean equals(Object o) {
    return o == this || (o instanceof Row) && ((Row) o).data().equals(data());
  }
  

  /**
   * Consistent with {@linkplain #equals(Object)}. Implemented for the Java
   * Collections classes.
   */
  @Override
  public final int hashCode() {
    return inputHash().hashCode();
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





























