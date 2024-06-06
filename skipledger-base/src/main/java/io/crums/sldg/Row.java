/*
 * Copyright 2020-2021 Babak Farhang
 */
package io.crums.sldg;


import static io.crums.sldg.SldgConstants.DIGEST;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;

import io.crums.util.IntegralStrings;
import io.crums.util.Lists;
import io.crums.util.mrkl.FixedLeafBuilder;

/**
 * A row in a ledger.
 * Concrete instances must have immutable state.
 * 
 * @see RowHash#prevLevels()
 * @see RowHash#prevRowNumber(int)
 */
public abstract class Row extends RowHash {
  
  
  
  

  /**
   * Returns the user-inputed hash. This is the hash of the abstract object (whatever it is, we
   * don't know).
   * 
   * @return non-null, {@linkplain #hashWidth()} bytes remaining
   */
  public abstract ByteBuffer inputHash();
  

  
  
  /**
   * {@inheritDoc}
   * 
   * @return may be read-only, 32-bytes remaining
   * 
   */
  public ByteBuffer hash() {

    return SkipLedger.rowHash(
      no(),
      inputHash(),
      prevHashes());
  }


  final List<ByteBuffer> prevHashes() {
    return Lists.functorList(
        prevLevels(),
        this::prevHash);
  }




  public boolean hasAllLevels() {
    return SkipLedger.alwaysAllLevels(no()) || hasAllPtrs();
  }


  public final boolean isCondensed() {
    return !hasAllLevels();
  }



  public boolean hasAllPtrs() {
    return true;
  }
  
  
  /**
   * Returns the hash of the given row number. An instance knows the hash of not
   * just itself, but also of the rows it references thru its skip pointers.
   * 
   * @see #coveredRowNumbers()
   */
  public final ByteBuffer hash(long rowNumber) {
    final long rn = no();
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
    return SkipLedger.coverage(Collections.singletonList(no()));
  }
  

  /**
   * Returns the hash of the row referenced at the given level.
   * 
   * @param level &ge; 0 and &lt; {@linkplain #prevLevels()}
   * 
   * @return non-null, {@linkplain #hashWidth()} bytes wide
   */
  public abstract ByteBuffer prevHash(int level);
  
  
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
    
    string.append('[').append(no()).append("] ");
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


