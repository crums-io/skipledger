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
  

  // /**
  //  * Returns the byte string used to compute the {@linkplain #hash() hash} of this row. This
  //  * contains 1 + {@linkplain SkipLedger#skipCount(long) skipCount(rowNumber())} many hash cells, each cell
  //  * {@linkplain #hashWidth()} many bytes wide.
  //  * <p>
  //  * <em>Note this has nothing to do with how an instance is serialized for storage or
  //  * over-the-wire; it's about what data contributes a row's hash.</em>
  //  * </p>
  //  * 
  //  * @return may be read-only
  //  */
  // public ByteBuffer data() {
  //   int levels = prevLevels();
  //   ByteBuffer serial = ByteBuffer.allocate((1 + levels) * hashWidth());
  //   serial.put(inputHash());
  //   for (int i = 0; i < levels; ++i)
  //     serial.put(prevHash(i));
  //   return serial.flip();
  // }
  
  /**
   * {@inheritDoc}
   * 
   * @return may be read-only, 32-bytes remaining
   * 
   */
  public ByteBuffer hash() {

    // MessageDigest digest = SldgConstants.DIGEST.newDigest();
    
    // digest.update(inputHash());

    // // final int levels = prevLevels();

    // digest.update(hiPtrHash());

    // var basePtrs = basePtrsHash();

    // if (basePtrs != null)
    //   digest.update(basePtrs);
    
    // // for (int level = 0; level < levels; ++level)
    // //   digest.update(prevHash(level));

    // return ByteBuffer.wrap(digest.digest());

    return SkipLedger.rowHash(
      no(),
      inputHash(),
      hiPtrHash(),
      basePtrsHash());
  }


  public final long hiPtrNo() {
    return SkipLedger.hiPtrNo(no());
  }

  public ByteBuffer hiPtrHash() {
    return SkipLedger.hiPtrHash(no(), prevHashes());
  }


  final List<ByteBuffer> prevHashes() {
    final long rn = no();
    final boolean power2 = rn == Long.highestOneBit(rn);
    return Lists.functorList(
        prevLevels(),
        power2 ? this::filterPrevHash : this::prevHash);
  }


  private ByteBuffer filterPrevHash(int index) {
    if (no() == (1L << index))
      return DIGEST.sentinelHash();
    return prevHash(index);
  }


  public ByteBuffer basePtrsHash() {
    return SkipLedger.basePtrsHash(no(), prevHashes());
  }

  /**
   * 
   * @param levels  &ge; 2
   * @return
   */
  protected final ByteBuffer basePtrHash(final int levels) {
    if (levels == 2)
      return prevHash(0);
    var builder =
        new FixedLeafBuilder(SldgConstants.DIGEST.hashAlgo(), false);
    byte[] hptr = new byte[SldgConstants.HASH_WIDTH];
    for (int level = levels - 1; level-- > 0; ) {
      prevHash(level).get(hptr);
      builder.add(hptr);
    }
    return ByteBuffer.wrap(builder.build().hash());
  }



  public boolean hasAllLevels() {
    return SkipLedger.alwaysAllLevels(no()) || hasLoPtrs();
  }


  public final boolean isCondensed() {
    return !hasAllLevels();
  }


  protected boolean hasLoPtrs() {
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


