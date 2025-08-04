/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.bindle;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import io.crums.io.Serial;
import io.crums.io.SerialFormatException;

/**
 * Specifies an row entry (cell) in one ledger referencing an entry in another
 * ledger. The actual ledgers are not identified, as all references from one
 * ledger to another are packaged together.
 * 
 * @param fromRow &ge; 1
 * @param fromCol &ge; -1: -1 means <em>all</em> of the columns; otherwise, this
 *                is a zero-based index into the row's cells.
 * @param toRow   &ge; 1
 * @param toCol   &ge; -2: -2 means the skipledger (commitment) hash of the
 *                entire row is referenced; -1 means <em>all</em> of the columns
 */
public record Reference(long fromRow, int fromCol, long toRow, int toCol)
    implements Comparable<Reference>, Serial {
  
  
  public final static Reference FIRST_KEY = newSearchKey(1L);
  
  /**
   * Fixed serial byte size (24).
   */
  public final static int SERIAL_SIZE = 24;   // 8 + 4 + 8 + 4

  
  public Reference {
    
    if (fromRow <= 0L)
      throw new IllegalArgumentException("fromRow " + fromRow);
    if (fromCol < -1)
      throw new IllegalArgumentException("fromCol " + fromCol);
    if (toRow <= 0L)
      throw new IllegalArgumentException("toRow " + toRow);
    if (toCol < -2)
      throw new IllegalArgumentException("toCol " + toCol);
    if (fromCol == -1 && toCol != -1 ||
        fromCol != -1 && toCol == -1)
      throw new IllegalArgumentException(
          "either both must be -1 or neither: fromCol %d; toCol %d"
          .formatted(fromCol, toCol));
  }
  
  /**
   * Creates a <em>same-contents</em> reference. The contents of the 2
   * rows are the equal.
   * 
   * @param fromRow &ge; 1
   * @param toRow   $ge; 1
   */
  public Reference(long fromRow, long toRow) {
    this(fromRow, -1, toRow, -1);
  }
  
  
  /**
   * Creates a <em>commitment-hash</em> reference. This is a reference from
   * a single cell to the skipledger row-hash of another ledger.
   * 
   * @param fromRow &ge; 1
   * @param fromCol &ge; 0
   * @param toRow   &ge; 1
   */
  public Reference(long fromRow, int fromCol, long toRow) {
    this(fromRow, fromCol, toRow, -2);
  }
  
  
  /**
   * Returns {@code true} iff the reference is from a single cell in the
   * {@linkplain #fromRow fromRow}.
   * 
   * @return {@code fromCol() >= 0}
   */
  public boolean fromSingleCell() {
    return fromCol >= 0;
  }
  
  /**
   * Returns {@code true} iff the contents of both rows (i.e. all cell values)
   * are equal.
   * 
   * @return {@code fromCol() == -1}
   */
  public boolean sameContent() {
    return fromCol == -1;
  }
  
  
  /**
   * Returns {@code true} iff a specific cell is referenced.
   * 
   * @return {@code toCol() >= 0}
   */
  public boolean toSingleCell() {
    return toCol >= 0;
  }
  
  
  
  /**
   * Returns {@code true} iff the skipledger row-hash (commitment hash) is
   * referenced.
   * 
   * @return {@code toCol() == -2}
   * @see #toSource()
   */
  public boolean toCommit() {
    return toCol == -2;
  }
  
  
  /**
   * Return {@code true} iff the foreign source-row is referenced. I.e.
   * returns {@code true}, iff either of {@linkplain #sameContent()} or
   * {@linkplain #toSingleCell()} return {@code true}.
   * 
   * @return {@code !toCommit()}
   * @see #toCommit()
   */
  public boolean toSource() {
    return !toCommit();
  }
  

  
  /**
   * Instances are ordered by:
   * <ol>
   * <li>{@linkplain #fromRow()}</li>
   * <li>{@linkplain #fromCol()}</li>
   * <li>{@linkplain #toRow()}</li>
   * <li>{@linkplain #toCol()}</li>
   * </ol>
   */
  @Override
  public int compareTo(Reference o) {
    int comp = compCoordinates(fromRow, fromCol, o.fromRow, o.fromCol);
    return comp == 0 ? compCoordinates(toRow, toCol, o.toRow, o.toCol) : comp;
  }
  
  
  
  private int compCoordinates(long row, int c, long oRow, int oC) {
    int comp = Long.compare(row, oRow);
    return comp == 0 ? Integer.compare(c, oC) : comp;
  }

  @Override
  public int serialSize() {
    return SERIAL_SIZE;
  }

  @Override
  public ByteBuffer writeTo(ByteBuffer out) throws BufferOverflowException {
    return out.putLong(fromRow).putInt(fromCol).putLong(toRow).putInt(toCol);
  }
  
  
  
  /**
   * @throws IllegalArgumentException
   *         <em>in lieu of</em> {@linkplain SerialFormatException}
   */
  public static Reference load(ByteBuffer in)
      throws IllegalArgumentException, BufferUnderflowException {
    
    long fromRn = in.getLong();
    int fromColNo = in.getInt();
    long toRn = in.getLong();
    int toColNo = in.getInt();
    
    return new Reference(fromRn, fromColNo, toRn, toColNo);
  }
  
  
  
  
  public static Reference newSearchKey(long fromRn) {
    return new Reference(fromRn, -1, 1L, -1);
  }
  
  

}











