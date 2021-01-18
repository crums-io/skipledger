/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg;


import static io.crums.sldg.RowIntersect.*;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;


/**
 * A pair of intersecting rows. Since rows in a {@linkplain Ledger skip ledger} contain
 * hash pointers to previous rows, rows do not necessarily intersect at their own row
 * numbers: they may also intersect at a row referenced by one or both rows' hash pointers.
 * <p>
 * The relationship, then, between pairs of rows ({@linkplain #first()}, {@linkplain #second()})
 * and instances of this class is one-to-many.
 * </p>
 * 
 * @see #type()
 * @see PathIntersector
 */
public class RowIntersection {

  private final long rowNumber;
  private final Row a;
  private final Row b;
  private final boolean conflict;
  
  
  /**
   * Creates a {@linkplain RowIntersect#direct() direct} intersection.
   */
  public RowIntersection(Row row) {
    this.rowNumber = Objects.requireNonNull(row, "null row").rowNumber();
    this.b = this.a = row;
    this.conflict = false;
  }
  
  public RowIntersection(long rowNumber, Row a, Row b) {
    this(rowNumber, a, b, false);
  }

  /**
   * 
   */
  public RowIntersection(long rowNumber, Row a, Row b, boolean conflict) {
    this.rowNumber = rowNumber;
    Objects.requireNonNull(a, "null row a");
    Objects.requireNonNull(b, "null row b");
    if (a.rowNumber() <= b.rowNumber()) {
      this.a = a;
      this.b = b;
    } else {
      this.a = b;
      this.b = a;
    }
    this.conflict = conflict;
    verify();
  }
  
  
  private void verify() {
    if (rowNumber <= 0)
      throw new IllegalArgumentException("rowNumber " + rowNumber + " not positive");
    boolean eq = a.hash(rowNumber).equals(b.hash(rowNumber));
    boolean ok = eq && !conflict || (!eq) && conflict;
    if (!ok) {
      String msg;
      if (conflict)
        msg =
            "rows actually intersect at [" + rowNumber + "]: source rows (" +
            a.rowNumber() + "," + b.rowNumber() + "); conflict=true";
      else
        msg =
            "rows do not intersect at [" + rowNumber + "]: source rows (" +
                a.rowNumber() + "," + b.rowNumber() + "); conflict=false";
      
      throw new IllegalArgumentException(msg);
    }
  }
  
  
  /**
   * Determines if the intersection is in fact a conflict.
   */
  public final boolean isConflict() {
    return conflict;
  }


  /**
   * Returns the row number at the intersection.
   */
  public final long rowNumber() {
    return rowNumber;
  }
  
  
  /**
   * Returns the type of interesection.
   */
  public final RowIntersect type() {
    if (rowNumber == a.rowNumber())
      return rowNumber == b.rowNumber() ? NUM_NUM : NUM_REFNUM;
    else
      return REFNUM_REFNUM;
  }
  
  
  /**
   * Returns the hash of the intersected row.
   */
  public final ByteBuffer rowHash() {
    return a.hash(rowNumber);
  }
  
  
  /**
   * Returns the actual row at the intersection. If the intersection {@linkplain #type() type}
   * is {@linkplain RowIntersect#REFNUM_REFNUM} then the return value
   * {@linkplain Optional#isEmpty() is empty} (in which case, partial info about that row is
   * provided thru this class).
   * 
   * @see #rowHash()
   * @see #rowNumber()
   */
  public final Optional<Row> getRow() {
    return rowNumber == a.rowNumber() ? Optional.of(a) : Optional.empty();
  }
  
  
  /**
   * Returns the row with the <em>lower</em> row number (or the same row if their numbers are
   * equal).
   * 
   * @see #second()
   */
  public final Row first() {
    return a;
  }
  

  /**
   * Returns the row with the <em>higher</em> row number (or the same row if their numbers are
   * equal).
   * 
   * @see #first()
   */
  public final Row second() {
    return b;
  }
  
  
  @Override
  public String toString() {
    RowIntersect type = type();
    StringBuilder string = new StringBuilder();
    string.append('<').append(rowNumber).append('|').append(type);
    if (type.byReference())
      string.append(':').append(second().rowNumber());
    else if (type.byLineage())
      string.append(':').append(first().rowNumber()).append(',').append(second().rowNumber());
    
    string.append('>');
    return string.toString();
  }
  
  
  /**
   * Determines if instances with higher {@linkplain #rowNumber row number}s exist for this
   * same pair of rows ({@linkplain #first()}, {@linkplain #second()}).
   */
  public final boolean isDerivative() {
    //     type().byLineage()         && Ledger.rowsLinked(a.rowNumber(), b.rowNumber());
    return rowNumber != a.rowNumber() && Ledger.rowsLinked(a.rowNumber(), b.rowNumber());
  }
  
  
  
  public final boolean sameSource(RowIntersection other) {
    return other.first().equals(first()) && other.second().equals(second());
  }
  
  public final boolean sameSourceNumbers(RowIntersection other) {
    return
        other.first().rowNumber() == first().rowNumber() &&
        other.second().rowNumber() == second().rowNumber();
  }
  
  
  /**
   * Two instances are equal if they have exactly the same state, that is, if they have the
   * same row numbers and have the same rows.
   * 
   * @see #rowNumber()
   * @see #first()
   * @see #second()
   */
  @Override
  public final boolean equals(Object o) {
    if (o == this)
      return true;
    else if (o instanceof RowIntersection) {
      RowIntersection other = (RowIntersection) o;
      return
          rowNumber() == other.rowNumber() &&
          other.first().equals(first()) &&
          other.second().equals(second());
    } else
      return false;
  }
  
  
  
  /**
   * Consistent with equals.
   */
  @Override
  public int hashCode() {
    return Long.hashCode(rowNumber) ^ a.hashCode();
  }

}











