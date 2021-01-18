/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg;

/**
 * Classification of how a pair of rows intersect. Does not include the
 * null-set. If 2 rows intersect, they may only intersect in the following
 * ways.
 */
public enum RowIntersect {
  
  /**
   * Rows intersect at a row referenced by both rows. The intersection's
   * row number is less than both rows'.
   * 
   * @see #byLineage()
   */
  REFNUM_REFNUM,
  /**
   * Rows interesect at the row with the lower row number.
   * 
   * @see #byReference()
   */
  NUM_REFNUM,
  /**
   * Rows intersect because they're the same row.
   * 
   * @see #direct()
   */
  NUM_NUM;
  
  
  /**
   * The two rows are the same (and the intersection is at their
   * row number).
   * 
   * @return {@code this == NUM_NUM}
   */
  public boolean direct() {
    return this == NUM_NUM;
  }

  /**
   * The intersection is at the row with lower row number. The other
   * row intersects this earlier row thru one of its hash pointers. Example
   * row number pairs:
   * <pre>
   *  8 10
   * 11 12
   * </pre>
   * 
   * @return {@code this == NUM_REFNUM}
   */
  public boolean byReference() {
    return this == NUM_REFNUM;
  }
  
  
  /**
   * The intersection is at a row with a lower number than that of both rows.
   * It is at a row referenced thru the 2 rows' hash pointers. Example
   * row number pairs:
   * <pre>
   * [8]   9 12 
   * [10] 11 12
   * [16] 18 32 
   * </pre>
   * 
   * @return {@code this == REFNUM_REFNUM}
   */
  public boolean byLineage() {
    return this == REFNUM_REFNUM;
  }

}













