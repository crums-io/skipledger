/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg;


/**
 * Partial information about an {@linkplain Entry entry}. It is not named <em>Meta..</em> because
 * it doesn't yet have <em>extra</em> information about an entry; but a subclass could change
 * that (see for example {@linkplain #name()}).
 * <p>
 * Equality and comparison semantics are governed solely by {@linkplain #rowNumber() row number}.
 * Instances are immutable.
 * </p>
 */
public class EntryInfo implements Comparable<EntryInfo> {
  
  private final long rowNumber;
  private final int size;
  
  /**
   * Constructor validates.
   * 
   * @param rowNumber &ge; 1
   * @param size &ge; 1
   */
  public EntryInfo(long rowNumber, int size) {
    this.rowNumber = rowNumber;
    this.size = size;
    
    if (rowNumber <= 0)
      throw new IllegalArgumentException("row number " + rowNumber);
    if (size <= 0)
      throw new IllegalArgumentException("size " + rowNumber);
  }
  
  
  /**
   * Returns the entry's {@linkplain Entry#rowNumber() row number}.
   * 
   * @return &ge; 1
   */
  public final long rowNumber() {
    return rowNumber;
  }
  

  /**
   * Returns the number of bytes in the entry's {@linkplain Entry#contents() contents}.
   * 
   * @return &ge; 1
   */
  public final int size() {
    return size;
  }
  
  
  /**
   * Determines if the instance is overridden to have a meta string.
   * 
   * @see #meta()
   */
  public boolean hasMeta() {
    return false;
  }
  
  /**
   * Returns the meta string, which would usually be just a name. If
   * overridden, then {@linkplain #hasMeta()} should be overridden also.
   * 
   * @return defaults to the row number in decimal
   */
  public String meta() {
    return String.valueOf(rowNumber);
  }
  
  
  /**
   * Equality semanitics governed solely by {@linkplain #rowNumber() row number}.
   */
  public final boolean equals(Object obj) {
    return
        obj == this ||
        (obj instanceof EntryInfo) && rowNumber == ((EntryInfo) obj).rowNumber;
  }
  
  
  /**
   * @return derived from the row number only
   * @see #equals(Object)
   */
  public final int hashCode() {
    return Long.hashCode(rowNumber);
  }


  /**
   * Instances are ordered by {@linkplain #rowNumber() row number}.
   * 
   * @see #equals(Object)
   */
  @Override
  public final int compareTo(EntryInfo other) {
    return Long.compare(rowNumber, other.rowNumber);
  }
  
  
  @Override
  public String toString() {
    return "(" + rowNumber + ":" + size + ")";
  }

}
