/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.src;


import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import io.crums.util.Lists;

/**
 * Meta information about a column. Localization is kicked down the road
 * for now.
 * 
 * @see SourceInfo#SourceInfo(String, String, List)
 */
public class ColumnInfo implements Comparable<ColumnInfo> {
  
  private final String name;
  private final int colNumber;
  private final String desc;
  private final String units;


  /**
   * Creates an instance with no optional parameters.
   * 
   * @param name required name (but may be empty)
   * @param colNumber 1-based column number
   */
  public ColumnInfo(String name, int colNumber) {
    this(name, colNumber, null, null);
  }
  
  /**
   * 
   * @param name required name (but may be empty)
   * @param colNumber 1-based column number
   * @param desc  optional
   * @param units optional
   */
  public ColumnInfo(String name, int colNumber, String desc, String units) {
    this.name = Objects.requireNonNull(name);
    this.colNumber = colNumber;
    this.desc = desc;
    this.units = units;
    
    if (colNumber < 1)
      throw new IllegalArgumentException("colNumber " + colNumber + " < 1");
  }
  
  
  /**
   * Returns the column name. (Kicking localizaton down the road).
   * @return
   */
  public String getName() {
    return name;
  }
  
  
  /**
   * Returns the 1-based column number. I.e. the first column is numbered one.
   * 
   * @return &ge; 1
   */
  public int getColumnNumber() {
    return colNumber;
  }
  
  
  public String getDescription() {
    return desc;
  }
  
  
  public String getUnits() {
    return units;
  }


  /**
   * Instances are ordered by {@linkplain #getColumnNumber() column number}.
   */
  @Override
  public int compareTo(ColumnInfo o) {
    return Integer.compare(colNumber, o.colNumber);
  }
  
  
  /**
   * Sorts and returns the given list of instances, if they're not already sorted.
   * If 2 instances describe the same {@linkplain #getColumnNumber() column number},
   * an exception is thrown.
   * 
   * @param list non-null, with no dups
   * 
   * @return unless {@code list} is already sorted, a new read-only sorted list
   *         is returned
   */
  public static List<ColumnInfo> sort(List<ColumnInfo> list) {
    if (Lists.isSortedNoDups(list))
      return list;
    ColumnInfo[] array = list.toArray(new ColumnInfo[list.size()]);
    Arrays.sort(array);
    list = Lists.asReadOnlyList(array);
    if (!Lists.isSortedNoDups(list))
      throw new IllegalArgumentException("list contains dups: " + list);
    return Lists.asReadOnlyList(array);
  }

}
