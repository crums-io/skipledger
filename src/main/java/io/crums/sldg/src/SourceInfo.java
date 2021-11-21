/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.src;


import java.util.Collections;
import java.util.List;
import java.util.Objects;

import io.crums.util.Lists;

/**
 * Meta information about the source ledger and source rows. Note, while this
 * information may be included in a morsel, it is not designed to be validated.
 */
public class SourceInfo {
  
  private final String name;
  private final String desc;
  private final List<ColumnInfo> columns;
  
  

  /**
   * 
   * @param name    required name
   * @param desc    optional description
   * @param columns a null or empty list indicates the source does not have a fixed
   *        number of columns per row. Do not modify the given list--unless you're sure
   *        it's <em>not</em> sorted (in which case a new sorted list is created).
   */
  public SourceInfo(String name, String desc, List<ColumnInfo> columns) {
    this.name = Objects.requireNonNull(name);
    this.desc = desc;
    this.columns = columns == null ? Collections.emptyList() : ColumnInfo.sort(columns);
  }
  
  
  /**
   * Returns the name of the ledger.
   * 
   * @return not null
   */
  public String getName() {
    return name;
  }
  
  
  /**
   * Returns the optional description.
   */
  public String getDescription() {
    return desc;
  }
  
  
  /**
   * Determines if the advertised ledger
   * @return
   */
  public boolean hasFixedColumns() {
    return getColumnInfoCount() > 0;
  }
  
  
  /**
   * Returns the number of columns having meta information. Not all columns
   * need have meta information.
   */
  public int getColumnInfoCount() {
    return columns.size();
  }
  
  
  public List<String> getColumnNames() {
    return Lists.map(columns, c -> c.getName());
  }
  
  
  public List<ColumnInfo> getColumnInfos() {
    return columns;
  }
  

}
