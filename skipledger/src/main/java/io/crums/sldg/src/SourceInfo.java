/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.src;


import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import io.crums.sldg.SldgConstants;
import io.crums.util.Lists;

/**
 * Meta information about the source ledger and source rows. Note, while this
 * information may be included in a morsel, it is not designed to be validated.
 */
public class SourceInfo {
  
  private final String name;
  private final String desc;
  private final List<ColumnInfo> columns;
  private final String dateFormat;
  


  /**
   * Creates an instance with no date format pattern.
   * 
   * @param name        required name
   * @param desc        optional description
   * @param columns     a null or empty list indicates the source does not have a fixed
   *        number of columns per row. Do not modify the given list--unless you're sure
   *        it's <em>not</em> sorted (in which case a new sorted list is created).
   */
  public SourceInfo(String name, String desc, List<ColumnInfo> columns) {
    this(name, desc, columns, null);
  }

  /**
   * Full constructor.
   * 
   * @param name        required name
   * @param desc        optional description
   * @param columns     a null or empty list indicates the source does not have a fixed
   *        number of columns per row. Do not modify the given list--unless you're sure
   *        it's <em>not</em> sorted (in which case a new sorted list is created).
   * @param dateFormat  optional date format pattern (as specified in {@linkplain SimpleDateFormat})
   */
  public SourceInfo(String name, String desc, List<ColumnInfo> columns, String dateFormat) {
    this.name = Objects.requireNonNull(name);
    this.desc = desc;
    this.columns = columns == null || columns.isEmpty() ?
        Collections.emptyList() : ColumnInfo.sort(columns);
    this.dateFormat = dateFormat;
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
  
  
  public String getDateFormatPattern() {
    return dateFormat;
  }
  
  
  public Optional<DateFormat> getDateFormat() {
    if (dateFormat == null)
      return Optional.empty();
    try {
      return Optional.of(new SimpleDateFormat(dateFormat));
    } catch (IllegalArgumentException iax) {
      SldgConstants.getLogger().warning("ignoring bad date format pattern: '" + dateFormat + "'");
      return Optional.empty();
    }
  }
  

}
