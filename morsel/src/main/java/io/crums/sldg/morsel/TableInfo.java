/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.morsel;


import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Optional;

/**
 * Meta info about a {@linkplain LedgerType#TABLE TABLE}-like ledger.
 */
public class TableInfo extends LedgerInfo {
  
  
  private final List<Column> columns;
  private final String dateFormatPattern;
  

  /**
   * Main constructor.
   * 
   * @param alias               locally unique name (trimmed)
   * @param uri                 optional (may be {@code null})
   * @param desc                optional description ({@code null} or blank counts
   *                            for naught)
   * @param columns             optional ordered list of column names and
   *                            descriptions (all-or-none, since positional
   *                            info is inferred from index in list)
   * @param dateFormatPattern   date format pattern
   * 
   * @see SimpleDateFormat
   */
  public TableInfo(
      String alias, URI uri, String desc, List<Column> columns, String dateFormatPattern) {
    super(LedgerType.TABLE, alias, uri, desc);
    this.columns = columns == null ? List.of() : columns;
    this.dateFormatPattern = dateFormatPattern;
    
    if (dateFormatPattern != null) try {
      dateFormat();
    } catch (Exception x) {
      throw new IllegalArgumentException(
          "malformed dateFormatPattern %s -- %s"
          .formatted(dateFormatPattern, x.getMessage()), x);
    }
  }
  
  
  /**
   * Returns the (optional) list of columns.
   * 
   * @see TableInfo.Column
   */
  public List<Column> columns() {
    return columns;
  }
  
  
  /**
   * Returns the optional date format pattern used to construct
   * the date format.
   * 
   * @see SimpleDateFormat
   * @see #dateFormat()
   */
  public Optional<String> dateFormatPattern() {
    return Optional.ofNullable(dateFormatPattern);
  }
  
  
  /** Returns the optional date formmat. */
  public Optional<DateFormat> dateFormat() {
    return dateFormatPattern == null ?
        Optional.empty() : Optional.of(new SimpleDateFormat(dateFormatPattern));
  }
  
  
  
  
  /**
   * Column meta info: required name, optional description and units.
   */
  public static class Column {
    
    private final String name;
    private final String desc;
    private final String units;
    
    
    /**
     * Constructs an instance with the given name.
     * 
     * @param name      required (trimmed)
     * @param desc      optional description (may be null)
     * @param units     optional units  (may be null)
     */
    public Column(String name, String desc, String units) {
      this.name = name.trim();
      this.desc = desc;
      this.units = units;
      if (this.name.isEmpty())
        throw new IllegalArgumentException("empty or blank name");
    }
    
    
    /**
     * Returns the column name.
     * 
     * @return a non-empty, trimmed string
     */
    public String name() {
      return name;
    }
    
    
    /** Returns the description, if any. */
    public Optional<String> description() {
      return nonBlankOpt(desc);
    }
    

    /** Returns the units, if any. */
    public Optional<String> units() {
      return nonBlankOpt(units);
    }
    
    
    
    
    
  }

}


















