/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.morsel;


import java.net.URI;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Optional;

import io.crums.io.Serial;
import io.crums.io.SerialFormatException;
import io.crums.sldg.morsel.LedgerInfo.StdProps;
import io.crums.util.Lists;

/**
 * Meta info about a {@linkplain LedgerType#TABLE TABLE}-like ledger.
 */
public final class TableInfo extends LedgerInfo {
  
  
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
   * @param dateFormatPattern   optional date format pattern
   * 
   * @see SimpleDateFormat
   */
  public TableInfo(
      String alias, URI uri, String desc,
      List<Column> columns, String dateFormatPattern) {
    
    this(
        new StdProps(LedgerType.TABLE, alias, uri, desc),
        columns,
        dateFormatPattern);
  }
  
  
  /**
   * Main constructor.
   * 
   * @param props               of type {@linkplain LedgerType#TABLE}
   * @param columns             optional ordered list of column names and
   *                            descriptions (all-or-none, since positional
   *                            info is inferred from index in list)
   * @param dateFormatPattern   optional date format pattern
   */
  public TableInfo(StdProps props, List<Column> columns, String dateFormatPattern) {
    super(props);
    this.columns = columns == null ? List.of() : columns;
    this.dateFormatPattern = dateFormatPattern;
    
    if (!props.type().isTable())
      throw new IllegalArgumentException(
          "expected type %s; actual given was %s in argument %s"
          .formatted(LedgerType.TABLE, props));
    
    if (dateFormatPattern != null) try {
      dateFormat();
    } catch (Exception x) {
      throw new IllegalArgumentException(
          "malformed dateFormatPattern %s -- %s"
          .formatted(dateFormatPattern, x.getMessage()), x);
    }
  }
  
  
  private TableInfo(
      StdProps props, TableInfo copy) {
    super(props); 
    this.columns = copy.columns;
    this.dateFormatPattern = copy.dateFormatPattern;
  }
  
  
  
  
  
  @Override
  LedgerInfo edit(StdProps props) {
    return new TableInfo(props, this);
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
  
  

  @Override
  protected final Object otherProperties() {
    return List.of(columns(), dateFormatPattern());
  }


  @Override
  public int serialSize() {
    int tally = props.serialSize();
    tally += strlen(dateFormatPattern);
    tally += 4;
    for (int index = columns.size(); index-- > 0; )
      tally += columns.get(index).serialSize();
    return tally;
  }


  @Override
  public ByteBuffer writeTo(ByteBuffer out) throws BufferOverflowException {
    props.writeTo(out);
    putString(dateFormatPattern, out);
    final int count = columns.size();
    out.putInt(count);
    for (int index = 0; index < count; ++index)
      columns.get(index).writeTo(out);
    return out;
  }
  
  
  
  
  
  static TableInfo load(StdProps props, ByteBuffer in) {
    var dateFormatPattern = loadString(in);
    final int count = in.getInt();
    if (count < 0)
      throw new SerialFormatException(
          "read negative count %d at offset %d in %s"
          .formatted(count, in.position() - 4, in));
    List<Column> columns;
    if (count == 0)
      columns = List.of();
    else {
      Column[] array = new Column[count];
      for (int index = 0; index < count; ++index) {
        array[index] = Column.load(in);
      }
      columns = Lists.asReadOnlyList(array);
    }
    
    return new TableInfo(props, columns, dateFormatPattern);
  }
  
  
  
  
  /**
   * Column meta info: required name, optional description and units.
   */
  public static class Column implements Serial {
    
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


    @Override
    public int serialSize() {
      return strlen(name) + strlen(desc) + strlen(units);
    }


    @Override
    public ByteBuffer writeTo(ByteBuffer out) throws BufferOverflowException {
      putString(name, out);
      putString(desc, out);
      putString(units, out);
      return out;
    }
    
    
    
    public static Column load(ByteBuffer in) {
      var name = loadString(in);
      var desc = loadString(in);
      var units = loadString(in);
      try {
        return new Column(name, desc, units);
      } catch (SerialFormatException sfx) {
        throw sfx;
      } catch (Exception x) {
        throw new SerialFormatException(
            "on loading Column from %s: %s".formatted(in, x), x);
      }
    }
    
    
  }












}


















