/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.json;


import java.text.DateFormat;
import java.util.List;

import io.crums.util.json.simple.JSONArray;
import io.crums.util.json.simple.JSONObject;

import io.crums.model.CrumTrail;
import io.crums.sldg.src.ColumnType;
import io.crums.sldg.src.ColumnValue;
import io.crums.sldg.src.SourceRow;
import io.crums.sldg.time.TrailedRow;
import io.crums.util.Lists;

/**
 * 
 */
public class SourceRowParser implements JsonEntityParser<SourceRow> {
  
  public final static String RN = RowHashWriter.RN;
  public final static String COLS = "cols";
  
  public final static String DEFAULT_REDACT_SYM = "[X]";
  
  public final static String WIT = "wit";
  public final static String TRAIL = "trail";
  public final static String TREF = "tref";
  
  
  
  
  
  
  private final ColumnValueParser columnParser;
  
  private final DateFormat witDateFormat;


  
  /**
   * Constructs a default instance with no date formatting (dates are
   * represented in UTC millis).
   */
  public SourceRowParser() {
    this(null, null);
  }

  /**
   * Constructs an instance with optional date formatting.
   * <em>Generally, not thread safe</em> (since {@code DateFormat}s
   * are not).
   * 
   * @param dateFormatter optional formating for date values
   */
  public SourceRowParser(DateFormat dateFormatter) {
    this.columnParser = new ColumnValueParser(dateFormatter);
    this.witDateFormat = dateFormatter;
  }
  
  /**
   * This constructor allows mixing date formats for witness dates
   * and column-dates. Either argument may be null.
   * 
   * @param colDateFormat used when a column's type is {@linkplain ColumnType#DATE DATE} 
   * @param witDateFormat used to display the witness time
   */
  public SourceRowParser(DateFormat colDateFormat, DateFormat witDateFormat) {
    this.columnParser = new ColumnValueParser(colDateFormat);
    this.witDateFormat = witDateFormat;
  }
  

  
  
  
  
  
  
  
  
  @SuppressWarnings("unchecked")
  @Override
  public JSONObject injectEntity(SourceRow srcRow, JSONObject jObj) {
    jObj.put(RN, srcRow.rowNumber());
    
    var jArray = new JSONArray();
    for (var column : srcRow.getColumns())
      jArray.add(columnParser.toJsonObject(column));
    jObj.put(COLS, jArray);
    return jObj;
  }
  
  /**
   * Returns the given source row as a JSON object. Embeds the given
   * {@code trail} in the {@linkplain #TRAIL}, {@linkplain #TREF}, and
   * {@linkplain #WIT} JSON fields.
   * 
   * @param srcRow  not null
   * @param trail   not null
   */
  public JSONObject toJsonObject(SourceRow srcRow, CrumTrail trail) {
    var jObj = toJsonObject(srcRow);
    return TrailedRowWriter.DEFAULT_INSTANCE.injectEntity(
        new TrailedRow(srcRow.rowNumber(), trail), jObj, witDateFormat);
  }
  
  /**
   * Returns the given source row as a JSON object. Embeds the given
   * {@code trail} in the {@linkplain #TRAIL}, {@linkplain #TREF}, and
   * {@linkplain #WIT} JSON fields.
   * 
   * @param srcRow  not null
   * @param trail   not null
   */
  public JSONObject toJsonObject(SourceRow srcRow, TrailedRow trail) {
    var jObj = toJsonObject(srcRow);
    return TrailedRowWriter.DEFAULT_INSTANCE.injectEntity(
        trail, jObj, witDateFormat);
  }
  
  
  
  
  

  public JSONObject toSlimJsonObject(SourceRow srcRow) {
    return toSlimJsonObject(srcRow, DEFAULT_REDACT_SYM);
  }
  
  
  @SuppressWarnings("unchecked")
  public JSONObject toSlimJsonObject(SourceRow srcRow, String redactSymbol) {
    var jObj = new JSONObject();
    jObj.put(RN, srcRow.rowNumber());
    
    var jArray = new JSONArray();
    if (redactSymbol == null) {
      for (var column : srcRow.getColumns())
        jArray.add(columnParser.getJsonValue(column));
    } else {
      for (var column : srcRow.getColumns()) {
        boolean redacted = column.getType().isHash();
        jArray.add(redacted ? redactSymbol : columnParser.getJsonValue(column));
      }
    }
    jObj.put(COLS, jArray);
    return jObj;
  }
  

  @SuppressWarnings("unchecked")
  @Override
  public SourceRow toEntity(JSONObject jObj) throws JsonParsingException {
    long rowNumber = JsonUtils.getNumber(jObj, RN, true).longValue();
    var jCols = JsonUtils.getJsonArray(jObj, COLS, true);
    List<ColumnValue> cols = Lists.map(jCols, c -> columnParser.toEntity((JSONObject) c));
    return new SourceRow(rowNumber, cols);
  }

}
