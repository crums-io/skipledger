/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.json;


import java.text.DateFormat;
import java.util.List;
import java.util.Objects;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import io.crums.sldg.src.ColumnValue;
import io.crums.sldg.src.SourceRow;
import io.crums.util.Lists;

/**
 * 
 */
public class SourceRowParser implements JsonEntityParser<SourceRow> {
  
  public final static String RN = "rn";
  public final static String COLS = "cols";
  
  public final static String DEFAULT_REDACT_SYM = "[X]";
  
  
  
  
  
  
  private final ColumnValueParser columnParser;


  
  /**
   * Constructs a default instance with no date formatting (dates are
   * represented in UTC millis).
   */
  public SourceRowParser() {
    this(new ColumnValueParser());
  }

  /**
   * Constructs an instance with optional date formatting.
   * <em>Generally, not thread safe</em> (since {@code DateFormat}s
   * are not).
   * 
   * @param dateFormatter optional formating for date values
   */
  public SourceRowParser(DateFormat dateFormatter) {
    this(new ColumnValueParser(dateFormatter));
  }
  
  /**
   * Creates a new instance. <em>Iff thread safe, if argument is
   * thread safe.</em>
   * 
   * @param columnParser non-null
   */
  public SourceRowParser(ColumnValueParser columnParser) {
    this.columnParser = Objects.requireNonNull(columnParser, "null columnParser");
  }

  
  
  
  
  
  
  
  
  @SuppressWarnings("unchecked")
  @Override
  public JSONObject toJsonObject(SourceRow srcRow) {
    var jObj = new JSONObject();
    jObj.put(RN, srcRow.rowNumber());
    
    var jArray = new JSONArray();
    for (var column : srcRow.getColumns())
      jArray.add(columnParser.toJsonObject(column));
    jObj.put(COLS, jArray);
    return jObj;
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
