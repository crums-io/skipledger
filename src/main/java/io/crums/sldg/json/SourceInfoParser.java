/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.json;


import static io.crums.sldg.json.JsonUtils.*;

import java.util.Objects;

import org.json.simple.JSONObject;

import io.crums.sldg.src.SourceInfo;

/**
 * {@linkplain SourceInfo} JSON parser.
 */
public class SourceInfoParser implements JsonEntityParser<SourceInfo> {
  
  public final static String NAME = "name";
  public final static String DESC = "desc";
  public final static String COLS = "columns";
  

  /**
   * Singleton stateless instance.
   * 
   * @see #SourceInfoParser(ColumnInfoParser)
   */
  public final static SourceInfoParser INSTANCE =
      new SourceInfoParser(ColumnInfoParser.INSTANCE);
  
  
  private final ColumnInfoParser columnParser;
  
  
  
  protected SourceInfoParser(ColumnInfoParser columnParser) {
    this.columnParser = Objects.requireNonNull(columnParser, "null columnParser");
  }

  

  @SuppressWarnings("unchecked")
  @Override
  public JSONObject toJsonObject(SourceInfo sourceInfo) {
    var jObj = new JSONObject();
    jObj.put(NAME, sourceInfo.getName());
    addIfPresent(jObj, DESC, sourceInfo.getDescription());
    if (sourceInfo.hasFixedColumns())
      jObj.put(
          COLS,
          ColumnInfoParser.INSTANCE.toJsonArray(sourceInfo.getColumnInfos()));
    return jObj;
  }

  @Override
  public SourceInfo toEntity(JSONObject jObj) throws JsonParsingException {
    String name = getString(jObj, NAME, true);
    String desc = getString(jObj, DESC, false);
    var jArray = getJsonArray(jObj, COLS, false);
    var columns = columnParser.toColumnInfos(jArray);
    try {
      return new SourceInfo(name, desc, columns);
    } catch (IllegalArgumentException iax) {
      throw new JsonParsingException(iax.getMessage());
    }
  }

}