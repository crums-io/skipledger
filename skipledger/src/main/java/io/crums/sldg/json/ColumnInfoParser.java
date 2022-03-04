/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.json;


import static io.crums.util.json.JsonUtils.*;

import java.util.List;

import io.crums.util.json.JsonEntityParser;
import io.crums.util.json.JsonParsingException;
import io.crums.util.json.simple.JSONArray;
import io.crums.util.json.simple.JSONObject;

import io.crums.sldg.src.ColumnInfo;

/**
 * {@linkplain ColumnInfo} JSON parser.
 */
public class ColumnInfoParser implements JsonEntityParser<ColumnInfo> {
  
  public final static String NAME = "name";
  public final static String CN = "cn";
  public final static String DESC = "desc";
  public final static String UNITS = "units";
  
  
  /**
   * Singleton stateless instance.
   * 
   * @see #ColumnInfoParser()
   */
  public final static ColumnInfoParser INSTANCE = new ColumnInfoParser();
  

  protected ColumnInfoParser() {  }
  

  @Override
  public JSONObject injectEntity(ColumnInfo column, JSONObject jObj) {
    jObj.put(CN, column.getColumnNumber());
    jObj.put(NAME, column.getName());
    putNotNull(jObj, DESC, column.getDescription());
    putNotNull(jObj, UNITS, column.getUnits());
    return jObj;
  }

  @Override
  public ColumnInfo toEntity(JSONObject jObj) throws JsonParsingException {
    String name = getString(jObj, NAME, true);
    int colNumber = getInt(jObj, CN);
    if (colNumber < 1)
      throw new JsonParsingException("'" + CN + "' " + colNumber + " < 1");
    String desc = getString(jObj, DESC, false);
    String units = getString(jObj, UNITS, false);
    return new ColumnInfo(name, colNumber, desc, units);
  }
  
  
  /**
   * Pseudonym for {@linkplain #toEntity(JSONObject)}.
   */
  public ColumnInfo toColumnInfo(JSONObject jObj) throws JsonParsingException {
    return toEntity(jObj);
  }
  
  
  /**
   * Pseudonym for {@linkplain #toEntityList(JSONArray)}.
   */
  public List<ColumnInfo> toColumnInfos(JSONArray jArray) throws JsonParsingException {
    return toEntityList(jArray);
  }
  

}
