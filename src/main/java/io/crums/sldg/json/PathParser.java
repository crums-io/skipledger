/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg.json;


import java.util.Objects;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import io.crums.sldg.Path;

/**
 * Parses both lists of rows and paths. The representations are exactly the
 * same; paths involve constraints on both the order and the row numbers.
 */
public class PathParser implements JsonEntityParser<Path> {
  
  /**
   * The rows tag
   */
  public final static String ROWS = "rows";
  
  
  public final static PathParser INSTANCE = new PathParser();
  
  
  
  
  private final RowParser rowParser;
  
  
  public PathParser() {
    this(RowParser.INSTANCE);
  }
  
  
  public PathParser(RowParser rowParser) {
    this.rowParser = Objects.requireNonNull(rowParser, "null rowParser");
  }
  
  
  
  @SuppressWarnings("unchecked")
  @Override
  public JSONObject toJsonObject(Path path) {
    
    JSONArray jPath = rowParser.toJsonArray(path.path());
    
    JSONObject jObj = new JSONObject();
    jObj.put(ROWS, jPath);
    return jObj;
  }
  
  
  
  public Path toPath(String json) {
    try {
      
      return toPath((JSONObject) new JSONParser().parse(json));
    
    } catch (ParseException px) {
      throw new IllegalArgumentException("malformed json: " + json);
    }
  }
  
  
  
  public Path toPath(JSONObject jObj) {
    JSONArray jArray = (JSONArray) jObj.get(ROWS);
    return new Path(rowParser.toRows(jArray));
  }
  
  
  
  public RowParser getRowParser() {
    return rowParser;
  }


  @Override
  public Path toEntity(JSONObject jObj) {
    return toPath(jObj);
  }
  

}
