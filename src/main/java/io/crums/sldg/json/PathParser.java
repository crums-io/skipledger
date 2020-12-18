/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg.json;


import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import io.crums.sldg.Path;
import io.crums.sldg.Row;

/**
 * Parses both lists of rows and paths. The representations are exactly the
 * same; paths involve constraints on both the order and the row numbers.
 */
public class PathParser {
  
  
  public final static PathParser INSTANCE = new PathParser();
  
  
  
  
  private final RowParser rowParser;
  
  
  public PathParser() {
    this(RowParser.INSTANCE);
  }
  
  
  public PathParser(RowParser rowParser) {
    this.rowParser = Objects.requireNonNull(rowParser, "null rowParser");
  }
  
  
  
  @SuppressWarnings("unchecked")
  public JSONArray toJsonArray(Path path) {
    
    JSONArray jPath = new JSONArray();
    for (Row row : path.path())
      jPath.add(rowParser.toJsonObject(row));
    
    return jPath;
  }
  
  
  
  @SuppressWarnings("unchecked")
  public JSONArray toJsonArray(List<Row> rows) {

    JSONArray jRows = new JSONArray();
    for (Row row : rows)
      jRows.add(rowParser.toJsonObject(row));
    
    return jRows;
  }
  
  
  
  public Path toPath(String json) {
    try {
      
      return toPath((JSONArray) new JSONParser().parse(json));
    
    } catch (ParseException px) {
      throw new IllegalArgumentException("malformed json: " + json);
    }
  }
  
  
  
  public Path toPath(JSONArray jArray) {
    return new Path(toRows(jArray));
  }
  
  
  
  public List<Row> toRows(JSONArray jArray) {
    int size = jArray.size();
    ArrayList<Row> rows = new ArrayList<>(size);
    for (int index = 0; index < size; ++index)
      rows.add( rowParser.toRow((JSONObject) jArray.get(index)) );
    return rows;
  }
  
  
  
  public RowParser getRowParser() {
    return rowParser;
  }
  

}
