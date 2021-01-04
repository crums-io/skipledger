/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg.json;


import static io.crums.sldg.json.RowParser.RN;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import io.crums.sldg.Path;
import io.crums.sldg.Row;
import io.crums.sldg.TargetPath;
import io.crums.util.Tuple;

/**
 * Parses both lists of rows and paths. The representations are exactly the
 * same; paths involve constraints on both the order and the row numbers.
 */
public class PathParser implements JsonEntityParser<Path> {
  
  /**
   * The rows tag.
   */
  public final static String ROWS = "rows";
  public final static String TARGET = "target";
  public final static String BEACONS = "beacons";
  public final static String UTC = "utc";
  
  
  
  
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
    
    JSONObject jObj = new JSONObject();
    
    JSONArray jPath = rowParser.toJsonArray(path.rows());
    jObj.put(ROWS, jPath);
    
    if (path.isTargeted())
      jObj.put(TARGET, path.target().rowNumber());
    
    
    if (path.hasBeacon()) {
      JSONArray bArray = new JSONArray();
      for (Tuple<Long,Long> rnUtc : path.beacons()) {
        JSONObject jBeacon = new JSONObject();
        jBeacon.put(RN, rnUtc.a);
        jBeacon.put(UTC, rnUtc.b);
        bArray.add(jBeacon);
      }
      jObj.put(BEACONS, bArray);
    }
    
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
    List<Row> rows = rowParser.toRows(jArray);
    
    List<Tuple<Long,Long>> beacons;
    Object jBeaconsObj = jObj.get(BEACONS);
    if (jBeaconsObj == null)
      beacons = Collections.emptyList();
    else {
      JSONArray jBeacons = (JSONArray) jBeaconsObj;
      beacons = new ArrayList<>(jBeacons.size());
      for (int index = 0; index < jBeacons.size(); ++index) {
        JSONObject jBcn = (JSONObject) jBeacons.get(index);
        Long rn = (Long) jBcn.get(RN);
        Long utc = (Long) jBcn.get(UTC);
        beacons.add(new Tuple<>(rn, utc));
      }
    }
    
    Object target = jObj.get(TARGET);
    
    
    return target == null ? new Path(rows, beacons) : new TargetPath(rows,  beacons, (Long) target);
  }
  
  
  
  public RowParser getRowParser() {
    return rowParser;
  }


  @Override
  public Path toEntity(JSONObject jObj) {
    return toPath(jObj);
  }
  

}
