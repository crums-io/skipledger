/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg.scraps;


import java.util.Objects;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import io.crums.model.CrumTrail;
import io.crums.model.json.CrumTrailParser;
import io.crums.sldg.Path;
import io.crums.sldg.json.PathParser;

/**
 * 
 */
public class TrailedPathParser {
  
  public final static String PATH = "path";
  
  public final static String TRAIL = "trail";
  
  
  public final static TrailedPathParser INSTANCE = new TrailedPathParser();
  
  
  
  private final PathParser pathParser;
  private final CrumTrailParser trailParser;
  
  
  
  public TrailedPathParser() {
    this(PathParser.INSTANCE, CrumTrailParser.INSTANCE);
  }
  
  public TrailedPathParser(PathParser pathParser, CrumTrailParser trailParser) {
    this.pathParser = Objects.requireNonNull(pathParser, "null pathParser");
    this.trailParser = Objects.requireNonNull(trailParser, "null trailParser");
  }
  
  
  
  
  
  
  @SuppressWarnings("unchecked")
  public JSONObject toJsonObject(TrailedPath tp) {
    
    JSONObject jTrailed = new JSONObject();
    jTrailed.put(PATH, pathParser.toJsonObject(tp.path()));
    jTrailed.put(TRAIL, trailParser.toJsonObject(tp.trail()));
    
    return jTrailed;
  }
  
  
  
  public TrailedPath toTrailedPath(String json) {
    try {
      return toTrailedPath((JSONObject) new JSONParser().parse(json));
    } catch (ParseException px) {
      throw new IllegalArgumentException("malformed json: " + json, px);
    }
  }
  
  
  
  
  public TrailedPath toTrailedPath(JSONObject jTrailed) {
    
    Path path = pathParser.toPath((JSONObject) jTrailed.get(PATH));
    CrumTrail trail = trailParser.toCrumTrail((JSONObject) jTrailed.get(TRAIL));
    
    return new TrailedPath(path, trail);
  }
  
  
  
  public PathParser getPathParser() {
    return pathParser;
  }
  
  
  public CrumTrailParser getTrailParser() {
    return trailParser;
  }

}
