/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg.json;

import java.util.Objects;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import io.crums.sldg.Nugget;
import io.crums.sldg.Path;
import io.crums.sldg.TrailedPath;

/**
 * 
 */
public class NuggetParser {
  
  public final static String PATH = "path";
  public final static String WIT = "wit";
  
  
  public final static NuggetParser INSTANCE = new NuggetParser();
  
  
  private final TrailedPathParser parser;
  
  
  
  public NuggetParser() {
    this(TrailedPathParser.INSTANCE);
  }
  

  
  public NuggetParser(TrailedPathParser parser) {
    this.parser = Objects.requireNonNull(parser, "null parser");
  }
  
  
  
  public TrailedPathParser getTrailedPathParser() {
    return parser;
  }
  
  
  
  
  @SuppressWarnings("unchecked")
  public JSONObject toJsonObject(Nugget nugget) {
    
    JSONObject jNug = new JSONObject();
    jNug.put(PATH, parser.getPathParser().toJsonArray(nugget.ledgerPath()));
    jNug.put(WIT, parser.toJsonObject(nugget.firstWitness()));
    
    return jNug;
  }
  
  
  
  public Nugget toNugget(String json) {
    try {
      return toNugget((JSONObject) new JSONParser().parse(json));
    } catch (ParseException px) {
      throw new IllegalArgumentException("malformed json: " + json);
    }
  }
  
  
  
  public Nugget toNugget(JSONObject jObj) {
    
    Path path = parser.getPathParser().toPath((JSONArray) jObj.get(PATH));
    TrailedPath firstWitness = parser.toTrailedPath((JSONObject) jObj.get(WIT));
    
    return new Nugget(path, firstWitness);
  }
  

}
