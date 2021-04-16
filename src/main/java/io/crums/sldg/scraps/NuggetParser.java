/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg.scraps;


import java.util.Objects;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import io.crums.sldg.Path;
import io.crums.sldg.json.JsonEntityParser;

/**
 * Parses and generates JSON for {@linkplain Nugget}s.
 */
public class NuggetParser implements JsonEntityParser<Nugget> {
  
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
  
  
  
  @Override
  @SuppressWarnings("unchecked")
  public JSONObject toJsonObject(Nugget nugget) {
    
    JSONObject jNug = new JSONObject();
    jNug.put(PATH, parser.getPathParser().toJsonObject(nugget.ledgerPath()));
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
  

  @Override
  public Nugget toEntity(JSONObject jObj) {
    return toNugget(jObj);
  }
  
  
  
  public Nugget toNugget(JSONObject jObj) {
    
    Path path = parser.getPathParser().toPath((JSONObject) jObj.get(PATH));
    TrailedPath firstWitness = parser.toTrailedPath((JSONObject) jObj.get(WIT));
    
    return new Nugget(path, firstWitness);
  }
  

}
