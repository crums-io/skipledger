/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.json;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 * 
 */
public class JsonUtils {

  private JsonUtils() {  }
  
  
  public static String getString(JSONObject jObj, String name, boolean require) throws JsonParsingException {
    Object value = jObj.get(name);
    if (value == null) {
      if (require)
        throw new JsonParsingException("expected '" + name + "' missing");
      return null;
    }
    if (!(value instanceof String))
      throw new JsonParsingException("'" + name + "' expects a simple string: " + value);
    return value.toString();
  }
  
  
  public static Number getNumber(JSONObject jObj, String name, boolean require) throws JsonParsingException {
    Object value = jObj.get(name);
    if (value == null) {
      if (require)
        throw new JsonParsingException("expected numeral '" + name + "' missing");
      return null;
    }
    try {
      return (Number) value;
    } catch (ClassCastException ccx) {
      throw new JsonParsingException("'" + name + "' expects a numeral: " + value, ccx);
    }
  }
  
  
  public static int getInt(JSONObject jObj, String name) throws JsonParsingException {
    return getNumber(jObj, name, true).intValue();
  }
  
  
  
  public static JSONArray getJsonArray(JSONObject jObj, String name, boolean require) throws JsonParsingException {
    Object value = jObj.get(name);
    if (value == null) {
      if (require)
        throw new JsonParsingException("expected JSON array '" + name + "' missing");
      return null;
    }
    try {
      return (JSONArray) value;
    } catch (ClassCastException ccx) {
      throw new JsonParsingException("'" + name + "' expects a JSON array: " + value, ccx);
    }
  }
  
  
  public static JSONObject getJsonObject(JSONObject jObj, String name, boolean require) throws JsonParsingException {
    Object value = jObj.get(name);
    if (value == null) {
      if (require)
        throw new JsonParsingException("expected JSON object '" + name + "' missing");
      return null;
    }
    try {
      return (JSONObject) value;
    } catch (ClassCastException ccx) {
      throw new JsonParsingException("'" + name + "' expects a JSON object: " + value, ccx);
    }
  }
  
  
  
  @SuppressWarnings("unchecked")
  public static boolean addIfPresent(JSONObject jObj, String name, Object value) {
    if (value == null)
      return false;
    jObj.put(name, value);
    return true;
  }

}
