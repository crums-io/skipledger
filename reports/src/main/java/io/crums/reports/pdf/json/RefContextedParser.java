/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.reports.pdf.json;

import java.util.ArrayList;
import java.util.List;

import io.crums.util.json.JsonEntityParser;
import io.crums.util.json.JsonParsingException;
import io.crums.util.json.simple.JSONArray;
import io.crums.util.json.simple.JSONObject;

/**
 * 
 */
public interface RefContextedParser<T> extends JsonEntityParser<T> {
  
  default T toEntity(JSONObject jObj) throws JsonParsingException {
    return toEntity(jObj, RefContext.EMPTY);
  }
  
  
  T toEntity(JSONObject jObj, RefContext context) throws JsonParsingException;
  
  
  default List<T> toEntityList(JSONArray jArray, RefContext context) throws JsonParsingException {
    var list = new ArrayList<T>(jArray.size());
    try {
      jArray.forEach(o -> list.add(toEntity((JSONObject) o, context)));
    } catch (ClassCastException ccx) {
      throw new JsonParsingException("on parsing " + jArray + ": " + ccx.getMessage(), ccx);
    }
    return list;
  }
  
  
  
  default JSONObject toJsonObject(T entity, RefContext context) {
    return injectEntity(entity, new JSONObject(), context);
  }
  
  
  default JSONObject injectEntity(T entity, JSONObject jObj) {
    return injectEntity(entity, jObj, RefContext.EMPTY);
  }
  
  JSONObject injectEntity(T entity, JSONObject jObj, RefContext context);
  
  
  default JSONArray toJsonArray(List<T> entities, RefContext context) {
    var jArray = new JSONArray(entities.size());
    entities.forEach(e -> jArray.add(toJsonObject(e, context)));
    return jArray;
  }
  
  

}





