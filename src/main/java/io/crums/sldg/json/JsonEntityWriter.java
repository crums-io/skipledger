/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.json;

import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 * The JSON entity output interface.
 * 
 * @param <T> the entity type
 */
public interface JsonEntityWriter<T> {
  
  
  /**
   * Returns the given {@code entity} as JSON.
   */
  JSONObject toJsonObject(T entity);
  
  
  /**
   * Returns the given list as a JSON array.
   * 
   * @param list
   * @return
   */
  @SuppressWarnings("unchecked")
  default JSONArray toJsonArray(List<T> list) {
    JSONArray jArray = new JSONArray();
    for (T entity : list)
      jArray.add( toJsonObject(entity) );
    return jArray;
  }

}
