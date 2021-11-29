/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.json;

import java.util.List;

import io.crums.util.json.simple.JSONArray;
import io.crums.util.json.simple.JSONObject;

/**
 * The JSON entity output interface.
 * 
 * @param <T> the entity type
 */
public interface JsonEntityWriter<T> {
  
  
  /**
   * Returns the given {@code entity} as JSON.
   * 
   * @return {@code injectEntity(entity, new JSONObject())}
   */
  default JSONObject toJsonObject(T entity) {
    return injectEntity(entity, new JSONObject());
  }
  
  
  /**
   * Injects the given {@code entity}'s fields into the
   * given {@code JSONObject}.
   * 
   * @param entity  not null
   * @param jObj    not null
   * 
   * @return the given {@code jObj}
   */
  JSONObject injectEntity(T entity, JSONObject jObj);
  
  
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
