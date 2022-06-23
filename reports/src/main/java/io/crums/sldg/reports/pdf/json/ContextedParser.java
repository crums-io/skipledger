/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.json;

import java.util.ArrayList;
import java.util.List;

import io.crums.util.json.JsonEntityParser;
import io.crums.util.json.JsonParsingException;
import io.crums.util.json.simple.JSONArray;
import io.crums.util.json.simple.JSONObject;

/**
 * A {@linkplain JsonEntityParser} with a look-up table for symbolic links
 * ({@linkplain RefContext}). The general idea here is that some RHS values
 * in JSON may reference values defined in a dictionary. The dictionary itself
 * is defined in some part of the larger JSON structure.
 * 
 * <h3>Motivation</h3>
 * <p>
 * Keep the {@linkplain JsonEntityParser} pattern, but overload each method with
 * a <em>{@code context}</em> parameter: for closure, the super interface methods
 * in {@code JsonEntityParser} methods delegate to their overloaded versions using
 * the {@linkplain #defaultContext()} method.
 * </p>
 * 
 * @see RefContext
 */
public interface ContextedParser<T> extends JsonEntityParser<T> {
  
  @Override
  default T toEntity(JSONObject jObj) throws JsonParsingException {
    return toEntity(jObj, defaultContext());
  }
  
  
  T toEntity(JSONObject jObj, RefContext context) throws JsonParsingException;
  

  @Override
  default List<T> toEntityList(JSONArray jArray) throws JsonParsingException {
    return toEntityList(jArray, defaultContext());
  }
  
  
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
  

  @Override
  default JSONObject injectEntity(T entity, JSONObject jObj) {
    return injectEntity(entity, jObj, defaultContext());
  }
  
  JSONObject injectEntity(T entity, JSONObject jObj, RefContext context);
  
  
  default JSONArray toJsonArray(List<T> entities, RefContext context) {
    var jArray = new JSONArray(entities.size());
    entities.forEach(e -> jArray.add(toJsonObject(e, context)));
    return jArray;
  }
  
  
  /**
   * Returns the instance's default ref context.
   * 
   * @return default impl is empty and immutable ({@code RefContext.EMPTY})
   */
  default RefContext defaultContext() {
    return RefContext.EMPTY;
  }

}





