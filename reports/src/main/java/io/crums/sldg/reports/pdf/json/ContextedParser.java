/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.json;

import java.util.ArrayList;
import java.util.List;

import io.crums.util.json.JsonEntityParser;
import io.crums.util.json.JsonParsingException;
import io.crums.util.json.JsonUtils;
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
 * <h4>TODO:</h4>
 * <p>
 * This idea can be generalized with generics: instead of the application specific
 * {@code RefContext}, use a generic type {@code <C>}. (After all, the interface
 * knows nothing about how {@code context} object is used by the implementation.)
 * </p>
 * 
 * @see RefContext
 */
public interface ContextedParser<T> extends JsonEntityParser<T> {
  
  /** @return {@code toEntity(jObj, defaultContext())} */
  @Override
  default T toEntity(JSONObject jObj) throws JsonParsingException {
    return toEntity(jObj, defaultContext());
  }
  
  /** Returns an entity. May use referenced objects in the given {@code context}. */
  T toEntity(JSONObject jObj, RefContext context) throws JsonParsingException;
  

  /**
   * @return {@code toEntityList(jArray, defaultContext())}
   * @see #toEntityList(JSONArray, RefContext)
   */
  @Override
  default List<T> toEntityList(JSONArray jArray) throws JsonParsingException {
    return toEntityList(jArray, defaultContext());
  }
  
  /** Returns the given JSON array as a typed list, using the given {@code context}. */
  default List<T> toEntityList(JSONArray jArray, RefContext context) throws JsonParsingException {
    var list = new ArrayList<T>(jArray.size());
    try {
      jArray.forEach(o -> list.add(toEntity((JSONObject) o, context)));
    } catch (ClassCastException ccx) {
      throw new JsonParsingException("on parsing " + jArray + ": " + ccx.getMessage(), ccx);
    }
    return list;
  }
  
  
  
  /** @return {@code injectEntity(entity, new JSONObject(), context)} */
  default JSONObject toJsonObject(T entity, RefContext context) {
    return injectEntity(entity, new JSONObject(), context);
  }
  

  /**
   * @return {@code injectEntity(entity, jObj, defaultContext())}
   * @see #injectEntity(Object, JSONObject, RefContext)
   */
  @Override
  default JSONObject injectEntity(T entity, JSONObject jObj) {
    return injectEntity(entity, jObj, defaultContext());
  }
  
  
  /**
   * Injects the given entity, optionally minding whether it's referenced.
   * The pattern is <em>not for populating</em> the given {@code context}.
   * 
   * @return {@code jObj}
   */
  JSONObject injectEntity(T entity, JSONObject jObj, RefContext context);
  
  
  /** Returns a JSON array using {@linkplain #toJsonObject(Object, RefContext)}. */
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


  /**
   * Returns a named instance, if present; {@code null} otherwise.
   * 
   * @return may be null (!)
   */
  default T parseIfPresent(JSONObject jObj, String name, RefContext context) throws JsonParsingException {
    var jSub = JsonUtils.getJsonObject(jObj, name, false);
    return jSub == null ? null : toEntity(jSub, context);
  }

}





