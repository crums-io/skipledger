/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg.json;


import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * Emergent pattern for implementing parsing and generating JSON,
 * abstracted into an interface.
 * 
 * @param <T> the entity type
 */
public interface JsonEntityParser<T> {
  
  
  /**
   * Returns the given <tt>entity</tt> 
   * @param entity
   * @return
   */
  JSONObject toJsonObject(T entity);
  
  /**
   * Returns the given JSON as the typed instance.
   */
  T toEntity(JSONObject jObj);
  
  default T toEntity(String json) {
    try {
      return toEntity((JSONObject) new JSONParser().parse(json));
    } catch (ParseException px) {
      throw new IllegalArgumentException("malformed json: " + json);
    } catch (ClassCastException ccx) {
      throw new IllegalArgumentException(
          "appears to be a JSON array: " + json.substring(0, Math.max(20, json.length())) + "...");
    }
  }
  
  
  default T toEntity(Reader reader) {
    try {
      return toEntity((JSONObject) new JSONParser().parse(reader));
    } catch (ParseException px) {
      throw new IllegalArgumentException("malformed json");
    } catch (ClassCastException ccx) {
      throw new IllegalArgumentException("appears to be a JSON array");
    } catch (IOException iox) {
      throw new UncheckedIOException(iox);
    }
  }

}
