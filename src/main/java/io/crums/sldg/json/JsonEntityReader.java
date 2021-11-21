/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.json;


import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * JSON read-interface for an entity.
 * 
 * @param <T> the entity type
 */
public interface JsonEntityReader<T> {

  
  /**
   * Returns the given JSON as the typed instance.
   * 
   * @throws JsonParsingException if the given object is malformed, or breaks the entity's grammar
   */
  T toEntity(JSONObject jObj) throws JsonParsingException;
  
  
  /**
   * Returns the given JSON input as a typed entity.
   * Invokes {@linkplain #toEntity(JSONObject)} after constructing a {@code JSONObject}
   * using the {@code json.simple} library.
   * 
   * @throws JsonParsingException if the given object is malformed, or if the given
   * JSON is not a single object and is in fact an array of objects
   */
  default T toEntity(String json) throws JsonParsingException {
    try {
      return toEntity((JSONObject) new JSONParser().parse(json));
    } catch (ParseException px) {
      throw new JsonParsingException("malformed json: " + json, px);
    } catch (ClassCastException ccx) {
      throw new JsonParsingException(
          "appears to be a JSON array: " + json.substring(0, Math.max(20, json.length())) + "...", ccx);
    }
  }
  

  /**
   * Returns the given JSON input as a typed entity.
   * Invokes {@linkplain #toEntity(JSONObject)} after constructing a {@code JSONObject}
   * using the {@code json.simple} library.
   * 
   * @throws JsonParsingException if the given object is malformed, or if the given
   * JSON is not a single object and is in fact an array of objects
   */
  default T toEntity(Reader reader) throws JsonParsingException {
    try {
      return toEntity((JSONObject) new JSONParser().parse(reader));
    } catch (ParseException px) {
      throw new JsonParsingException("malformed json", px);
    } catch (ClassCastException ccx) {
      throw new JsonParsingException("appears to be a JSON array", ccx);
    } catch (IOException iox) {
      throw new UncheckedIOException(iox);
    }
  }
  
  /**
   * Returns the given JSON array as a typed list.
   * 
   * @param jArray null counts as empty
   * 
   * @return read-only, possibly empty list
   */
  default List<T> toEntityList(JSONArray jArray) throws JsonParsingException {
    int size = jArray == null ? 0 : jArray.size();
    if (size == 0)
      return Collections.emptyList();
    
    ArrayList<T> list = new ArrayList<>(size);
    for (int index = 0; index < size; ++index)
      list.add( toEntity((JSONObject) jArray.get(index)));
    return Collections.unmodifiableList(list);
  }
  
}
