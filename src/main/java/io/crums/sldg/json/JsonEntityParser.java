/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg.json;


/**
 * Emergent pattern for implementing parsing and generating JSON,
 * abstracted into an interface.
 * 
 * @param <T> the entity type
 */
public interface JsonEntityParser<T> extends JsonEntityWriter<T>, JsonEntityReader<T> {
  

}
