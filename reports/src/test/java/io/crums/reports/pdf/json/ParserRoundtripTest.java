/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.reports.pdf.json;


import static org.junit.jupiter.api.Assertions.*;

import io.crums.util.json.JsonEntityParser;
import io.crums.util.json.simple.JSONObject;

/**
 * I kept repeating myself. So I created this mix-in type for a JSON parser test.
 */
interface ParserRoundtripTest<T> {
  
  
  
  default void testRoundtrip(T expected) throws Exception {
    var parser = parser();
    var jObj = parser.toJsonObject(expected);
    observeJson(jObj, expected);
    var actual = parser.toEntity(jObj);
    assertTripEquals(expected, actual);
  }
  
  
  
  /**
   * Hook for observing parser output. Defaults to noop.
   */
  default void observeJson(JSONObject jObj, T expected) {  }
  
  /**
   * Default assertion based on object equality.
   * 
   * @param expected
   * @param actual
   */
  default void assertTripEquals(T expected, T actual) {
    assertEquals(expected, actual);
  }

  /**
   * Returns the type-specific {@code<T>} parser.
   */
  JsonEntityParser<T> parser() throws Exception;

}
