/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.json;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.BeforeEach;

import io.crums.testing.SelfAwareTestCase;

import io.crums.util.json.JsonEntityParser;
import io.crums.util.json.JsonPrinter;
import io.crums.util.json.simple.JSONObject;

/**
 * I kept repeating myself. So I created this mix-in type for a JSON parser test.
 * 
 * @param <T> the type whose parser is tested
 */
interface ParserRoundtripTest<T> {
  
  
  
  default void testRoundtrip(T expected) throws Exception {
    var jObj = writeParser().toJsonObject(expected);
    observeJson(jObj, expected);
    var actual = readParser().toEntity(jObj);
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
  
  
  default JsonEntityParser<T> writeParser() throws Exception {
    return parser();
  }
  
  
  default JsonEntityParser<T> readParser() throws Exception {
    return parser();
  }

  /**
   * Returns the type-specific {@code<T>} parser.
   */
  JsonEntityParser<T> parser() throws Exception;
  
  
  /**
   * Concrete implementation.
   * 
   * @param <T> the type tested
   */
  static class Base<T> extends SelfAwareTestCase implements ParserRoundtripTest<T> {
    
    private final JsonEntityParser<T> parser;
    
    protected Object methodLabel;
    
    public Base(JsonEntityParser<T> parser) {
      this.parser = parser;
      assertNotNull(parser, "null parser");
    }
    
    @BeforeEach
    public void setUp() {
      clearPrint();
    }
    
    
    public void clearPrint() {
      methodLabel = null;
    }
    

    @Override
    public JsonEntityParser<T> parser() throws Exception {
      return parser;
    }

    
    @Override
    public void observeJson(JSONObject jObj, T expected) {
      if (methodLabel == null)
        return;
      System.out.println(" -- " + method(methodLabel) + " --");
      JsonPrinter.println(jObj);
    }
    
  }

}
