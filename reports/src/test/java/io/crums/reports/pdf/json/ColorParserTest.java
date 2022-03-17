/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.reports.pdf.json;


import static org.junit.jupiter.api.Assertions.*;

import java.awt.Color;

import org.junit.jupiter.api.Test;

import io.crums.util.json.simple.JSONObject;

/**
 * 
 */
public class ColorParserTest {
  
  @Test
  public void testRGB_gray() {
    final int val = 128;
    JSONObject jObj = new JSONObject();
    jObj.put(ColorParser.R, val);
    jObj.put(ColorParser.G, val);
    jObj.put(ColorParser.B, val);
    var color = ColorParser.INSTANCE.toEntity(jObj);
    assertEquals(val, color.getRed());
    assertEquals(val, color.getGreen());
    assertEquals(val, color.getBlue());
  }
  
  @Test
  public void testRGB_defaults() {
    final int val = 127;
    JSONObject jObj = new JSONObject();
    jObj.put(ColorParser.R, val);
    var color = ColorParser.INSTANCE.toEntity(jObj);
    assertEquals(val, color.getRed());
    assertEquals(0, color.getGreen());
    assertEquals(0, color.getBlue());
    

    jObj = new JSONObject();
    jObj.put(ColorParser.G, val);
    color = ColorParser.INSTANCE.toEntity(jObj);
    assertEquals(0, color.getRed());
    assertEquals(val, color.getGreen());
    assertEquals(0, color.getBlue());

    jObj = new JSONObject();
    jObj.put(ColorParser.B, val);
    color = ColorParser.INSTANCE.toEntity(jObj);
    assertEquals(0, color.getRed());
    assertEquals(0, color.getGreen());
    assertEquals(val, color.getBlue());
  }
  
  
  @Test
  public void testRoundtrip() {
    testRoundtrip(5, 7, 11);
  }
  
  
  private void testRoundtrip(int red, int green, int blue) {
    Color expected = new Color(red, green, blue);
    var jObj = ColorParser.INSTANCE.toJsonObject(expected);
    Color actual = ColorParser.INSTANCE.toEntity(jObj);
    assertEquals(expected, actual);
  }

}












