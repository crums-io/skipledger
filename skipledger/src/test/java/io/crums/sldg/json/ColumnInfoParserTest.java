/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.json;


import static org.junit.Assert.*;

import org.junit.Test;

import io.crums.sldg.src.ColumnInfo;

/**
 * 
 */
public class ColumnInfoParserTest {

  
  
  @Test
  public void testMinRoundtrip() {
    String name = "Carrots";
    int colNum = 1;
    String desc = null;
    String units = null;
    ColumnInfo expected = new ColumnInfo(name, colNum, desc, units);
    testRoundtrip(expected);
  }
  
  @Test
  public void testRoundtrip02() {
    String name = "Carrots";
    int colNum = 5;
    String desc = "Number of carrots entering (+) or leaving (-1) the warehouse";
    String units = null;
    ColumnInfo expected = new ColumnInfo(name, colNum, desc, units);
    testRoundtrip(expected);
  }
  
  @Test
  public void testRoundtrip03() {
    String name = "Carrots";
    int colNum = 5;
    String desc = null;
    String units = "carrot";
    ColumnInfo expected = new ColumnInfo(name, colNum, desc, units);
    testRoundtrip(expected);
  }
  
  @Test
  public void testRoundtrip04() {
    String name = "Carrots";
    int colNum = 5;
    String desc = "Number of carrots entering (+) or leaving (-1) the warehouse";
    String units = "carrot";
    ColumnInfo expected = new ColumnInfo(name, colNum, desc, units);
    testRoundtrip(expected);
  }
  
  
  
  private void testRoundtrip(ColumnInfo expected) {
    var parser = ColumnInfoParser.INSTANCE;
    String json = parser.toJsonObject(expected).toJSONString();
    var readBack = parser.toEntity(json);
    assertEqual(expected, readBack);
  }
  
  
  public static void assertEqual(ColumnInfo expected, ColumnInfo readBack) {
    assertEquals(expected.getName(), readBack.getName());
    assertEquals(expected.getColumnNumber(), readBack.getColumnNumber());
    assertEquals(expected.getDescription(), readBack.getDescription());
    assertEquals(expected.getUnits(), readBack.getUnits());
  }

}














