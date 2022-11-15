/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.logs.text;


import static org.junit.jupiter.api.Assertions.*;

import java.util.Random;
import java.util.function.Predicate;

import org.junit.jupiter.api.Test;

import io.crums.sldg.src.TableSalt;

/**
 * 
 */
public class LogHasherTest {
  
  final static int HD_NO_BLANK = 9;
  final static int HD_COMMENT = 3;
  
  @Test
  public void testHd() throws Exception {
    long expectedRows = HD_NO_BLANK;
    var salter = newSalter(10);
    
    var logHasher = new LogHasher(new RowTokenizer(), salter);
    try (var in = getClass().getResourceAsStream(LogParserTest.HD_LOG)) {
      logHasher.appendLog(in);
    }
    assertEquals(expectedRows, logHasher.frontier().rowNumber());
  }
  
  
  @Test
  public void testHdSansComment() throws Exception {
    long expectedRows = HD_NO_BLANK - HD_COMMENT;
    Predicate<String> commentFilter = (s) -> !s.startsWith("#");
    var salter = newSalter(11);
    var rowParser = new FilteredRowParser(commentFilter, new RowTokenizer());
    var logHasher = new LogHasher(rowParser, salter);
    try (var in = getClass().getResourceAsStream(LogParserTest.HD_LOG)) {
      logHasher.appendLog(in);
    }
    assertEquals(expectedRows, logHasher.frontier().rowNumber());
  }
  
  
  
  private TableSalt newSalter(long seed) {
    byte[] tableSeed = new byte[32];
    new Random(seed).nextBytes(tableSeed);
    return new TableSalt(tableSeed);
  }

}


