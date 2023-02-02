/*
 * Copyright 2023 Babak Farhang
 */
package io.crums.sldg.logs.text;


import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.function.Predicate;

import org.junit.jupiter.api.Test;

import io.crums.sldg.cache.HashFrontier;
import io.crums.sldg.src.TableSalt;


public class StateHasherTest {

  
  /** Test log resource name. */
  public final static String HD_LOG = "hd.log";
  
  final static int HD_NO_BLANK = 9;
  final static int HD_COMMENT = 3;

  

  @Test
  public void testHd() throws Exception {
    long expectedRows = HD_NO_BLANK;
    var salter = newSalter(10);
    
    var hasher = new StateHasher(salter, null, null);
    HashFrontier state;
    try (var log = getClass().getResourceAsStream(HD_LOG)) {
      state = hasher.play(log);
    }
    assertEquals(expectedRows, state.rowNumber());
  }
  

  @Test
  public void testHdWithSmallBuffer() throws Exception {
    long expectedRows = HD_NO_BLANK;
    var salter = newSalter(10);
    
    var hasher = new StateHasher(salter, null, null);
    HashFrontier expected;
    try (var log = getClass().getResourceAsStream(HD_LOG)) {
      expected = hasher.play(log);
    }
    assertEquals(expectedRows, expected.rowNumber());
    
    hasher = new StateHasher(salter, null, null) {
      @Override
      protected ByteBuffer newLineBuffer() {
        return ByteBuffer.allocate(100);
      }
    };
    
    HashFrontier state;
    try (var log = getClass().getResourceAsStream(HD_LOG)) {
      state = hasher.play(log);
    }
    
    assertEquals(expected, state);
  }
  

  
  
  @Test
  public void testHdSansComment() throws Exception {
    long expectedRows = HD_NO_BLANK - HD_COMMENT;
    Predicate<ByteBuffer> commentFilter = (b) -> b.get(b.position()) == '#';
    var salter = newSalter(11);
    
    var hasher = new StateHasher(salter, commentFilter, null);
    HashFrontier state;
    try (var log = getClass().getResourceAsStream(HD_LOG)) {
      state = hasher.play(log);
    }
    assertEquals(expectedRows, state.rowNumber());
  }
  

  
  
  @Test
  public void testHdSansComment2() throws Exception {
    long expectedRows = HD_NO_BLANK - HD_COMMENT;
    Predicate<ByteBuffer> commentFilter = (b) -> b.get(b.position()) == '#';
    var salter = newSalter(11);
    var tokenDelimiters = ", \t\f\r\n";
    var hasher = new StateHasher(salter, commentFilter, tokenDelimiters);
    HashFrontier state;
    try (var log = getClass().getResourceAsStream(HD_LOG)) {
      state = hasher.play(log);
    }
    assertEquals(expectedRows, state.rowNumber());
  }
  


  static TableSalt newSalter(long seed) {
    byte[] tableSeed = new byte[32];
    new Random(seed).nextBytes(tableSeed);
    return new TableSalt(tableSeed);
  }
  
}
