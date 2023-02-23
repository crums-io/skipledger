/*
 * Copyright 2023 Babak Farhang
 */
package io.crums.sldg.logs.text;


import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.Predicate;

import org.junit.jupiter.api.Test;

import com.gnahraf.test.IoTestCase;

import io.crums.io.Opening;
import io.crums.sldg.Path;
import io.crums.sldg.Row;
import io.crums.sldg.cache.HashFrontier;
import io.crums.sldg.src.ColumnValue;
import io.crums.sldg.src.StringValue;
import io.crums.sldg.src.TableSalt;
import io.crums.util.Lists;
import io.crums.util.TaskStack;


public class StateHasherTest extends IoTestCase {

  
  /** Test log resource name. */
  public final static String HD_LOG = "hd.log";
  
  final static int HD_NO_BLANK = 9;
  final static int HD_COMMENT = 3;
  final static int HD_SANS_COMMENT = HD_NO_BLANK - HD_COMMENT;
  
  final static Predicate<ByteBuffer> COMMENT_TEST =
      (b) -> b.get(b.position()) == '#';

  

  @Test
  public void testHd() throws Exception {
    long expectedRows = HD_NO_BLANK;
    var salter = newSalter(10);
    
    var hasher = new StateHasher(salter, null, null);
    HashFrontier state;
    try (var log = getClass().getResourceAsStream(HD_LOG)) {
      state = hasher.play(log).frontier();
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
      expected = hasher.play(log).frontier();
    }
    assertEquals(expectedRows, expected.rowNumber());
    
    hasher = new StateHasher(salter, null, null) {
      @Override
      protected int lineBufferSize() {
        return 100;
      }
    };
    
    HashFrontier state;
    try (var log = getClass().getResourceAsStream(HD_LOG)) {
      state = hasher.play(log).frontier();
    }
    
    assertEquals(expected, state);
  }
  

  
  
  @Test
  public void testHdSansComment() throws Exception {
    long expectedRows = HD_NO_BLANK - HD_COMMENT;
    var salter = newSalter(11);
    
    var hasher = new StateHasher(salter, COMMENT_TEST, null);
    HashFrontier state;
    try (var log = getClass().getResourceAsStream(HD_LOG)) {
      state = hasher.play(log).frontier();
    }
    assertEquals(expectedRows, state.rowNumber());
  }
  

  
  
  @Test
  public void testHdSansComment2() throws Exception {
    long expectedRows = HD_NO_BLANK - HD_COMMENT;
    var salter = newSalter(11);
    var tokenDelimiters = ", \t\f\r\n";
    var hasher = new StateHasher(salter, COMMENT_TEST, tokenDelimiters);
    HashFrontier state;
    try (var log = getClass().getResourceAsStream(HD_LOG)) {
      state = hasher.play(log).frontier();
    }
    assertEquals(expectedRows, state.rowNumber());
  }
  

  @Test
  public void testGetFullRow() throws Exception {
    long rn = 4;
    String expectedLine = "Sat on a wall and took a great fall";
    var salter = newSalter(17);
    var hasher = new StateHasher(salter, COMMENT_TEST, null);
    
    FullRow row;
    try (var log = getClass().getResourceAsStream(HD_LOG)) {
      row = hasher.getFullRow(
          rn, State.EMPTY, Channels.newChannel(log));
    }
    
    assertEquals(rn, row.rowNumber());
    assertEquals(expectedLine, asText(row.columns()));
  }
  
  
  
  @Test
  public void testGetRows() throws Exception {
    Object label = new Object() { };
    long[] rns = { 1L, 2L, 3L, 8L, 9L};
    
    var salter = newSalter(17);
    var hasher = new StateHasher(salter);
    
    File dir = getMethodOutputFilepath(label);
    dir.mkdirs();
    assert dir.isDirectory();
    File logFile = copyResource(dir, HD_LOG);
    List<Row> rows;
    try (var log = Opening.READ_ONLY.openChannel(logFile)) {
      rows = hasher.getRows(Lists.longList(rns), log);
    }
    assertEquals(rns.length, rows.size());
    for (int index = 0; index < rns.length; ++index)
      assertEquals(rns[index], rows.get(index).rowNumber());
  }
  
  
  @Test
  public void testGetPath() throws Exception {
    Object label = new Object() { };
    
    var salter = newSalter(18);
    var hasher = new StateHasher(salter);
    
    File dir = getMethodOutputFilepath(label);
    dir.mkdirs();
    assert dir.isDirectory();
    File logFile = copyResource(dir, HD_LOG);
    
    try (var log = Opening.READ_ONLY.openChannel(logFile)) {
      hasher.getPath(1, 9, log);
    }
    
  }
  


  static TableSalt newSalter(long seed) {
    byte[] tableSeed = new byte[32];
    new Random(seed).nextBytes(tableSeed);
    return new TableSalt(tableSeed);
  }
  
  
  static String asText(List<ColumnValue> cols) {
    var line = new StringBuilder();
    for (var col : cols) {
      line.append(((StringValue) col).getString()).append(' ');
    }
    line.setLength(line.length() - 1);
    return line.toString();
  }
  
  static File copyResource(File dir, String resource) throws IOException {
    File copy = new File(dir, resource);
    copyResourceToFile(copy, resource);
    return copy;
  }
  
  static void copyResourceToFile(File file, String resource) throws IOException {
    var res = StateHasherTest.class.getResourceAsStream(resource);
    copyToFile(file, res);
  }
  
  static void copyToFile(File file, InputStream res) throws IOException {
    try (TaskStack closer = new TaskStack()) {
      closer.pushClose(res);
      var fstream = new FileOutputStream(file);
      closer.pushClose(fstream);
      byte[] buffer = new byte[4096];
      while (true) {
        int len = res.read(buffer);
        if (len == -1)
          break;
        fstream.write(buffer, 0, len);
      }
    }
  }
  
}
