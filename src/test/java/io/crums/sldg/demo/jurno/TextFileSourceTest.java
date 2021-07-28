/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.demo.jurno;


import static org.junit.Assert.*;

import java.io.File;
import java.io.InputStream;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;

import com.gnahraf.test.IoTestCase;

import org.junit.Test;

import io.crums.io.FileUtils;
import io.crums.sldg.SldgConstants;
import io.crums.sldg.src.ColumnType;
import io.crums.sldg.src.ColumnValue;
import io.crums.sldg.src.SourceRow;
import io.crums.sldg.src.StringValue;
import io.crums.sldg.src.TableSalt;
import io.crums.util.TaskStack;


/**
 * 
 */
public class TextFileSourceTest extends IoTestCase {

  
  @Test
  public void test5_12() {

    final long seed = 33L;
    
    final String filename = "5_12.txt";
    final Object label = new Object() {  };
    
    File file = prepareTextFile(filename, label);
    
    TableSalt shaker = makeTableSalt(seed);
    
    try (TextFileSource textSource = new TextFileSource(file, shaker)) {
      assertEquals(5, textSource.size());
      SourceRow srcRow = textSource.getSourceRow(5);
      List<ColumnValue> cells = srcRow.getColumns();
      assertTrue(cells.size() > 2);
      assertExpected(cells.get(0), "So");
      assertExpected(cells.get(cells.size() - 1), "5.");
      assertEquals(3, textSource.lineNumber(1));
      assertEquals(10, textSource.lineNumber(5));
    }
  }

  
  @Test
  public void test5_12_values() {

    final long seed = 101L;
    final String filename = "5_12.txt";
    
    final Object label = new Object() {  };
    
    File file = prepareTextFile(filename, label);
    
    TableSalt shaker = makeTableSalt(seed);
    
    try (TextFileSource textSource = new TextFileSource(file, shaker)) {
      assertEquals(5, textSource.size());
      assertExpectedLine(textSource.getSourceRow(1), "If I were a line, I'd be this one");
      assertExpectedLine(textSource.getSourceRow(2), "Or this second");
      assertExpectedLine(textSource.getSourceRow(3), "Really..");
      assertExpectedLine(textSource.getSourceRow(4), "anything would be ok");
      assertExpectedLine(textSource.getSourceRow(5), "So this would be row (ledgerable line) 5.");
    }
  }
  
  private void assertExpectedLine(SourceRow srcRow, String line) {
    List<ColumnValue> cells = srcRow.getColumns();
    StringTokenizer words = new StringTokenizer(line);
    for (ColumnValue val : cells) {
      assertTrue(words.hasMoreTokens());
      assertExpected(val, words.nextToken());
    }
  }
  
  private void assertExpected(ColumnValue cell, String word) {
    assertEquals(ColumnType.STRING, cell.getType());
    StringValue stringVal = (StringValue) cell;
    assertEquals(word, stringVal.getString());
  }
  
  
  
  private TableSalt makeTableSalt(long seed) {
    byte[] saltSeed = new byte[SldgConstants.HASH_WIDTH];
    Random random = new Random(seed);
    random.nextBytes(saltSeed);
    return new TableSalt(saltSeed);
  }
  
  
  private File prepareTextFile(String filename, Object label) {
    File dir = getMethodOutputFilepath(label);
    dir.mkdir();
    File file = new File(dir, filename);
    
    try (TaskStack closer = new TaskStack()) {
      InputStream resource = getClass().getResourceAsStream(filename);
      assertNotNull("no such resource: " + filename, resource);
      closer.pushClose(resource);
      
      FileUtils.writeNewFile(file, resource);
    }
    
    return file;
  }

}
