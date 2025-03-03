/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.logledger;


import static org.junit.jupiter.api.Assertions.*;
import static io.crums.sldg.logledger.LineParserTest.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

import io.crums.io.Opening;
import io.crums.testing.IoTestCase;
import io.crums.util.Strings;

/**
 * 
 */
public class LogParserTest extends IoTestCase {
  
  final static int HD_BLANK_LINE_COUNT = 7;
  final static int HD_POUND_PREFIX_LINE_COUNT = 3;
  final static int HD_POUND_SPACE_PREFIX_LINE_COUNT = 2;
  
  
  
  static class BaseListener implements LogParser.Listener {
    @Override
    public void lineOffsets(long offset, long lineNo) {    }

    @Override
    public void ledgeredLine(
        long rowNo, Grammar grammar, long offset, long lineNo, ByteBuffer line) {   }
    @Override
    public void skippedLine(long offset, long lineNo, ByteBuffer line) {    }
    
  }
  
  
  
  @Test
  public void testVanilla() throws Exception {
    Object label = new Object() { };
    
    var parser = newParser(label, HD, Grammar.DEFAULT);
    parser.parse();
    
    assertEquals(HD_LINE_COUNT, parser.rowNo());
    parser.close();
  }
  
  
  @Test
  public void testVanillaSkipEmptyLines() throws Exception {
    Object label = new Object() { };
    
    var grammar = Grammar.DEFAULT.skipBlankLines(true);
    
    var parser = newParser(label, HD, grammar);
    parser.parse();
    parser.close();
    
    assertEquals(HD_LINE_COUNT - HD_BLANK_LINE_COUNT, parser.rowNo());
  }
  
  

  
  
  @Test
  public void testSkipEmptyLinesAndPoundPrefix() throws Exception {
    Object label = new Object() { };
    
    var commentMatcher = Grammar.prefixMatcher("#");
    var grammar = Grammar.DEFAULT.skipBlankLines(true).commentMatcher(commentMatcher);
    
    var parser = newParser(label, HD, grammar);
    parser.parse();
    parser.close();
    
    assertEquals(
        HD_LINE_COUNT - HD_BLANK_LINE_COUNT - HD_POUND_PREFIX_LINE_COUNT,
        parser.rowNo());
  }
  
  
  
  
  @Test
  public void testSkipEmptyLinesAndPoundSpacePrefix() throws Exception {
    Object label = new Object() { };
    
    var commentMatcher = Grammar.prefixMatcher("# ");
    var grammar = Grammar.DEFAULT.skipBlankLines(true).commentMatcher(commentMatcher);
    
    var parser = newParser(label, HD, grammar);
    parser.parse();
    parser.close();
    
    assertEquals(
        HD_LINE_COUNT - HD_BLANK_LINE_COUNT - HD_POUND_SPACE_PREFIX_LINE_COUNT,
        parser.rowNo());
  }
  
  
  
  @Test
  public void testSingleListener() throws Exception {
    Object label = new Object() { };
    
    var commentMatcher = Grammar.prefixMatcher("# ");
    var grammar = Grammar.DEFAULT.skipBlankLines(true).commentMatcher(commentMatcher);
    
    var parser = newParser(label, HD, grammar);
    
    var out = System.out;
    LogParser.Listener listener = new LogParser.Listener() {

      @Override
      public void lineOffsets(long offset, long lineNo) {
        out.printf("offset %d, line no %d%n", offset, lineNo);
      }

      @Override
      public void ledgeredLine(long rowNo, Grammar grammar, long offset, long lineNo, ByteBuffer line) {
        out.printf("[%d : %d : %d] %s", rowNo, lineNo, offset, Strings.utf8String(line));
      }

      @Override
      public void skippedLine(long offset, long lineNo, ByteBuffer line) {
        out.printf("[skip : %d : %d] %s", lineNo, offset, Strings.utf8String(line));
      }
      
    };
    
    parser.pushListener(listener);
    parser.parse();
    parser.close();
  }
  
  

  @Test
  public void testThreeListeners() throws Exception {
    Object label = new Object() { };
    
    var commentMatcher = Grammar.prefixMatcher("# ");
    var grammar = Grammar.DEFAULT.skipBlankLines(true).commentMatcher(commentMatcher);
    
    var parser = newParser(label, HD, grammar);
    
    var out = System.out;
    
    LogParser.Listener nosy = new BaseListener() {
      @Override public void lineOffsets(long offset, long lineNo) {
        out.printf("%s: offset %d, line no %d%n", this, offset, lineNo);
      }
      @Override public String toString() { return "nosyListener"; }
    };
    
    parser.pushListener(nosy);
    
    LogParser.Listener ledgeList = new BaseListener() {
      @Override public void ledgeredLine(
          long rowNo, Grammar grammar, long offset, long lineNo, ByteBuffer line) {
        out.printf("%s: [%d : %d : %d] %s",
            this, rowNo, lineNo, offset, Strings.utf8String(line));
      }
      @Override public String toString() { return "ledgeListener"; }
    };
    parser.pushListener(ledgeList);
    
    LogParser.Listener commentList = new BaseListener() {
      @Override public void skippedLine(long offset, long lineNo, ByteBuffer line) {
        out.printf("%s: [%d : %d] %s", this, lineNo, offset, Strings.utf8String(line));
      }
      @Override public String toString() { return "skipListener"; }
    };
    parser.pushListener(commentList);
    
    
    out.println(method(label) + ": " + parser.listeners());
    
    
    parser.parse();
    parser.close();
  }
  
  
  
  
  private LogParser newParser(Object label, String resource, Grammar grammar)
      throws IOException {

    File dir = makeTestDir(label);
    File file = copyResource(dir, resource);
    var fc = Opening.READ_ONLY.openChannel(file);
    
    return new LogParser(grammar, fc);
  }
  
  

  
  File makeTestDir(Object label) {
    File dir = getMethodOutputFilepath(label);
    assertTrue( dir.mkdirs() );
    return dir;
  }

}





