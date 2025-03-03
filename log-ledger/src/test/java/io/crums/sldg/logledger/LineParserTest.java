/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.logledger;


import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

import io.crums.io.Opening;
import io.crums.testing.IoTestCase;
import io.crums.util.Strings;
import io.crums.util.TaskStack;

/**
 * 
 */
public class LineParserTest extends IoTestCase {

  final static String HD = "hd.log";
  final static String HD_MOD_OFFSET = "hd-mod_offset.log";
  final static String HD_MOD_ROW = "hd-mod_row.log";
  
  final static int HD_LINE_COUNT = 16;
  
  
  @Test
  public void testBasic() throws IOException {
    Object label = new Object() { };
    
    File dir = makeTestDir(label);
    File hd = copyResource(dir, HD);
    
    var fc = Opening.READ_ONLY.openChannel(hd);
    assertEquals(0L, fc.position());
    
//    var parser = new LineParser(fc) {
//
//      @Override
//      protected void observeLine(long lineOffset, long lineNo, ByteBuffer line) {
//        System.out.printf("%-10s] %s",
//            "[%d:%d".formatted(lineNo, lineOffset), Strings.utf8String(line));
//      }
//      
//    };
    
    var parser = new LineParser(fc);
    
    parser.parse();
    parser.close();
    assertEquals(HD_LINE_COUNT, parser.lineNo());
    assertEquals(hd.length(), parser.lineEndOffset());
  }
  
  
  @Test
  public void testExpandBuffer() throws IOException {
    Object label = new Object() { };
    
    File dir = makeTestDir(label);
    File hd = copyResource(dir, HD);
    
    var fc = Opening.READ_ONLY.openChannel(hd);
    assertEquals(0L, fc.position());
    
    var parser = new LineParser(fc) {
//      @Override
//      protected void observeLine(long lineOffset, long lineNo, ByteBuffer line) {
//        System.out.printf("%-10s] %s",
//            "[%d:%d".formatted(lineNo, lineOffset), Strings.utf8String(line));
//      }
      @Override
      protected int initBufferSize() {   return 8;   }
    };
    
    parser.parse();
    parser.close();
    assertEquals(HD_LINE_COUNT, parser.lineNo());
    assertEquals(hd.length(), parser.lineEndOffset());
    
  }
  
  
  @Test
  public void testExpandBuffer2() throws IOException {
    Object label = new Object() { };
    
    File dir = makeTestDir(label);
    File hd = copyResource(dir, HD);
    
    var fc = Opening.READ_ONLY.openChannel(hd);
    assertEquals(0L, fc.position());
    
    // un-comment below to see it in action..
    var parser = new LineParser(fc) {
//      @Override
//      protected void observeLine(long lineOffset, long lineNo, ByteBuffer line) {
//        System.out.printf("%-10s] %s",
//            "[%d:%d".formatted(lineNo, lineOffset), Strings.utf8String(line));
//      }
//      @Override
//      ByteBuffer expandAndCopy(ByteBuffer buffer) {
//        var out = super.expandAndCopy(buffer);
//        System.out.printf("expandAndCopy: %s -> %s%n", buffer, out);
//        return out;
//      }
      @Override
      protected int initBufferSize() {   return 3;   }
    };
    
    parser.parse();
    parser.close();
    assertEquals(HD_LINE_COUNT, parser.lineNo());
    assertEquals(hd.length(), parser.lineEndOffset());
    
  }
  
  
  
  
  
  
  
  
  
  File makeTestDir(Object label) {
    File dir = getMethodOutputFilepath(label);
    assertTrue( dir.mkdirs() );
    return dir;
  }
  
  
  
  
  
  
  static File copyResource(File dir, String resource) throws IOException {
    File copy = new File(dir, resource);
    copyResourceToFile(copy, resource);
    return copy;
  }
  
  static void copyResourceToFile(File file, String resource) throws IOException {
    var res = LineParserTest.class.getResourceAsStream(resource);
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
