/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.logs.text;


import static org.junit.jupiter.api.Assertions.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.junit.jupiter.api.Test;

import io.crums.util.Strings;

/**
 * 
 */
public class LogParserTest {
  
  /** Test log resource name. */
  public final static String HD_LOG = "hd.log";

  @Test
  public void testHd() throws Exception {
    var expected = new ArrayList<String>();
    try (var in = getClass().getResourceAsStream(HD_LOG)) {
      var reader = new BufferedReader(new InputStreamReader(in));
      while (true) {
        var line = reader.readLine();
        if (line == null)
          break;
        expected.add(line + "\n");
      }
    }
    
    var actual = new ArrayList<String>();
    var offsets = new ArrayList<Long>();
    LogParser.SourceListener listener = new LogParser.SourceListener() {
      @Override
      public boolean acceptRow(ByteBuffer text, long offset) {
        var str = new String(
            text.array(),
            text.arrayOffset(),
            text.remaining(),
            Strings.UTF_8);
        actual.add(str);
        offsets.add(offset);
        return true;
      }
    };

    try (var in = getClass().getResourceAsStream(HD_LOG)) {
      new LogParser(listener, in).run();
    }
    
    byte[] inBytes;
    try (var in = getClass().getResourceAsStream(HD_LOG)) {
      inBytes = in.readAllBytes();
    }
    
    assertEquals(expected, actual);
    
    for (int index = offsets.size(), endOff = inBytes.length; index-- > 0; ) {
      int offset = offsets.get(index).intValue();
      assertEquals(expected.get(index), new String(inBytes, offset, endOff - offset));
      endOff = offset;
    }
  }
  
  
  

}
