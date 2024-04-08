/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.json;


import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Random;

import org.junit.jupiter.api.Test;

import io.crums.testing.SelfAwareTestCase;

import io.crums.sldg.src.BytesValue;
import io.crums.sldg.src.ColumnValue;
import io.crums.sldg.src.DateValue;
import io.crums.sldg.src.DoubleValue;
import io.crums.sldg.src.HashValue;
import io.crums.sldg.src.LongValue;
import io.crums.sldg.src.NullValue;
import io.crums.sldg.src.SourceRow;
import io.crums.sldg.src.StringValue;
import io.crums.util.json.JsonPrinter;

/**
 * 
 */
public class SourceRowParserTest extends SelfAwareTestCase {

  @Test
  public void testRoundtrip00() {
    
//    final Object label = new Object() { };
    long rn = 4034592704119995L;
    Random rand = new Random(1);
    
    ColumnValue[] columns = {
        new NullValue(randomHash(rand)),
    };
    
    SourceRow srcRow = new SourceRow(rn, columns);
    var parser = new SourceRowParser();
    testRoundtrip(srcRow, parser, null);
  }

  @Test
  public void testRoundtrip01() {
    
//    final Object label = new Object() { };
    long rn = 4034592704119995L;
    Random rand = new Random(1);
    
    ColumnValue[] columns = {
        new LongValue(77, randomHash(rand)),
        new DateValue(2525, randomHash(rand)),
        new NullValue(randomHash(rand)),
        new StringValue("if there was a programmer alive", randomHash(rand)),
    };
    
    SourceRow srcRow = new SourceRow(rn, columns);
    var parser = new SourceRowParser();
    testRoundtrip(srcRow, parser, null);
  }

  @Test
  public void testRoundtrip02() {
    
    final Object label = new Object() { };
    long rn = 4034592704119008L;
    Random rand = new Random(2);
    
    ColumnValue[] columns = {
        new LongValue(64, randomHash(rand)),
        new DateValue(2525, randomHash(rand)),
        new NullValue(randomHash(rand)),
        new StringValue("when I'm 64", randomHash(rand)),
        new HashValue(randomHash(rand)),
        new DoubleValue(1.0091, randomHash(rand)),
        new BytesValue(randomBytes(rand, 48), randomHash(rand)),
    };
    
    SourceRow srcRow = new SourceRow(rn, columns);
    var parser = new SourceRowParser(new SimpleDateFormat("yyyy-MM-dd 'T' HH:mm XXX"));
    testRoundtrip(srcRow, parser, label);
    
    System.out.println();
    System.out.println(" - slim version - (values only)");
    System.out.println();
    var jObj = parser.toSlimJsonObject(srcRow);
    System.out.println(JsonPrinter.toJson(jObj));
  }
  
  
  
  
  
  private void testRoundtrip(SourceRow srcRow, SourceRowParser parser, Object label) {
    boolean print = label != null;
    if (print) {
      System.out.println();
      System.out.println("==== " + method(label) + " ====");
    }
    var jObj = parser.toJsonObject(srcRow);
    var json = JsonPrinter.toJson(jObj);
    if (print)
      System.out.println(json);
    var retRow = parser.toEntity(json);
    assertEquals(srcRow, retRow);
  }
  
  
  private ByteBuffer randomHash(Random rand) {
    return randomBytes(rand, 32);
  }
  
  
  private ByteBuffer randomBytes(Random rand, int size) {
    byte[] b = new byte[size];
    rand.nextBytes(b);
    return ByteBuffer.wrap(b);
  }

}
