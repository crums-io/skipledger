/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.bindle;


import static org.junit.jupiter.api.Assertions.*;


import org.junit.jupiter.api.Test;

import static io.crums.sldg.bindle.BindleBuilderHumptyTest.*;
import static io.crums.sldg.bindle.NuggetBuilderDumptyTest.assertNugget;

import io.crums.testing.IoTestCase;

/**
 * Serial interface test based on Humpty test case.
 */
public class BundleTest extends IoTestCase {
  
  
  
  static Bundle assertRoundtrip(Bundle bun) {
    
    var buffer = bun.serialize();
    assertEquals(buffer.remaining(), bun.serialSize());
    LazyBun copy = LazyBun.load(buffer);
    assertBindle(bun, copy);
    return copy;
  }
  
  
  static void assertBindle(Bindle expected, Bindle actual) {
    assertEquals(expected.ids(), actual.ids());
    for (var id : expected.ids())
      assertNugget(expected.getNugget(id), actual.getNugget(id));
  }
  
  
  @Test
  public void testTwoLogsWithSource() throws Exception {
    final Object label = new Object() {  };
    
    BindleBuilder bun = prepare(this, label, false);
    assertRoundtrip(bun);
  }
  
  
  @Test
  public void testTwoLogsNotarized() throws Exception {
    final Object label = new Object() {  };
    
    BindleBuilder bun = prepare(this, label, false);
    mockWitnessLogs(bun, false);
    assertRoundtrip(bun);
  }
  
  
  @Test
  public void testTwoLogsReferenced() throws Exception {
    final Object label = new Object() {  };
    
    BindleBuilder bun = prepare(this, label, false);
    addReferences(bun, false);
    assertRoundtrip(bun);
  }
  
  
  @Test
  public void testTwoLogsNotarizedReferenced() throws Exception {
    final Object label = new Object() {  };
    
    BindleBuilder bun = prepare(this, label, false);
    addReferences(bun, false);
    mockWitnessLogs(bun, false);
    assertRoundtrip(bun);
  }

}










