/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.bindle;


import static io.crums.sldg.bindle.BindleBuilderHumptyTest.addReferences;
import static io.crums.sldg.bindle.BindleBuilderHumptyTest.mockWitnessLogs;
import static io.crums.sldg.bindle.BindleBuilderHumptyTest.prepare;
import static io.crums.sldg.bindle.NuggetBuilderDumptyTest.assertNugget;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;

import org.junit.jupiter.api.Test;

import io.crums.testing.IoTestCase;

/**
 * 
 */
public class BindleFileHumptyTest extends IoTestCase {
  
  
  @Test
  public void testBasic() throws Exception {
    Object label = new Object() { };
    File dir = getMethodOutputFilepath(label);
    var builder = prepare(this, label, false);
    assertRoundtripHumpty(dir, builder);
  }
  
  
  @Test
  public void testWithReferences() throws Exception {
    Object label = new Object() { };
    File dir = getMethodOutputFilepath(label);
    var builder = prepare(this, label, false);
    addReferences(builder, false);
    assertRoundtripHumpty(dir, builder);
  }
  
  
  @Test
  public void testNotarized() throws Exception {
    Object label = new Object() { };
    File dir = getMethodOutputFilepath(label);
    var builder = prepare(this, label, false);
    mockWitnessLogs(builder, false);
    assertRoundtripHumpty(dir, builder);
  }
  
  
  @Test
  public void testWithReferencesAndNotarized() throws Exception {
    Object label = new Object() { };
    File dir = getMethodOutputFilepath(label);
    var builder = prepare(this, label, false);
    mockWitnessLogs(builder, false);
    addReferences(builder, false);
    assertRoundtripHumpty(dir, builder);
    
  }
  
  
  
  
  BindleFile assertRoundtripHumpty(File dir, Bindle bindle) {
    assertTrue(dir.isDirectory());
    File bindleFilepath = new File(dir, "humpty" + BindleConstants.FILENAME_EXT);
    
    BindleFile.create(bindle, bindleFilepath);
    var bindleFile = BindleFile.load(bindleFilepath);
    assertEquals(bindle.ids(), bindleFile.ids());
    for (var id : bindle.ids())
      assertNugget(bindle.getNugget(id), bindleFile.getNugget(id));
    return bindleFile;
  }
  
  
  
  
  
  
  
  

}
