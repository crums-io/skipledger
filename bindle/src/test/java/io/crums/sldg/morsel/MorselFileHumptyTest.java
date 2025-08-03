/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.morsel;


import static org.junit.jupiter.api.Assertions.*;

import java.io.File;

import static io.crums.sldg.morsel.MorselBuilderHumptyTest.*;
import static io.crums.sldg.morsel.NuggetBuilderDumptyTest.*;

import org.junit.jupiter.api.Test;

import io.crums.testing.IoTestCase;

/**
 * 
 */
public class MorselFileHumptyTest extends IoTestCase {
  
  
  @Test
  public void testBasic() throws Exception {
    Object label = new Object() { };
    File dir = getMethodOutputFilepath(label);
    var builder = prepare(this, label, false);
    assertTrue(dir.isDirectory());
    File morselFilepath = new File(dir, "humpty" + MorselConstants.FILENAME_EXT);
    
    MorselFile.create(builder, morselFilepath);
    var morselFile = MorselFile.load(morselFilepath);
    
  }
  
  
  
  
  
  
  
  

}
