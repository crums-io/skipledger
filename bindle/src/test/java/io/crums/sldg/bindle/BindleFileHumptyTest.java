/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.bindle;


import static io.crums.sldg.bindle.BindleBuilderHumptyTest.*;
import static io.crums.sldg.bindle.NuggetBuilderDumptyTest.*;
import static org.junit.jupiter.api.Assertions.*;

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
    assertTrue(dir.isDirectory());
    File bindleFilepath = new File(dir, "humpty" + BindleConstants.FILENAME_EXT);
    
    BindleFile.create(builder, bindleFilepath);
    var bindleFile = BindleFile.load(bindleFilepath);
    // TODO
  }
  
  
  
  
  
  
  
  

}
