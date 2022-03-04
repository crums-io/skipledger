/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.reports.pdf.json;


import java.awt.Color;

import org.junit.Test;

import io.crums.reports.pdf.LineSpec;
import io.crums.util.json.JsonEntityParser;

/**
 * 
 */
public class LineSpecParserTest implements ParserRoundtripTest<LineSpec> {
  
  @Test
  public void testBlank() throws Exception {
    testRoundtrip(LineSpec.BLANK);
  }
  
  @Test
  public void testMore() throws Exception {
    testRoundtrip(new LineSpec(1.01f, new Color(11, 67, 255)));
  }

  
  
  
  @Override
  public JsonEntityParser<LineSpec> parser() {
    return LineSpecParser.INSTANCE;
  }
  
  
  
  

}
