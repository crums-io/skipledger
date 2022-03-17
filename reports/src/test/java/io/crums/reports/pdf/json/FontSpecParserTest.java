/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.reports.pdf.json;


import java.awt.Color;

import org.junit.jupiter.api.Test;

import com.lowagie.text.Font;

import io.crums.reports.pdf.FontSpec;
import io.crums.util.json.JsonEntityParser;

/**
 * 
 */
public class FontSpecParserTest implements ParserRoundtripTest<FontSpec> {
  

  @Test
  public void testHelvetica() throws Exception {
    testRoundtrip("Helvetica", 10, Font.NORMAL, Color.GRAY);
  }
  
  
  @Test
  public void testHelveticaBold() throws Exception {
    testRoundtrip("Helvetica", 10, Font.BOLD, Color.BLACK);
  }
  
  
  @Test
  public void testHelveticaItalic() throws Exception {
    testRoundtrip("Helvetica", 10, Font.ITALIC, Color.BLACK);
  }
  
  
  private void testRoundtrip(String fontName, float size, int style, Color color) throws Exception {
    testRoundtrip(new FontSpec(fontName, size, style, color));
  }
  
  

  @Override
  public JsonEntityParser<FontSpec> parser() {
    return FontSpecParser.INSTANCE;
  }

}
