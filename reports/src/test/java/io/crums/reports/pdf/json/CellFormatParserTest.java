/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.reports.pdf.json;


import java.awt.Color;

import org.junit.jupiter.api.Test;

import com.lowagie.text.Font;

import io.crums.reports.pdf.Align;
import io.crums.reports.pdf.CellFormat;
import io.crums.reports.pdf.FontSpec;
import io.crums.util.json.JsonEntityParser;

/**
 * 
 */
public class CellFormatParserTest implements ParserRoundtripTest<CellFormat> {
  
  

  @Test
  public void testSimple() throws Exception {
    var font = new FontSpec("Helvetica", 8, Font.BOLD, Color.BLACK);
    var column = new CellFormat(font);
    testRoundtrip(column);
  }
  
  
  @Test
  public void testMore() throws Exception {
    var font = new FontSpec("Helvetica", 8, Font.ITALIC, Color.CYAN);
    var column = new CellFormat(font);
    column.setAlignV(Align.V.BOTTOM);
    column.setAlignH(Align.H.CENTER);
    column.setLeading(1.5f);
    column.scalePaddingToFont(1.4f);
    testRoundtrip(column);
  }
  
  

  @Override
  public JsonEntityParser<CellFormat> parser() {
    return CellFormatParser.INSTANCE;
  }
  

}











