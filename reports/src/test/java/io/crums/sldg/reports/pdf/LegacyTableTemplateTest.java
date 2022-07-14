/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf;


import static org.junit.jupiter.api.Assertions.*;

import java.awt.Color;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import org.junit.jupiter.api.Test;
import com.gnahraf.test.IoTestCase;
import com.lowagie.text.Document;
import com.lowagie.text.Font;
import com.lowagie.text.PageSize;
import com.lowagie.text.pdf.PdfPTable;
import com.lowagie.text.pdf.PdfWriter;
import io.crums.sldg.reports.pdf.json.LegacyTableTemplateParser;
import io.crums.util.Lists;

/**
 * Careful with changing test method names: its name might implicitly
 * match the name of the resource used for that test.
 */
public class LegacyTableTemplateTest extends IoTestCase {
  
  private File dir;
  
  private File init(Object methodLabel) {
    dir = getMethodOutputFilepath(methodLabel);
    dir.mkdirs();
    
    return dir;
  }
  

  
  @Test
  public void simple() throws IOException {

    Object label = new Object() { };
    File file = newPdfFilepath(label);
    
    FontSpec font = new FontSpec("Helvetica", 10, Font.BOLD, Color.BLACK);
    
    var col = new CellFormat(font);
    col.setLeading(1.5f);
    col.scalePaddingToFont(1.5f);
    
    var tableTemplate = new LegacyTableTemplate(List.of(col, col));
    
    tableTemplate.setDefaultLines(new LineSpec(1, Color.BLACK));
    
    tableTemplate.setTableBorders(new LineSpec(3, Color.BLACK));
    
    String[] inputs = {
        "2 b or not",
        "is that a question?"
    };

    var table = tableTemplate.createTable(Lists.map(List.of(inputs), s -> CellData.forText(s)));
//    var table = tableTemplate.createTableOld(List.of(inputs));
    table.setWidthPercentage(100f);
    
    var document = new Document(PageSize.A4, 50, 50, 50, 50);
    PdfWriter.getInstance(
        document,
        new FileOutputStream(file));
    document.open();
    
    document.add(table);
    document.close();
  }
  

  @Test
  public void tt_c2() throws IOException {
    Object label = new Object() { };
    
    generatePdf(label, 7);
    
  }
  

  @Test
  public void test2UniqueCols80x20() throws IOException {
    Object label = new Object() { };
    generatePdf(label, 7);
  }
  
  
  private void generatePdf(Object methodLabel, int rows) throws IOException {
    var tableTemplate = loadFromResource(methodLabel);
    String[] inputs = testInputs(rows, tableTemplate.getColumnCount());
    var file = newPdfFilepath(methodLabel);
    writePdf(file, tableTemplate, inputs);
    
  }
  
  private void writePdf(File file, LegacyTableTemplate table, String[] inputs) throws IOException {
    List<CellData> cells = Lists.map(List.of(inputs), s -> CellData.forText(s));
    var pdfTable = table.createTable(cells);
    writePdf(pdfTable, file);
  }
  
  
  private void writePdf(PdfPTable pdfTable, File file) throws IOException {
    pdfTable.setWidthPercentage(100f);
    var document = new Document(PageSize.A4, 50, 50, 50, 50);
    PdfWriter.getInstance(
        document,
        new FileOutputStream(file));
    document.open();
    document.add(pdfTable);
    document.close();
  }
  
  private String[] testInputs(int rows, int cols) {
    String[] inputs = new String[rows * cols];
    
    for (int rowNum, index = rowNum = 0; index < inputs.length;) {
      ++rowNum;
      for (int colNum = 1; colNum <= cols; ++colNum) {
        inputs[index++] = "[ " + rowNum + " , " + colNum + " ]";
      }
    }
    return inputs;
  }
  
  
  
  private LegacyTableTemplate loadFromResource(Object methodLabel) {
    return loadFromResource(method(methodLabel) + ".json");
  }
  
  private LegacyTableTemplate loadFromResource(String resourceName) {
    var in = getClass().getResourceAsStream(resourceName);
    assertNotNull(in, resourceName);
    try (var reader = new InputStreamReader(in)) {
      return LegacyTableTemplateParser.INSTANCE.toEntity(reader);
    } catch (IOException iox) {
      fail();
      // never reached
      return null;
    }
  }
  
  
  private File newPdfFilepath(Object methodLabel) {
    if (dir == null)
      init(methodLabel);
    return new File(dir, method(methodLabel) + ".pdf");
  }

}







