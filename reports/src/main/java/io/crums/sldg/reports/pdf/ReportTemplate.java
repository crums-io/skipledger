/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import com.lowagie.text.Document;
import com.lowagie.text.PageSize;
import com.lowagie.text.Rectangle;
import com.lowagie.text.pdf.PdfWriter;

import io.crums.sldg.reports.pdf.input.Query;
import io.crums.sldg.src.SourceRow;

/**
 * 
 */
public class ReportTemplate {
  
  
  public record Components(
      Header header,
      Optional<TableTemplate> subHeader,
      TableTemplate mainTable,
      Optional<BorderContent> footer) {
    
    /**
     * The {@code header} and {@code mainTable} are required.
     * No null arguments.
     * 
     * @param header    (required)
     * @param subHeader (optional)
     * @param mainTable (required)
     * @param footer    (optional)
     * 
     * @throws NullPointerException if a required argument is {@code null}
     */
    public Components {
      Objects.requireNonNull(header, "null header");
      Objects.requireNonNull(subHeader, "null subHeader opt");
      Objects.requireNonNull(mainTable, "null main table");
      Objects.requireNonNull(footer, "null footer opt");
    }
    
    
    
    public Components(Header header,
      TableTemplate subHeader,
      TableTemplate mainTable,
      BorderContent footer) {
      this(header, Optional.ofNullable(subHeader), mainTable, Optional.ofNullable(footer));
    }
    
  }
  

  public final static float DEFAULT_MARGIN = 24;
  
  
  private final Components components;
  private final Query query;
  

  private Rectangle pageSize = PageSize.A4;
  
  private float marginLeft = DEFAULT_MARGIN;
  
  private float marginRight = DEFAULT_MARGIN;
  
  private float marginTop = DEFAULT_MARGIN;
  
  private float marginBottom = DEFAULT_MARGIN;
  
  public ReportTemplate(Components components) {
    this(components, null);
  }
  
  public ReportTemplate(Components components, Query query) {
    this.components = Objects.requireNonNull(components, "null components");
    this.query = query;
  }
  
  
  
  
  
  public void writePdf(File file, List<SourceRow> rowset) throws UncheckedIOException {
    Objects.requireNonNull(file, "null file");
    Objects.requireNonNull(rowset, "null rowset");
    if (rowset.isEmpty())
      throw new IllegalArgumentException("empty rowset");
    if (file.exists())
      throw new IllegalArgumentException("cannot overwrite file: " + file);
    try {
      writePdfFile(file, rowset);
    } catch (IOException iox) {
      throw new UncheckedIOException(iox);
    }
  }
  
  
  protected void writePdfFile(File file, List<SourceRow> rowset) throws IOException {
    var document = new Document(pageSize, marginLeft, marginRight, marginTop, marginBottom);
    
    // Delay this step.. we're creating a file even if we encounter errors. Not good (!)
//    PdfWriter.getInstance(
//        document,
//        new FileOutputStream(file));
    
    // what's this? Can I add meta this way? TODO
//    document.addHeader("", "");
    
    
    // create the PDF tables before writing anymore to the file
    // (avoid more file I/O if an error is to occur)
    var headTable = components.header().headerTable().createTable(rowset);
    var subHeadTable = components.subHeader().map(subHeader -> subHeader.createTable(rowset));
    var mainTable = components.mainTable().createTable(rowset);

    PdfWriter.getInstance(
        document,
        new FileOutputStream(file));

    components.header().headContent().ifPresent(
        border -> document.setHeader(border.newHeaderFooter()));
    components.footer().ifPresent(
        footer -> document.setFooter(footer.newHeaderFooter()));
    
    // open file for writing
    document.open();
    document.add(headTable);
    subHeadTable.ifPresent(document::add);
    document.add(mainTable);
    document.close();
  }

  
  
  
  
  //         PROPERTY SETTERS / GETTERS
  
  
  public final Components getComponents() {
    return components;
  }
  
  
  public final float getPageWidth() {
    return pageSize.getWidth();
  }
  
  
  public final float getPageHeight() {
    return pageSize.getHeight();
  }
  
  
  public void setPageSize(float width, float height) {
    if (width <= marginLeft + marginRight || height <= marginTop + marginBottom)
      throw new IllegalArgumentException("illegal page size (" + width + ":" + height + ")");
    this.pageSize = new Rectangle(width, height);
  }


  public final float getMarginLeft() {
    return marginLeft;
  }


  public final float getMarginRight() {
    return marginRight;
  }


  public final float getMarginTop() {
    return marginTop;
  }


  public final float getMarginBottom() {
    return marginBottom;
  }
  
  
  public final boolean sameMargins() {
    return
        marginLeft == marginRight &&
        marginLeft == marginTop &&
        marginLeft == marginBottom;
  }
  
  
  public void setMargins(float margin) {
    setMarginLeft(margin);
    setMarginRight(margin);
    setMarginTop(margin);
    setMarginBottom(margin);
  }


  public void setMarginLeft(float margin) {
    checkMargin(margin);
    this.marginLeft = margin;
  }


  public void setMarginRight(float margin) {
    checkMargin(margin);
    this.marginRight = margin;
  }


  public void setMarginTop(float margin) {
    checkMargin(margin);
    this.marginTop = margin;
  }


  public void setMarginBottom(float margin) {
    checkMargin(margin);
    this.marginBottom = margin;
  }
  

  private void checkMargin(float margin) {
    if (margin < 0)
      throw new IllegalArgumentException("margin: " + margin);
  }

}



















