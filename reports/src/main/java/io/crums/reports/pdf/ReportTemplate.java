/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.reports.pdf;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

import com.lowagie.text.Document;
import com.lowagie.text.PageSize;
import com.lowagie.text.Rectangle;
import com.lowagie.text.pdf.PdfWriter;

import io.crums.reports.pdf.json.RefContext;

/**
 * 
 */
public class ReportTemplate {
  
  
  public static boolean equal(ReportTemplate a, ReportTemplate b) {
    return
        a == b ||
        a != null && b != null &&
        a.marginLeft == b.marginLeft &&
        a.marginRight == b.marginRight &&
        a.marginTop == b.marginTop &&
        a.marginBottom == b.marginBottom &&
        a.header.equals(b.header) &&
        Objects.equals(a.footer, b.footer) &&
        TableTemplate.equal(a.mainTable, b.mainTable);
        
  }
  
  public final static float DEFAULT_MARGIN = 24;
  
  
  
  
  // BEGIN INSTANCE STUFF..
  
  
  private final Header header;
  
  private final TableTemplate mainTable;
  
  
  private final BorderContent footer;
  
  
  private RefContext references = RefContext.EMPTY;
  
  
  private Rectangle pageSize = PageSize.A4;
  
  private float marginLeft = DEFAULT_MARGIN;
  
  private float marginRight = DEFAULT_MARGIN;
  
  private float marginTop = DEFAULT_MARGIN;
  
  private float marginBottom = DEFAULT_MARGIN;
  
  
  
  
  /**
   * Main constructor.
   * 
   * @param header    non-null
   * @param mainTable non-null
   * @param footer    optional (may be null)
   */
  public ReportTemplate(Header header, TableTemplate mainTable, BorderContent footer) {
    this.header = Objects.requireNonNull(header, "null header");
    this.mainTable = Objects.requireNonNull(mainTable, "null main table");
    this.footer = footer;
  }
  
  
  public ReportTemplate(FixedTable headerTable, TableTemplate mainTable) {
    this(new Header(headerTable, null), mainTable, null);
  }
  
  
  public final Header getHeader() {
    return header;
  }
  
  
  public final TableTemplate getMainTable() {
    return mainTable;
  }
  
  
  public final BorderContent getFooter() {
    return footer;
  }
  
  public final int getHeaderArgCount() {
    return header.getHeaderTable().getDynamicCellCount();
  }
  
  
  
  public void writePdfFile(File file, List<CellData> cells) throws IOException {
    Objects.requireNonNull(file, "null file");
    Objects.requireNonNull(cells, "null cells");
    List<CellData> headerCells, mainCells;
    {
      int headerCount = getHeaderArgCount();
      if (cells.size() < headerCount + mainTable.getColumnCount())
        throw new IllegalArgumentException("too few cells (" + cells.size() + ")");
      
      if (headerCount == 0) {
        headerCells = List.of();
        mainCells = cells;
      } else {
        headerCells = cells.subList(0, headerCount);
        mainCells = cells.subList(headerCount, cells.size());
      }
    }
    
    var headerPdfTable = header.getHeaderTable().createTable(headerCells);
    var mainPdfTable = mainTable.createTable(mainCells);
    
    var document = new Document(pageSize, marginLeft, marginRight, marginTop, marginBottom);
    PdfWriter.getInstance(
        document,
        new FileOutputStream(file));
    
    header.getHeadContent().ifPresent(border -> document.setHeader(border.newHeaderFooter()));
    if (footer != null)
      document.setFooter(footer.newHeaderFooter());
    
    
    document.open();
    document.add(headerPdfTable);
    document.add(mainPdfTable);
    document.close();
  }
  
  
  public final RefContext getReferences() {
    return references;
  }
  
  
  public void setReferences(RefContext references) {
    this.references = references == null ? RefContext.EMPTY : references;
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
