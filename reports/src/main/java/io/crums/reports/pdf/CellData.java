/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.reports.pdf;


import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import com.lowagie.text.Chunk;
import com.lowagie.text.Image;
import com.lowagie.text.Paragraph;
import com.lowagie.text.Rectangle;
import com.lowagie.text.pdf.PdfPCell;
import com.lowagie.text.pdf.PdfPTable;

import io.crums.util.Lists;

/**
 * Data for a PDF table cells. May optionally contain presentation / formatting
 * information.
 * 
 * @see #appendTable(Rectangle, CellFormat, PdfPTable)
 * @see #getFormat()
 */
public abstract class CellData {
  
  /**
   * Creates and returns a {@code TextCell} instance.
   * 
   * @param text non-null
   */
  public static TextCell forText(String text) {
    return new TextCell(text);
  }
  
  /**
   * Creates and a {@code TextCell}-<em>view</em> of the given strings.
   * I.e. lazily loaded/created.
   * 
   * @param cells non-null
   * 
   * @return a <em>view</em> of {@code cells}
   */
  public static List<CellData> forText(List<String> cells) {
    return Lists.map(cells, CellData::forText);
  }
  
  public static ImageCell forImage(String ref, Image image, float width, float height) {
    return new ImageCell(ref, image, width, height, null);
  }
  
  public static ImageCell forImage(String ref, byte[] bytes, float width, float height) {
    return forImage(ref, bytes, width, height, null);
  }
  
  public static ImageCell forImage(
      String ref, byte[] bytes, float width, float height, CellFormat format) {
    Image image;
    try {
      image = Image.getInstance(bytes);
    } catch (IOException iox) {
      throw new UncheckedIOException("on loading ref '" + ref + "':" + iox.getMessage(), iox);
    }
    return new ImageCell(ref, image, width, height, format);
  }
  
  
  
  
  
  
  

  final CellFormat format;
  
  
  
  /**
   * Subclasses defined only here.
   */
  private CellData(CellFormat format) {
    this.format = format;
  }
  
  
  /**
   * Returns the optional <em>override</em> format for this cell. If set, then it
   * <em>overrides</em> the one given in {@linkplain #appendTable(Rectangle, CellFormat, PdfPTable)}.
   */
  public final Optional<CellFormat> getFormat() {
    return Optional.ofNullable(format);
  }
  
  boolean formatEquals(CellData other) {
    return Objects.equals(format, other.format);
  }
  
  
  int formatHashCode() {
    return format == null ? -1 : format.hashCode();
  }
  
   
  
  /**
   * Generates and appends a cell to the given PDF table using the given hint
   * paramaters.
   * 
   * @param borders  border width and color settings (positional info ignored)
   * @param col      default cell settings (font text, leading, padding, etc.)
   *                 If the instance has its own {@linkplain #getFormat() format},
   *                 then this argument is ignored.
   * @param table    the table the cell is appended to
   */
  public abstract void appendTable(Rectangle borders, CellFormat col, PdfPTable table);
  
  
  
  
  
  //    C O N C R E T E    I M P L S ..
  
  /**
   * Text cell data.
   */
  public final static class TextCell extends CellData {
    
    public final static TextCell BLANK = new TextCell(" ");
    
    private final String text;
    
    public TextCell(String text) {
      this(text, null);
    }
    
    public TextCell(String text, CellFormat format) {
      super(format);
      if (Objects.requireNonNull(text, "null text").isEmpty())
        throw new IllegalArgumentException("empty text");
      this.text = text;
    }
    
    public String getText() {
      return text;
    }

    @Override
    public void appendTable(Rectangle borders, CellFormat col, PdfPTable table) {
      if (format != null)
        col = format;
      Paragraph p = new Paragraph(text, col.getFont().getFont());
      p.setLeading(col.getLeading());
      PdfPCell cell = new PdfPCell(p);
      cell.setLeading(col.getLeading(), 0);
      cell.setVerticalAlignment(col.getAlignV().code);
      cell.setHorizontalAlignment(col.getAlignH().code);
      cell.cloneNonPositionParameters(borders);
      cell.setUseBorderPadding(true);
      cell.setUseAscender(col.isAscender());
      cell.setUseDescender(col.isDescender());
      cell.setPadding(col.getPadding());
      col.getBackgroundColor().ifPresent(cell::setBackgroundColor);
      table.addCell(cell);
    }

    @Override
    public boolean equals(Object o) {
      return
         o instanceof TextCell other &&
         other.text.equals(text) &&
         formatEquals(other);
    }

    @Override
    public int hashCode() {
      return text.hashCode() ^ formatHashCode();
    }
  }
  
  
  
  
  public final static class ImageCell extends CellData {
    
    private final String ref;
    private final Image image;
    private final float width;
    private final float height;
    
    
    
    
    public ImageCell(String ref, Image image, float width, float height, CellFormat format) {
      super(format);
      Objects.requireNonNull(image, "null image");
      this.ref = ref;
      this.image = Image.getInstance(image);
      this.width = width;
      this.height = height;
      if (width <= 0)
        throw new IllegalArgumentException("negative width: " + width);
      if (height <= 0)
        throw new IllegalArgumentException("negative height" + height);
      
      this.image.scaleToFit(width, height);
    }
    
    public String getRef() {
      return ref;
    }
    
    public Image getImage() {
      return image;
    }

    public float getWidth() {
      return width;
    }

    public float getHeight() {
      return height;
    }

    @Override
    public void appendTable(Rectangle borders, CellFormat col, PdfPTable table) {
      var img = Image.getInstance(image);
      PdfPCell cell = new PdfPCell();
      cell.addElement(new Chunk(img, 0, 0));
      cell.setVerticalAlignment(col.getAlignV().code);
      cell.setHorizontalAlignment(col.getAlignH().code);
      cell.cloneNonPositionParameters(borders);
      cell.setUseBorderPadding(true);
      cell.setUseAscender(col.isAscender());
      cell.setUseDescender(col.isDescender());
      cell.setPadding(col.getPadding());
      col.getBackgroundColor().ifPresent(cell::setBackgroundColor);
      table.addCell(cell);
    }
    

    @Override
    public boolean equals(Object o) {
      return
          o == this ||
          (o instanceof ImageCell other) &&
          other.ref.equals(ref) &&
          other.width == width &&
          other.height == height &&
          formatEquals(other);
    }
    

    @Override
    public int hashCode() {
      int code = ref.hashCode() + Float.hashCode(width) + Float.hashCode(height);
      return code ^ formatHashCode();
    }
    
  }
  
  
  
  public static class NestedTableCell extends CellData {
    
    private final PdfPTable nestedPdfTable;
    
    
    public NestedTableCell(PdfPTable pdfTable) {
      super(null);
      this.nestedPdfTable = Objects.requireNonNull(pdfTable, "null nested table");
    }

    @Override
    public void appendTable(Rectangle borders, CellFormat col, PdfPTable table) {
      table.addCell(nestedPdfTable);
    }
    
  }

}

















