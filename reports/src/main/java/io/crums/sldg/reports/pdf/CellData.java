/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf;


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
 * @see #appendToTable(Rectangle, CellFormat, PdfPTable)
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
    var image = toImage(ref, bytes);
    return new ImageCell(ref, image, width, height, format);
  }
  
  
  public static ImageCell forImageScaledToMaxHeight(String ref, byte[] bytes, float maxHeight, CellFormat format) {
    if (maxHeight <= 0)
      throw new IllegalArgumentException("maxHeight: " + maxHeight);
    var image = toImage(ref, bytes);
    float h = image.getPlainHeight();
    if (h > maxHeight)
      image.scalePercent(100 * maxHeight / h);
    return new ImageCell(ref, image, format);
  }
  
  
  
  
  private static Image toImage(String ref, byte[] bytes) {
    try {
      return Image.getInstance(bytes);
    } catch (IOException iox) {
      throw new UncheckedIOException("on loading ref '" + ref + "':" + iox.getMessage(), iox);
    }
  }
  
  
  
  
  
  
  

  final CellFormat format;
  
  
  
  /**
   * Subclasses defined in this package.
   */
  CellData(CellFormat format) {
    this.format = format;
  }
  
  
  /**
   * Returns the optional <em>override</em> format for this cell. If set, then it
   * <em>overrides</em> the one given in {@linkplain #appendToTable(Rectangle, CellFormat, PdfPTable)}.
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
   * @param format   default cell settings (font text, leading, padding, etc.)
   *                 If the instance has its own {@linkplain #getFormat() format},
   *                 then this argument is ignored.
   * @param table    the table the cell is appended to
   */
  public abstract void appendToTable(Rectangle borders, CellFormat format, PdfPTable table);
  
  
  CellFormat format(CellFormat fmt) {
    return format == null ? fmt : format;
  }
  
  
  static abstract class TextualCell extends CellData {
    
    TextualCell(CellFormat format) {
      super(format);
    }
    
    public abstract String getText();

    @Override
    public void appendToTable(Rectangle borders, CellFormat fmt, PdfPTable table) {
      fmt = format(fmt);
      Paragraph p = new Paragraph(getText(), fmt.getFont().getFont());
      p.setLeading(fmt.getLeading());
      PdfPCell cell = new PdfPCell(p);
      cell.setLeading(fmt.getLeading(), 0);
      cell.setVerticalAlignment(fmt.getAlignV().code);
      cell.setHorizontalAlignment(fmt.getAlignH().code);
      cell.cloneNonPositionParameters(borders);
      cell.setUseBorderPadding(true);
      cell.setUseAscender(fmt.isAscender());
      cell.setUseDescender(fmt.isDescender());
      cell.setPadding(fmt.getPadding());
      fmt.getBackgroundColor().ifPresent(cell::setBackgroundColor);
      table.addCell(cell);
    }

    @Override
    public boolean equals(Object o) {
      return
         o instanceof TextCell other &&
         other.getText().equals(getText()) &&
         formatEquals(other);
    }

    @Override
    public final int hashCode() {
      return getText().hashCode() * 31 + formatHashCode();
    }
  }
  
  //    C O N C R E T E    I M P L S ..
  
  /**
   * Text cell data.
   */
  public static class TextCell extends TextualCell {
    
    /**
     * Blank cell with a single space.
     */
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

    @Override
    public String getText() {
      return text;
    }
  }
  
  
  
  
  
  
  
  
  public final static class ImageCell extends CellData {
    
    public static void checkDimensions(float width, float height) {
      if (width <= 1.0)
        throw new IllegalArgumentException("illegal width: " + width);
      if (height <= 1.0)
        throw new IllegalArgumentException("illegal height" + height);
    }
    
    private final String ref;
    private final Image image;
    private final float width;
    private final float height;
    
    
    
    
    public ImageCell(String ref, Image image, CellFormat format) {
      super(format);
      Objects.requireNonNull(image, "null image");
      this.ref = ref;
      this.image = Image.getInstance(image);
      this.width = image.getWidth();
      this.height = image.getHeight();
    }
    
    
    public ImageCell(String ref, Image image, float width, float height, CellFormat format) {
      super(format);
      Objects.requireNonNull(image, "null image");
      this.ref = ref;
      this.image = Image.getInstance(image);
      this.width = width;
      this.height = height;
      checkDimensions(width, height);
      
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
    public void appendToTable(Rectangle borders, CellFormat fmt, PdfPTable table) {
      fmt = format(fmt);
      var img = Image.getInstance(image);
      PdfPCell cell = new PdfPCell();
      cell.addElement(new Chunk(img, 0, 0));
      cell.setVerticalAlignment(fmt.getAlignV().code);
      cell.setHorizontalAlignment(fmt.getAlignH().code);
      cell.cloneNonPositionParameters(borders);
      cell.setUseBorderPadding(true);
      cell.setUseAscender(fmt.isAscender());
      cell.setUseDescender(fmt.isDescender());
      cell.setPadding(fmt.getPadding());
      fmt.getBackgroundColor().ifPresent(cell::setBackgroundColor);
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
  
  


}

















