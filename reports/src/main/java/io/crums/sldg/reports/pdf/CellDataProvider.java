/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf;

import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Objects;
import java.util.Optional;

import io.crums.sldg.reports.pdf.CellData.ImageCell;
import io.crums.sldg.reports.pdf.CellData.TextCell;

/**
 * Provides both semantic and display customizations for a specific
 * types of (source) column value.
 * 
 * @param <T> the source column value type.
 */
public interface CellDataProvider<T> {
  
  
  
 
  
  
  
  /**
   * Returns a {@linkplain CellData} instance for the given {@code value}.
   * If the conversion fails, a "blank" cell is returned.
   * 
   * @param value      not null
   * @param cellFormat optional (may be {@code null})
   * 
   * @return the cell after conversion; or a blank cell, if conversion fails (never {@code null})
   * @throws NullPointerException if {@code value} is {@code null}
   */
  CellData getCellData(T value, CellFormat cellFormat);
  
  
  
  
  
  
  
  //    - - -  A B S T R A C T  - - -    \\
  
  
  
  
  
  /**
   * Base text provider. This supports an optional {@linkplain #prefix()} and
   * {@linkplain #postfix()}.
   * 
   * @param <T>   the source value type
   */
  public static abstract class BaseTextProvider<T> implements CellDataProvider<T> {
    
    protected final String prefix;
    protected final String postfix;
    

    BaseTextProvider() {
      this("", "");
    }
    

    BaseTextProvider(String prefix) {
      this(prefix, "");
    }
    
    BaseTextProvider(String prefix, String postfix) {
      this.prefix = Objects.requireNonNull(prefix, "null prefix");
      this.postfix = Objects.requireNonNull(postfix, "null postfix");
    }
    
    
    String dressUp(String text) {
      return prefix.length() + postfix.length() == 0 ?
          text :
            prefix + text + postfix;
    }
    
    
    final boolean baseEquals(BaseTextProvider<?> other) {
      return other.prefix.equals(prefix) && other.postfix.equals(postfix);
    }
    
    /**
     * Optional prefix. If present, then the string is not empty.
     */
    public Optional<String> prefix() {
      return ofNotEmpty(prefix);
    }
    

    /**
     * Optional postfix. If present, then the string is not empty.
     */
    public Optional<String> postfix() {
      return ofNotEmpty(postfix);
    }
    
    private Optional<String> ofNotEmpty(String value) {
      return value.isEmpty() ? Optional.empty() : Optional.of(value);
    }
    

    
    @Override
    public int hashCode() {
      return prefix.hashCode() * 31 + postfix.hashCode();
    }
  }
  
  
  /**
   * Base class for patterned providers such as dates, numbers, etc.
   * 
   * @param <T> the column value type
   */
  public static abstract class PatternedProvider<T> extends BaseTextProvider<T> {
    
    protected final String pattern;
    
    PatternedProvider(String pattern) {
      this.pattern = Objects.requireNonNull(pattern, "null pattern");
    }
    
    PatternedProvider(String pattern, String prefix) {
      super(prefix);
      this.pattern = Objects.requireNonNull(pattern, "null pattern");
    }
    
    PatternedProvider(String pattern, String prefix, String postfix) {
      super(prefix, postfix);
      this.pattern = Objects.requireNonNull(pattern, "null pattern");
    }
    
    
    final boolean patternedEquals(PatternedProvider<?> other) {
      return baseEquals(other) && other.pattern.equals(pattern);
    }
    
    
    @Override
    public int hashCode() {
      return super.hashCode() * 31 + pattern.hashCode();
    }
    
    
    public final String pattern() {
      return pattern;
    }
    
  }
  
  
  
  
  
  //    - - -  C O N C R E T E  -  I M P L S  - - -    \\

  
  /**
   * Stateless string provider. <em>Safe under concurrent access.</em> This is designed to work with {@code String} types,
   * however, it can be used with [instances of] any class whose {@code toString()} method
   * has been overridden to display it's "value". For example, it works reasonably well with most
   * {@code Number} subclasses.
   */
  public static class StringProvider extends BaseTextProvider<Object> {
    
    /** Plain instance sans prefix or postfix. */
    public final static StringProvider PLAIN = new StringProvider();
    
    public StringProvider() {  }
    
    public StringProvider(String prefix) {
      super(prefix);
    }
    
    public StringProvider(String prefix, String postfix) {
      super(prefix, postfix);
    }

    @Override
    public CellData getCellData(Object value, CellFormat cellFormat) {
      return new TextCell(dressUp(value.toString()), cellFormat);
    }
    
    
    
    @Override
    public final boolean equals(Object o) {
      return o instanceof StringProvider other && baseEquals(other);
    }

    @Override
    public final int hashCode() {
      return super.hashCode() * 31 + 1;
    }
    
  }
  
  
  /** Number provider. <em>Not safe under concurrent access.</em> */
  public static class NumberProvider extends PatternedProvider<Number> {
    
    private final NumberFormat format;

    public NumberProvider() {
      this("");
    }
    
    public NumberProvider(String pattern)
        throws IllegalArgumentException {
      this(pattern, "", "");
    }

    
    public NumberProvider(String pattern, String prefix)
        throws IllegalArgumentException {
      this(pattern, prefix, "");
    }
    

    public NumberProvider(String pattern, String prefix, String postfix) {
      super(pattern, prefix, postfix);
      this.format = new DecimalFormat(pattern);
      if (pattern.isEmpty())
        format.setGroupingUsed(false);
    }
    

    @Override
    public CellData getCellData(Number value, CellFormat cellFormat) {
      String string;
      if (value instanceof Double || value instanceof Float)
        string = format.format(value.doubleValue());
      else
        string = format.format(value.longValue());
      
      return new TextCell(dressUp(string), cellFormat);
    }
    
    
    
    @Override
    public final int hashCode() {
      return super.hashCode() * 31 + 2;
    }

    @Override
    public final boolean equals(Object o) {
      return o instanceof NumberProvider np && patternedEquals(np);
    }
  }
  
  

  /**
   *  Date provider. <em>Not safe under concurrent access.</em>
   *  <p>TODO: needs work.. add TZ</p>
   */
  public static class DateProvider extends PatternedProvider<Long> {
    
    private final DateFormat format;
    
    
    public DateProvider(String pattern) {
      super(pattern);
      this.format = new SimpleDateFormat(pattern);
    }
    
    public DateProvider(String pattern, String prefix) {
      super(pattern, prefix);
      this.format = new SimpleDateFormat(pattern);
    }
    
    public DateProvider(String pattern, String prefix, String postfix) {
      super(pattern, prefix, postfix);
      this.format = new SimpleDateFormat(pattern);
    }

    /** @param utc  UTC millis */
    @Override
    public CellData getCellData(Long utc, CellFormat cellFormat) {
      var string = format.format(utc);
      return new TextCell(dressUp(string), cellFormat);
    }
    
    
    
    
    @Override
    public final int hashCode() {
      return super.hashCode() * 31 + 3;
    }

    @Override
    public final boolean equals(Object o) {
      return o instanceof DateProvider dp && patternedEquals(dp);
    }
    
  }
  
  
  /** Interprets a blob of bytes as an image. */
  public static class ImageProvider implements CellDataProvider<ByteBuffer> {
    
    private final float width;
    private final float height;
    
    private final CellData fallback;
    
    
    /**
     * Creates an instance with a blank cell as a fallback.
     * 
     * @param width     &gt; 1
     * @param height    &gt; 1
     */
    public ImageProvider(float width, float height) {
      this(width, height, TextCell.BLANK);
    }

    /**
     * Full constructor. Sets the image dimensions and the fallback.
     * 
     * @param width     &gt; 1
     * @param height    &gt; 1
     * @param fallback  fallback cell in the event an image cannot be constructed from
     *                  the source column bytes
     */
    public ImageProvider(float width, float height, CellData fallback) {
      this.width = width;
      this.height = height;
      this.fallback = Objects.requireNonNull(fallback, "null fallback");
      ImageCell.checkDimensions(width, height);
    }
    
    

    @Override
    public CellData getCellData(ByteBuffer value, CellFormat cellFormat) {
      if (!value.hasRemaining())
        return fallback;
      byte[] bytes = new byte[value.remaining()];
      value.get(bytes);
      try {
        return CellData.forImage(null, bytes, width, height, cellFormat);
      } catch (Exception error) {
        // TODO: nag conditionally
        return fallback;
      }
    }



    public final float getWidth() {
      return width;
    }



    public final float getHeight() {
      return height;
    }



    public final CellData getFallback() {
      return fallback;
    }
    
    
    
    
    
    @Override
    public final int hashCode() {
      return (int) (31 * width + height);
    }
    

    @Override
    public final boolean equals(Object o) {
      return
          o instanceof ImageProvider ip &&
          width == ip.width &&
          height == ip.height;
    }
    
  }
  
  
  
  

}
