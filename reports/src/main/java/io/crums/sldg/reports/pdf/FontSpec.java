/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf;


import java.awt.Color;
import java.util.Collections;
import java.util.Locale;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;

import com.lowagie.text.Font;
import com.lowagie.text.FontFactory;
import com.lowagie.text.StandardFonts;

/**
 * Created because there's no easy way to recover {@code FontFactory} arguments from
 * {@code Font} objects. Instances are immutable.
 */
public class FontSpec {
  
  
  public static int styleInt(boolean bold, boolean italic, boolean underline) {
    int intStyle = Font.NORMAL;
    if (bold)
      intStyle |= Font.BOLD;
    if (italic)
      intStyle |= Font.ITALIC;
    if (underline)
      intStyle |= Font.UNDERLINE;
    return intStyle;
  }
  
  
  /**
   * Returns the standard font family names (sans italic and such),
   * in uppercase.
   */
  public static SortedSet<String> standardFamilyNames() {
    return STD_FONT_FAMILIIES;
  }

  private final static String BOLD = "_BOLD";
  private final static String ITALIC = "_ITALIC";
  private final static String BOLDITALIC = "_BOLDITALIC";
  
  
  private final static SortedSet<String> STD_FONT_FAMILIIES;
  
  static {
    var families = new TreeSet<String>();
    for (var font : StandardFonts.values()) {
      var name = font.name();
      // the implementation doesn't distinguish these types..
      // use the style element instead
      if (name.endsWith(ITALIC) || name.endsWith(BOLD) || name.endsWith(BOLDITALIC))
        continue;
      families.add(name.toUpperCase(Locale.ROOT));
    }
    STD_FONT_FAMILIIES = Collections.unmodifiableSortedSet(families);
  }
  
  
  
  private final String name;
  private final float size;
  private final int style;
  private final Color color;
  
  private volatile Font font;
  
  
  public FontSpec(String name, float size, int style, Color color) {
    this.name = Objects.requireNonNull(name, "null name");
    this.size = size;
    this.style = style;
    this.color = color == null ? Color.BLACK : color;
  }


  public final String getName() {
    return name;
  }


  public final float getSize() {
    return size;
  }


  public final int getStyle() {
    return style;
  }


  public final Color getColor() {
    return color;
  }


  public Font getFont() {
    if (font == null)
      font = FontFactory.getFont(name, size, style, color);
    return font;
  }
  
  
  @Override
  public final boolean equals(Object o) {
    if (o == this)
      return true;
    else if (o instanceof FontSpec) {
      var other = (FontSpec) o;
      return
          other.name.equalsIgnoreCase(name) &&
          other.size == size &&
          other.style == style &&
          other.color.equals(color);
    } else
      return false;
  }
  

  @Override
  public final int hashCode() {
    return name.hashCode() ^ Float.hashCode(size) ^ style ^ color.hashCode();
  }

}
