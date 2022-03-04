/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.reports.pdf;


import java.util.Objects;

import com.lowagie.text.HeaderFooter;
import com.lowagie.text.Phrase;


/**
 * Phrase-based document border content. This can be a header or footer snippet.
 */
public class BorderContent {

  private final String phrase;
  private final FontSpec font;
  private final Align.H alignH;
  private final boolean paginated;

  public BorderContent(String phrase, FontSpec font, Align.H alignH, boolean paginate) {
    this.phrase = Objects.requireNonNull(phrase, "null phrase");
    this.font = Objects.requireNonNull(font, "null font");
    this.alignH = alignH == null ? Align.DEFAULT_H : alignH;
    this.paginated = paginate;
  }

  public final String getPhrase() {
    return phrase;
  }

  public final FontSpec getFont() {
    return font;
  }

  public final Align.H getAlignment() {
    return alignH;
  }
  
  public final boolean isPaginated() {
    return paginated;
  }

  /**
   * Returns the border content.
   */
  HeaderFooter newHeaderFooter() {
    var footer = new HeaderFooter(
      new Phrase(getPhrase(), getFont().getFont()),
      isPaginated());
    footer.setAlignment(getAlignment().code);
    footer.setBorderWidthTop(0);
    footer.setBorderWidthBottom(0);
    return footer;
  }
  
  
  public final boolean equals(Object o) {
    return (o == this) ||
        (o instanceof BorderContent other) &&
        paginated == other.paginated &&
        alignH == other.alignH &&
        other.phrase.equals(phrase) &&
        other.font.equals(font);
  }
  
  
  public int hashCode() {
    return (phrase.hashCode() + 499 * font.hashCode()) ^ alignH.hashCode() + Boolean.hashCode(paginated);
  }

}