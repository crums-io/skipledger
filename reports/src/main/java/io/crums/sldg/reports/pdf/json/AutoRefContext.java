/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.json;


import java.awt.Color;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import io.crums.sldg.reports.pdf.CellData;
import io.crums.sldg.reports.pdf.CellFormat;
import io.crums.sldg.reports.pdf.FontSpec;

/**
 * Auto discovery {@linkplain EditableRefContext}. Specifically, its {@code findRef(..)}
 * methods return a "present" {@code Optional}, if an object is encountered a second
 * time.
 * <h3>Motivation</h3>
 * <p>
 * Tho the JSON model for {@code ReportTemplate} is designed to be simple enough to be
 * editable by hand, it's much easier to build a prototype programmatically and let the
 * parser deduplicate stuff by introducing references.
 * </p><p>
 * Note if you use this class, then you want to print the JSON in a 2 passes: one, a
 * dry run to populate an instance of this class, then a second run to print the JSON with
 * the instance.
 * </p>
 */
public class AutoRefContext extends EditableRefContext {
  
  public record Prefixes(String color, String font, String format, String cellData) {
    /** Non-null arguments only. */
    public Prefixes {
      Objects.requireNonNull(color, "null color");
      Objects.requireNonNull(font, "null font");
      Objects.requireNonNull(format, "null format");
      Objects.requireNonNull(cellData, "null cellData");
    }
  }
  
  
  public final static Prefixes DEFAULT_PREFIXES =
      new Prefixes("AUTO_CLR-", "AUTO_FNT-", "AUTO_FMT-", "AUTO_CDT-");

  
  
  private final HashSet<Color> seenColors = new HashSet<>();
  private final HashSet<FontSpec> seenFonts = new HashSet<>();
  private final HashSet<CellFormat> seenFormats = new HashSet<>();
  private final HashSet<CellData> seenCellData = new HashSet<>();
  
  private final Prefixes prefixes;
  
  /**
   * 
   */
  public AutoRefContext() {
    this(Map.of(), DEFAULT_PREFIXES);
  }

  /**
   * @param imageRefs not null
   */
  /**
   * 
   * @param imageRefs not null
   * @param prefixes  not null
   */
  public AutoRefContext(Map<String, ByteBuffer> imageRefs, Prefixes prefixes) {
    super(imageRefs);
    this.prefixes = Objects.requireNonNull(prefixes, "null prefixes");
  }
  
  
  
  
  
  
  
  public Optional<String> findRef(CellFormat format) {
    var opt = super.findRef(format);
    return observeOrInsert(opt, seenFormats, cellFormatRefs(), format, prefixes.format());
  }
  
  public Optional<String> findRef(CellData cellData) {
    var opt = super.findRef(cellData);
    return observeOrInsert(opt, seenCellData, cellDataRefs(), cellData, prefixes.cellData());
  }
  
  public Optional<String> findRef(Color color) {
    var opt = super.findRef(color);
    return observeOrInsert(opt, seenColors, colorRefs(), color, prefixes.color());
  }
  
  public Optional<String> findRef(FontSpec font) {
    var opt = super.findRef(font);
    return observeOrInsert(opt, seenFonts, fontRefs(), font, prefixes.font());
  }
  
  
  
  
  
  private <T> Optional<String> observeOrInsert(
      Optional<String> opt, Set<T> seen, Map<String, T> map, T item, String prefix) {
    if (opt.isEmpty() && !seen.add(item)) {
      var autoKey = autoKey(prefix, map);
      map.put(autoKey, item);
      opt = Optional.of(autoKey);
    }
    return opt;
  }
  
  private String autoKey(String prefix, Map<String, ?> map) {
    int postfix = 1;
    String key = prefix + postfix;
    while (map.containsKey(key)) {
      ++postfix;
      key = prefix + postfix;
    }
    return key;
  }
  

}































