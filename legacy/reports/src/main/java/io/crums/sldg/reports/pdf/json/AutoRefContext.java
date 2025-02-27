/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.json;


import java.awt.Color;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import io.crums.sldg.reports.pdf.CellData;
import io.crums.sldg.reports.pdf.CellFormat;
import io.crums.sldg.reports.pdf.FontSpec;
import io.crums.util.Maps;

/**
 * Auto discovery {@linkplain EditableRefContext}. Specifically, its {@code findRef(..)}
 * methods return a "present" {@code Optional}, if an object is encountered a second
 * time.
 * <h2>Motivation</h2>
 * <p>
 * Tho the JSON model for {@code ReportTemplate} is designed to be simple enough to be
 * editable by hand, it's much easier to build a prototype programmatically and let the
 * parser deduplicate stuff by introducing references.
 * </p><p>
 * Note if you use this class, then you want to print the JSON in a 2 passes: one, a
 * dry run to populate an instance of this class, then a second run to print the JSON with
 * the instance.
 * </p>
 * <h2>Write-path Only</h2>
 * <p>
 * There is <em>no use case for using this class on the read path</em>.
 * </p>
 */
public class AutoRefContext extends EditableRefContext {
  
  /** Auto-discovered elements use these prefixes in the named references. */
  public record Prefixes(String color, String font, String format, String cellData) {
    /** Non-null arguments only. */
    public Prefixes {
      Objects.requireNonNull(color, "null color prefix");
      Objects.requireNonNull(font, "null font prefix");
      Objects.requireNonNull(format, "null format prefix");
      Objects.requireNonNull(cellData, "null cellData prefix");
    }
  }
  
  /**
   * Constructors default to using these prefixes in naming duplicated elements
   * that are discovered.
   */
  public final static Prefixes DEFAULT_PREFIXES =
      new Prefixes("AUTO_CLR-", "AUTO_FNT-", "AUTO_FMT-", "AUTO_CDT-");

  
  private static class Tally<T> {
    final HashSet<T> seen = new HashSet<>();
    final TreeMap<String,T> added = new TreeMap<>();
    
    Map<String,T> getAdded() { return Maps.asReadOnly(added); }
  }
  
  
  private final Tally<Color> colors = new Tally<>();
  private final Tally<FontSpec> fonts = new Tally<>();
  private final Tally<CellFormat> formats = new Tally<>();
  private final Tally<CellData> cells = new Tally<>();
  
  private final Prefixes prefixes;
  
  
  /**
   * Constructs an empty instance.
   */
  public AutoRefContext() {
    this(RefContext.EMPTY, DEFAULT_PREFIXES);
  }

  /**
   * Constructs an instance with the given {@code RefContext} copy. You typically want this
   * constructor: if the JSON already contains references, you don't want to lose (or have
   * to rename) them.
   * 
   * @param copy      not null, may be empty. Typically, this is the <em>gathered</em> references
   *                  from reading the JSON on the read path
   * 
   * @see ReportTemplateParser#getReferences()
   */
  public AutoRefContext(RefContext copy) {
    this(copy, DEFAULT_PREFIXES);
  }
  
  
  /**
   * Full constructor.
   * 
   * @param copy      not null, may be empty
   * @param prefixes  not null
   */
  public AutoRefContext(RefContext copy, Prefixes prefixes) {
    super(copy);
    this.prefixes = Objects.requireNonNull(prefixes, "null prefixes");
  }
  
  
  
  
  
  
  
  public Optional<String> findRef(CellFormat format) {
    var opt = super.findRef(format);
    return observeOrInsert(opt, formats, cellFormatRefs(), format, prefixes.format());
  }
  
  public Optional<String> findRef(CellData cellData) {
    var opt = super.findRef(cellData);
    return observeOrInsert(opt, cells, cellDataRefs(), cellData, prefixes.cellData());
  }
  
  public Optional<String> findRef(Color color) {
    var opt = super.findRef(color);
    return observeOrInsert(opt, colors, colorRefs(), color, prefixes.color());
  }
  
  public Optional<String> findRef(FontSpec font) {
    var opt = super.findRef(font);
    return observeOrInsert(opt, fonts, fontRefs(), font, prefixes.font());
  }
  
  
  
  public int countRefsCreated() {
    return
        colors.added.size() +
        fonts.added.size() +
        formats.added.size() +
        cells.added.size();
  }
  
  
  public Map<String, Color> colorRefsCreated() {
    return colors.getAdded();
  }
  
  
  public Map<String, FontSpec> fontRefsCreated() {
    return fonts.getAdded();
  }
  
  
  public Map<String, CellFormat> formatRefsCreated() {
    return formats.getAdded();
  }
  
  
  public Map<String, CellData> cellRefsCreated() {
    return cells.getAdded();
  }
  
  
  
  
  
  private <T> Optional<String> observeOrInsert(
      Optional<String> opt, Tally<T> tally, Map<String, T> map, T item, String prefix) {
    if (opt.isEmpty() && !tally.seen.add(item)) {
      var autoKey = autoKey(prefix, map);
      map.put(autoKey, item);
      tally.added.put(autoKey, item);
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































