/*
 * Copyright 2020-2021 Babak Farhang
 */
package io.crums.sldg;


import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;

import io.crums.io.Serial;
import io.crums.util.Lists;
import io.crums.util.Strings;

/**
 * A user-declaration highlighting important row numbers in a
 * morsel. It might as well be named a row-set. The reason why I'm using
 * the <em>Path</em> moniker is that the given "row-set" must be structurally
 * linked (usually through other rows) in order for this declaration
 * to be valid.
 * <h2>TODO: remove or fix me</h2>
 * <p>
 * This represents too many things. A path no. encoding, and meta info.
 * The path no. encoding part made it into
 * {@linkplain SkipLedger#stitch(List)} and
 * {@linkplain SkipLedger#stitchCompress(List)}; the meta info part, well..
 * not sure it necessarily belongs with every path: it's more like a
 * one-to-many relationship.
 * </p>
 */
public class PathInfo implements Serial {
  
  
  
  public static PathInfo load(ByteBuffer in) {
    List<Long> declaration;
    {
      int count = 0xff & in.getShort();
      Long[] decl = new Long[count];
      for (int index = 0; index < count; ++index)
        decl[index] = in.getLong();
      declaration = Lists.asReadOnlyList(decl);
    }
    String meta;
    {
      int size = 0xffff & in.getShort();
      if (size == 0)
        meta = null;
      else {
        byte[] bytes = new byte[size];
        in.get(bytes);
        meta = new String(bytes, Strings.UTF_8);
      }
    }
    return new PathInfo(declaration, meta);
  }
  
  
  
  /**
   * Maximum number of rows in a declaration.
   */
  public final static int MAX_DECLARATION_SIZE = 0xffff;

  /**
   * Maximum number of characters in the meta string. (Allowing for 2-byte unicode,
   * this should add up no greater than 64k inside a morsel.)
   */
  public final static int MAX_META_SIZE = 0xffff / 2;
  
  
  
  
  
  
  
  
  
  private final List<Long> declaration;
  
  private final String meta;
  
  
  
  

  /**
   * Constructs an instance with with no meta information.
   * 
   * @param declaration
   */
  public PathInfo(List<Long> declaration) {
    this(declaration, null);
  }
  
  
  /**
   * Constructs an instance using the given declaring row numbers. These are typically
   * a subset of the final {@linkplain #rowNumbers() row numbers} which are composed
   * by stitching in linking row numbers as necessary.
   * 
   * @param declaration positive, not empty, sorted list of positive row numbers
   * @param meta null counts as empty
   */
  public PathInfo(List<Long> declaration, String meta) {
    
    this.declaration = Lists.readOnlyOrderedCopy(declaration);
    this.meta = dressUpMeta(meta);
    
    if (declaration.isEmpty() || declaration.get(0) < 1)
      throw new IllegalArgumentException("declaration " + declaration);
    
    checkDeclSize();
  }
  
  
  private void checkDeclSize() {
    if (declaration.size() > MAX_DECLARATION_SIZE)
      throw new IllegalArgumentException("declaration size " + declaration.size() + " too large");
  }
  
  
  /**
   * Constructs an instance.
   * 
   * @param lo &ge; 1
   * @param targets the middle row numbers (&ge; lo, &le; hi). May be empty but not null.
   * @param hi &gt; {@code lo}
   * @param meta null counts as empty
   */
  public PathInfo(long lo, List<Long> targets, long hi, String meta) {
    if (lo < 1 || hi <= lo)
      throw new IllegalArgumentException("lo, hi: " + lo + ", " + hi);
    
    if (targets.isEmpty()) {
      
      Long[] rows = { lo, hi };
      this.declaration = Lists.asReadOnlyList(rows);
    
    } else {
      
      var decl = new TreeSet<>(targets);
      if (targets.size() != decl.size())
        throw new IllegalArgumentException(
            "target row numbers contain duplicates");
      
      decl.add(lo);
      decl.add(hi);
      
      if (decl.first() < lo)
        throw new IllegalArgumentException(
            "targets minimum (" + decl.first() + ") < lo (" + lo + ")");
      if (decl.last() > hi)
        throw new IllegalArgumentException(
            "targets maximum (" + decl.last() + ") > hi (" + hi + ")");
      
      Long[] declRn = decl.toArray(new Long[decl.size()]);
      this.declaration = Lists.asReadOnlyList(declRn);
      
      checkDeclSize();
    }
    
    this.meta = dressUpMeta(meta);
  }
  
  
  /**
   * Constructs a skip path instance.
   * 
   * @param lo &ge; 1
   * @param hi &gt; {@code lo}
   * @param meta null counts as empty
   */
  public PathInfo(long lo, long hi, String meta) {
    this(lo, Collections.emptyList(), hi, meta);
  }
  
  
  
  protected String dressUpMeta(String meta) {
    if (meta == null)
      return "";
    if (meta.length() > MAX_META_SIZE)
      throw new IllegalArgumentException("meta too large: " + meta.length());
    
    return meta;
  }
  
  
  
  
  public boolean hasMeta() {
    return !meta.isEmpty();
  }

  
  public String meta() {
    return meta;
  }
  
  
  /**
   * Returns the row numbers that structurally define the path.
   * If necessary (usually), this is a stitched version of the {@linkplain
   * #declaration() declaration}.
   * 
   * @return lazily loaded, read-only list of linked row numbers
   */
  public final List<Long> rowNumbers() {
    return SkipLedger.stitch(declaration);
  }
  
  
  /**
   * Returns the declared row numbers. These are the exactly the
   * numbers specified at construction.
   * 
   * @return ordered list of positive longs with no duplicates
   * 
   * @see #rowNumbers()
   */
  public final List<Long> declaration() {
    return declaration;
  }
  
  /**
   * Returns the lowest row number.
   */
  public final long lo() {
    return declaration.get(0);
  }
  
  
  /**
   * Determines if this is a state path. By convention, a path that starts at row number 1
   * is considered a <em>state path</em> from the ledger.
   * 
   * @return {@code lo() == 1L}
   */
  public final boolean isState() {
    return lo() == 1L;
  }
  
  
  /**
   * Returns the highest row number.
   */
  public final long hi() {
    return declaration.get(declaration.size() - 1);
  }
  
  
  /**
   * <p>Instances are equal if their data are equal.</p>
   * {@inheritDoc}
   */
  public final boolean equals(Object o) {
    if (o == this)
      return true;
    if (!(o instanceof PathInfo))
      return false;
    
    PathInfo other = (PathInfo) o;
    return other.meta.equals(meta) && other.declaration.equals(declaration);
  }
  
  
  /**
   * <p>Consistent with {@linkplain #equals(Object)}.</p>
   * {@inheritDoc}
   */
  public final int hashCode() {
    return declaration.hashCode();
  }

  
  

  @Override
  public int serialSize() {
    int metaSize = 2 + (hasMeta() ? meta.getBytes(Strings.UTF_8).length : 0);
    return metaSize + 2 + 8 * declaration.size();
  }


  @Override
  public ByteBuffer writeTo(ByteBuffer out) {
    out.putShort((short) declaration.size());
    for (long row : declaration)
      out.putLong(row);
    if (hasMeta()) {
      byte[] bytes = meta.getBytes(Strings.UTF_8);
      out.putShort((short) bytes.length);
      out.put(bytes);
    } else {
      out.putShort((short) 0);
    }
    return out;
  }
  
}
