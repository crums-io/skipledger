/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.packs;


import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

import io.crums.io.Serial;
import io.crums.sldg.Entry;
import io.crums.sldg.EntryInfo;
import io.crums.sldg.bags.EntryBag;
import io.crums.util.Lists;
import io.crums.util.Strings;

/**
 * Builds an {@linkplain EntryPack}. Also, the {@linkplain Serial serial} form of this class
 * is used to save and load these entry packs.
 * 
 * @see EntryPack#load(ByteBuffer)
 */
public class EntryPackBuilder implements EntryBag, Serial {
  
  
  
  private final TreeMap<Long,FullEntry> entries = new TreeMap<>();
  
  
  public List<Long> rowNumbers() {
    synchronized (entries) {
      return Lists.readOnlyCopy(entries.keySet());
    }
  }
  
  
  
  public Entry getEntry(long rowNumber) {
    synchronized (entries) {
      return entries.get(rowNumber);
    }
  }


  @Override
  public List<EntryInfo> availableEntries() {
    synchronized (entries) {
      return entries.values().stream().map(e -> e.getInfo()).collect(Collectors.toList());
    }
  }



  @Override
  public Entry entry(long rowNumber) {
    synchronized (entries) {
      return entries.get(rowNumber);
    }
  }
  
  
  
  public boolean hasEntry(long rowNumber) {
    synchronized (entries) {
      return entries.containsKey(rowNumber);
    }
  }
  
  
  /**
   * Adds the entries not already in this builder from the given bag.
   * 
   * @return the number of entries added
   */
  public int addAll(EntryPack bag) {
    return addAll(bag, false);
  }
  
  /**
   * Adds all the entries in the given bag.
   * 
   * @param overwrite if {@code true}, then existing entries are overwritten.
   *        Defaults to {@code false} in overload.
   * 
   * @return the number of entries added
   */
  public int addAll(EntryPack bag, boolean overwrite) {
    Objects.requireNonNull(bag, "null bag");
    return overwrite ? overwriteAll(bag) : complement(bag);
  }
  
  
  private int complement(EntryPack bag) {
    List<EntryInfo> infos = bag.availableEntries();
    synchronized (entries) {
      int initSize = entries.size();
      infos.forEach(i -> setEntryIfNotPresent(i, bag));
      return entries.size() - initSize;
    }
    
  }
  
  private int overwriteAll(EntryPack bag) {
    List<EntryInfo> infos = bag.availableEntries();
    synchronized (entries) {
      infos.forEach(i -> setEntry(i, bag));
    }
    return infos.size();
  }
  
  private void setEntryIfNotPresent(EntryInfo info, EntryPack bag) {
    Long rn = info.rowNumber();
    if (entries.get(rn) == null)
      setEntry(info, bag);
  }
  
  private void setEntry(EntryInfo info, EntryPack bag) {
    Entry e = bag.entry(info.rowNumber());
    String meta = info.hasMeta() ? info.meta() : null;
    setEntry(info.rowNumber(), e.content(), meta);
  }
  
  
  
  
  /**
   * Sets the entry at the given row number and returns the previous one if any.
   */
  public Entry setEntry(long rowNumber, ByteBuffer content, String meta) {
    FullEntry e = new FullEntry(content, rowNumber, meta);
    synchronized (entries) {
      return entries.put(rowNumber, e);
    }
  }
  
  /**
   * Updates the meta string for entry with the given row number. {@code null} unsets
   * it.
   */
  public void setMeta(long rowNumber, String meta) {
    synchronized (entries) {
      FullEntry e = entries.get(rowNumber);
      if (e == null)
        throw new IllegalArgumentException("no such row number in bag: " + rowNumber);
      e.setMeta(meta);
    }
  }
  
  


  /**
   * <p>Note this is relatively expensive.</p>
   * {@inheritDoc}
   */
  @Override
  public int serialSize() {
    synchronized (entries) {
      int total = 4;
      final int count = entries.size();
      
      if (count != 0) {
        total += count * ENTRY_HEAD_SIZE;
        
        // gather the blob sizes (from in EBG_HEADER)
        for (FullEntry e : entries.values()) {
          if (e.hasMeta())
            total += e.getMeta().getBytes(Strings.UTF_8).length;
          total += e.contentSize();
        }
      } // if
      
      return total;
    } // synchronized
    
  }
  
  
  private final static int ENTRY_HEAD_SIZE = 8 + 4 + + 2 + 2;



  @Override
  public ByteBuffer writeTo(ByteBuffer out) {
    synchronized (entries) {
      final int count = entries.size();
      
      // preamble ENTRY_COUNT
      out.putInt(count);
      if (count == 0)
        return out;
      
      // EBG_HEADER =: count-many ENTRY_HEADs
      byte[] EMPTY = {  };
      ArrayList<byte[]> metas = new ArrayList<>(count);
      for (FullEntry e : entries.values()) {
        // RN ENTRY_SIZE
        out.putLong(e.rowNumber()).putInt(e.contentSize());
        byte[] m = e.hasMeta() ? e.getMeta().getBytes(Strings.UTF_8) : EMPTY;
        metas.add(m);
        // META_SIZE RSVD
        out.putShort((short) m.length).putShort((short) 0);
      }
      // EBG_HEADER complete
      
      // ENTRY_CNTS := write out the variable-width fields
      int index = 0;
      for (FullEntry e : entries.values())
        out.put(metas.get(index++)).put(e.content());
    }
    return out;
  }
  
  
  
  
  
  private static class FullEntry extends Entry implements Comparable<FullEntry> {
    
    /**
     * We encode in UTF-8. Hopefully chars with above 2 code points are rare.
     */
    private final static int CHECK_META = 256 * 128;
    
    private String meta = "";

    public FullEntry(ByteBuffer contents, long rowNumber, String meta) {
      super(contents, rowNumber);
      setMeta(meta);
    }
    
    
    
    private void setMeta(String meta) {
      if (meta == null) {
        meta = "";
      } else if (meta.length() > CHECK_META) {
        int len = meta.getBytes(Strings.UTF_8).length;
        if (len > 0xffff)
          throw new IllegalArgumentException(
              "meta length " + len + " does not fit in 2 bytes; actual bytes " + len +
              ". prefix: " + meta.substring(0, 16) + " ..");
      }
      
      this.meta = meta;
    }
    
    
    
    public String getMeta() {
      return meta;
    }
    
    
    public EntryInfo getInfo() {
      return new MetaEntry(rowNumber(), contentSize(), meta);
    }
    
    
    public boolean hasMeta() {
      return !meta.isEmpty();
    }
    
    
    @Override
    public boolean equals(Object o) {
      return o == this || (o instanceof FullEntry) && ((FullEntry) o).rowNumber() == rowNumber();
    }
    

    @Override
    public int hashCode() {
      return Long.hashCode(rowNumber());
    }



    @Override
    public int compareTo(FullEntry o) {
      return Long.compare(rowNumber(), o.rowNumber());
    }
    
  }
  
  
  public static class MetaEntry extends EntryInfo {
    
    private final String meta;

    public MetaEntry(long rowNumber, int size, String meta) {
      super(rowNumber, size);
      this.meta = meta == null ? "" : meta;
    }

    @Override
    public boolean hasMeta() {
      return !meta.isEmpty();
    }

    @Override
    public String meta() {
      return hasMeta() ? meta : super.meta();
    }
    
  }

}
