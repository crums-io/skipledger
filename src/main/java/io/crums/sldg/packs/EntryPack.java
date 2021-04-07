/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.packs;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import io.crums.io.buffer.BufferUtils;
import io.crums.io.buffer.Partitioning;
import io.crums.sldg.bags.EntryBag;
import io.crums.sldg.entry.Entry;
import io.crums.sldg.entry.EntryInfo;
import io.crums.util.Lists;
import io.crums.util.Strings;

/**
 * Towards serializing a morsel.
 * <h2>REMOVE ME</h2>
 * <p>
 * Parts:
 * <ul>
 * <li>entry numbers</li>
 * <li>unused-2 bytes (maybe use me later for parsing and content types</li>
 * <li>partition: partition offsets</li>
 * <li>meta?</li>
 * <li></li>
 * </ul>
 * </p><p>
 * </p>
 * <h2>Serial Format</h2>
 * 
 * <h4>Primitives</h4>
 * <p>
 * We build up our definitions from the primitives. But first a note about the
 * symbols used here:
 * </p><p>
 * <pre>
 *    :=   means definition
 *    ^    power operator (eg BYTE ^3 := BYTE BYTE BYTE)
 *         if the RHS is a named primitive it is interpreted as an unsigned number
 *         if the RHS is 0, then it is a zero length byte string
 * </pre>
 * </p><p>
 * The primitives:
 * </p><p>
 * <pre>
 *    BYTE := 1 byte (big endian)
 *    SHORT := BTYE ^2
 *    INT := BYTE ^4
 *    LONG := BTYE ^8
 * </pre>
 * </p>
 * 
 * <h4>ENTRY_BAG</h4>
 * <p>
 * </p><p>
 * <pre>
 *    ENTRY_COUNT := INT
 *    
 *    RN := LONG
 *    ENTRY_SIZE  := INT
 *    META_SIZE   := SHORT
 *    RSVD        := SHORT
 *    
 *    ENTRY_HEAD  := RN ENTRY_SIZE META_SIZE RSVD
 *    
 *    EBG_HEADER  := ENTRY_COUNT  ENTRY_HEAD ^ENTRY_COUNT
 *                  where ENTRY_HEAD's are ordered by unique RN's
 *    ENTRY_CNTS  := BYTE ^n
 *                  where n is the sum of the ENTRY_SIZE and META_SIZE's in the EBG_HEADER
 *                  
 *                  or
 *                  
 *    META        := BTYE ^META_SIZE
 *    ENTRY       := BYTE ^ENTRY_SIZE
 *    
 *    ENTRY_CNTS  := [META ENTRY] ^ENTRY_COUNT
 *                  
 *    
 *    ENTRY_BAG   := EBG_HEADER ENTRY_CNTS
 * </pre>
 * </p>
 */
public class EntryPack implements EntryBag {
  
  
  public static EntryPack load(ByteBuffer in) {
    final int count = in.getInt();
    if (count <= 0) {
      if (count == 0)
        return NULL;
      throw new IllegalArgumentException("read negative entry count: " + count);
    }
    
    Integer[] sizes = new Integer[count * 2];
    EntryInfo[] infos = new EntryInfo[count];
    
    int sizeTally = 0;
    for (int index = 0; index < count; ++index) {
      long rn = in.getLong();
      int entrySize = in.getInt();
      int metaSize = 0xffff & in.getShort();
      // kill RSVD
      in.getShort();
      
      int x2 = index * 2;
      sizes[x2] = metaSize;
      sizes[x2 + 1] = entrySize;
      infos[index] = new EntryInfo(rn, entrySize);
      sizeTally += entrySize + metaSize;
      
    }
    
    if (in.remaining() < sizeTally)
      throw new IllegalArgumentException(
          "blob size tally " + sizeTally + " > remaining bytes " + in.remaining());
    
    ByteBuffer block = BufferUtils.slice(in, sizeTally);
    
    Partitioning parts = new Partitioning(block, Lists.asReadOnlyList(sizes));
    
    return new EntryPack(Lists.asReadOnlyList(infos), parts);
  }
  

  
  
  public final static EntryPack NULL = new EntryPack();
  
  
  
  
  
  
  
  

  
  private final Partitioning entries;
  
  private final List<EntryInfo> infos;
  
  
  /**
   * Null instance.
   */
  private EntryPack() {
    this.entries = Partitioning.NULL;
    this.infos = Collections.emptyList();
  }

  private EntryPack(List<EntryInfo> infos, Partitioning entries) {
    Objects.requireNonNull(infos, "null infos");
    this.entries = Objects.requireNonNull(entries, "null entries").readOnlyView();
    
    final int count = infos.size();
    
    if (entries.getParts() != 2 * count)
      throw new IllegalArgumentException(
          "infos/entries size ratio must be 1:2, actual was " + count + ":" + entries.getParts());
    
    EntryInfo[] ei = new EntryInfo[count];
    for (int index = 0; index < count; ++index)
      ei[index] = new MetaInfo(infos.get(index), index);
    
    this.infos = Lists.asReadOnlyList(ei);
  }
  

  @Override
  public List<EntryInfo> availableEntries() {
    return infos;
  }
  
  

  @Override
  public Entry entry(long rowNumber) {
    List<Long> rowNumbers = Lists.map(infos, i -> i.rowNumber());
    int index = Collections.binarySearch(rowNumbers, rowNumber);
    if (index < 0)
      throw new IllegalArgumentException("no entry available for row " + rowNumber);
    
    
    ByteBuffer contents = entries.getPart(2 * index + 1);
    EntryInfo info = infos.get(index);
    return new Entry(contents, info);
  }
  
  
  private class MetaInfo extends EntryInfo {
    
    private final int index;

    MetaInfo(EntryInfo copy, int index) {
      super(copy);
      this.index = 2 * index;
    }

    @Override
    public boolean hasMeta() {
      return !entries.isPartEmpty(index);
    }

    @Override
    public String meta() {
      
      ByteBuffer utf8 = entries.getPart(index);
      if (!utf8.hasRemaining())
        return super.meta();
      
      byte[] b = new byte[utf8.remaining()];
      utf8.get(b);
      
      return new String(b, Strings.UTF_8);
    }
    
    
    
    
  }

}














