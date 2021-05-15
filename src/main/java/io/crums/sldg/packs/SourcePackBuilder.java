/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.packs;


import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;

import io.crums.io.Serial;
import io.crums.sldg.bags.SourceBag;
import io.crums.sldg.src.SourceRow;
import io.crums.util.Lists;

/**
 * Builds a {@linkplain SourcePack}. Holds a maximum of 64k source-rows.
 */
public class SourcePackBuilder implements SourceBag, Serial {
  
  private final TreeMap<Long, SourceRow> sources = new TreeMap<>();

  
  
  
  public SourceRow setSourceRow(SourceRow src) {
    Objects.requireNonNull(src, "null src");
    synchronized (sources) {
      Long rn = src.rowNumber();
      if (sources.size() < 0xffff || sources.containsKey(rn))
        return sources.put(rn, src);
    }
    throw new IllegalStateException("filled to capacity");
  }
  
  
  public boolean addSourceRow(SourceRow src) {
    Objects.requireNonNull(src, "null src");
    synchronized (sources) {
      Long rn = src.rowNumber();
      if (sources.containsKey(rn) || sources.size() == 0xffff)
        return false;
      sources.put(rn, src);
      return true;
    }
  }
  
  
  public int addAll(SourcePack pack) {
    Objects.requireNonNull(pack, "null pack");
    synchronized (sources) {
      int count = 0;
      for (var srcRow : pack.sources()) {
        if (addSourceRow(srcRow))
          ++count;
      }
      return count;
    }
  }
  
  
  @Override
  public int serialSize() {
    int size = 2;
    for (var src : sources.values())
      size += src.serialSize();
    return size;
  }

  @Override
  public ByteBuffer writeTo(ByteBuffer out) {
    synchronized (sources) {
      out.putShort((short) sources.size());
      for (var src : sources.values())
        src.writeTo(out);
      return out;
    }
  }

  @Override
  public List<SourceRow> sources() {
    synchronized (sources) {
      return Lists.readOnlyCopy(sources.values());
    }
  }
  
  
  

}
