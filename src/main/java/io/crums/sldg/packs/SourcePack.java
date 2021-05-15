/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.packs;


import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import io.crums.sldg.ByteFormatException;
import io.crums.sldg.bags.SourceBag;
import io.crums.sldg.src.SourceRow;
import io.crums.util.Lists;

/**
 * Immutable {@linkplain SourceBag} implementation.
 * 
 * <h2>Serial Foramt</h2>
 * <p>
 * <pre>
 *    SRC_CNT   := SHORT
 *    
 *    // per row
 *    COL_CNT   := SHORT
 *    // pre column
 *    COL_TYPE  := BYTE         // 
 *    COL_VALUE := <em>function_of</em>( COL_TYPE ) // TODO: document this tedius bit
 *    
 *    COLUMN    := COL_TYPE COL_VALUE
 *    ROW       := RN [COLUMN ^COL_CNT]
 *    
 *    SRC_PACK  := ROW ^SRC_CNT
 * </pre>
 * </p>
 * 
 */
public class SourcePack implements SourceBag {
  
  
  public final static SourcePack EMPTY = new SourcePack(Collections.emptyList());
  
  
  public static SourcePack load(ByteBuffer in) {
    final int count = 0xffff & in.getShort();
    if (count == 0)
      return EMPTY;
    
    SourceRow[] src = new SourceRow[count];
    long lastRn = 0;
    
    for (int index = 0; index < count; ++index) {
      src[index] = SourceRow.load(in);
      long rn = src[index].rowNumber();
      if (lastRn >= rn)
        throw new ByteFormatException("out-of-sequence row numbers " + lastRn + ", " + rn);
      lastRn = rn;
    }
    
    return new SourcePack(Lists.asReadOnlyList(src));
  }
  
  
  
  
  private final List<SourceRow> sources;
  
  
  private SourcePack(List<SourceRow> sources) {
    this.sources = sources;
  }

  @Override
  public List<SourceRow> sources() {
    return sources;
  }

}
