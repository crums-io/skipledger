/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.packs;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import io.crums.sldg.ByteFormatException;
import io.crums.sldg.PathInfo;
import io.crums.sldg.bags.PathBag;
import io.crums.util.Lists;

/**
 * <p>{@linkplain PathBag} implementation.</p>
 * 
 * <h2>Serial Format</h2>
 * <p>
 * The serial form is a concatenation of variable size {@linkplain PathInfo}s,
 * with the number of these as premable.
 * </p>
 * <pre>{@code
 *    PATH_CNT    := SHORT                              // number of declared paths (<= 64k)
 *    
 *    // per PathInfo
 *    ROW_CNT     := SHORT                              // declarations too are brief
 *    META_LEN    := SHORT
 *    
 *    PATH_INFO   := ROW_CNT META_LEN [RN ^ROW_CNT] [BYTE ^META_LEN]
 *                   
 *    
 *    PATH_PACK   := PATH_CNT [PATH_INFO ^PATH_CNT]     // but note ea PATH_INFO is var-width
 *    }
 * </pre>
 * 
 */
public class PathPack implements PathBag {
  
  
  public final static PathPack EMPTY = new PathPack(Collections.emptyList());
  
  
  public static PathPack load(ByteBuffer in) {
    final int count = 0xffff & in.getShort();
    if (count == 0)
      return EMPTY;
    
    PathInfo[] infos = new PathInfo[count];
    try {
      
      for (int index = 0; index < count; ++index)
        infos[index] = PathInfo.load(in);
    
    } catch (IllegalArgumentException iax) {
      throw new ByteFormatException("on PathPack.load(): " + iax, iax);
    }
    
    List<PathInfo> declaredPaths = Lists.asReadOnlyList(infos);
    
    return new PathPack(declaredPaths);
  }
  
  
  
  
  private final List<PathInfo> declaredPaths;
  
  
  private PathPack(List<PathInfo> declaredPaths) {
    this.declaredPaths = declaredPaths;
  }
  
  
  @Override
  public List<PathInfo> declaredPaths() {
    return declaredPaths;
  }
  

}
