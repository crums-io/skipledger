/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.packs;


import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import io.crums.io.Serial;
import io.crums.sldg.PathInfo;
import io.crums.sldg.bags.PathBag;
import io.crums.util.Lists;

/**
 * A mutable {@code PathBag} whose serial form can be loaded as an immutable {@linkplain PathPack}.
 */
public class PathPackBuilder implements PathBag, Serial {
  
  /**
   * The maximum number of declared paths.
   */
  public final static int MAX_COUNT = 0xffff;
  
  
  
  private List<PathInfo> declaredPaths = new ArrayList<>();
  
  
  
  /**
   * Adds the given path declaration, if it doesn't already exist.
   * <p>
   * Note adding <em>n</em> of these takes <em>n</em><sup><small>2</small></sup> operations. We
   * don't expect that many declared paths (our file format maxes out at 64k), but even if usage
   * ever nears that magnitude, we'll need to consider sorting this in order improve performance.
   * </p>
   * 
   * @return {@code true} <b>iff</b> the given {@code path} was added
   */
  public boolean addDeclaredPath(PathInfo path) {
    
    synchronized (declaredPaths) {
      
      if (declaredPaths.contains(path))
        return false;
      
      if (declaredPaths.size() == MAX_COUNT)
        throw new IllegalStateException("instance is maxed out at " + MAX_COUNT + " declared paths");
        
      declaredPaths.add(path);
      return true;
    }
  }
  
  
  /**
   * Removes the given {@code path}, if it exists, from this collection.
   * 
   * @return {@code true} <b>iff</b> the {@code path} was removed
   */
  public boolean removeDeclaredPath(PathInfo path) {
    synchronized (declaredPaths) {
      return declaredPaths.remove(path);
    }
  }
  
  
  
  /**
   * Adds all the data from the given {@code pathPack} to this instance. When invoked on an
   * empty instance, fewer checks are necessary and is consequently more efficient.
   * 
   * @return the number of {@linkplain PathInfo}s added
   */
  public int addPathPack(PathPack pathPack) {
    Objects.requireNonNull(pathPack, "null pathPack");
    
    int newPathCount;
    synchronized (declaredPaths) {
      if (declaredPaths.isEmpty()) {
        declaredPaths.addAll(pathPack.declaredPaths());
        newPathCount = declaredPaths.size();
      } else {
        int[] count = { 0 };
        pathPack.declaredPaths().forEach(p -> { if (addDeclaredPath(p)) ++count[0]; } );
        newPathCount = count[0];
      }
    }
    return newPathCount;
  }

  @Override
  public List<PathInfo> declaredPaths() {
    synchronized (declaredPaths) {
      return Lists.readOnlyCopy(declaredPaths);
    }
  }

  @Override
  public int serialSize() {
    int bytes = 2;
    synchronized (declaredPaths) {
      for (PathInfo decl : declaredPaths)
        bytes += decl.serialSize();
    }
    return bytes;
  }

  /**
   * @see PathPack
   */
  @Override
  public ByteBuffer writeTo(ByteBuffer out) {
    
    synchronized (declaredPaths) {
      out.putShort((short) declaredPaths.size());
      for (PathInfo decl : declaredPaths)
        decl.writeTo(out);
    }
    return out;
  }

}







