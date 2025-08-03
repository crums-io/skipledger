/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.morsel.util;


import java.util.HashMap;
import java.util.List;
import java.util.WeakHashMap;

import io.crums.sldg.morsel.LedgerId;
import io.crums.sldg.morsel.Morsel;
import io.crums.sldg.morsel.Nugget;
import io.crums.sldg.morsel.ObjectNug;

/**
 * 
 */
public class CachingMorsel implements Morsel {
  
  /**
   * 
   */
  protected final WeakHashMap<Integer, Nugget> cache = new WeakHashMap<>();
  
  /** Presumably lazy morsel whose return values are cached. */
  protected final Morsel base;
  

  /**
   * Full constructor.
   * 
   * @param base  not empty, lazy morsel whose return values will be cached
   */
  public CachingMorsel(Morsel base) {
    this.base = base;
    if (base.ids().isEmpty())
      throw new IllegalArgumentException("empty base morsel " + base);
  }

  
  @Override
  public final List<LedgerId> ids() {
    return base.ids();
  }

  
  @Override
  public final Nugget getNugget(LedgerId id) throws IllegalArgumentException {
    synchronized (cache) {
      Nugget nugget = cache.get(id.id());
      if (nugget == null) {
        nugget = transformNugget(base.getNugget(id));
        assert nugget.id().equals(id);
        cache.put(id.id(), nugget);
      } else if (!nugget.id().equals(id))
        throw new IllegalArgumentException("unknown ID " + id);
      
      return nugget;
    }
  }
  

  /**
   * Clears the cache for the specified ledger nugget.
   * 
   * @param id ID of the nugget to be cleared from cache
   * 
   * @return {@code true} iff anything changed
   * 
   * @see #clearCache()
   */
  public boolean clearCache(LedgerId id) {
    synchronized (cache) {
      return cache.remove(id.id()) != null;
    }
  }
  
  /**
   * Clears the cache entirely.
   * 
   * @return {@code true} iff anything changed
   * 
   * @see #clearCache(LedgerId)
   */
  public boolean clearCache() {
    synchronized (cache) {
      if (cache.isEmpty())
        return false;
      cache.clear();
      return true;
    }
  }
  
  
  /**
   * Subclass hook for transforming the base nugget. For e.g., to
   * an {@linkplain ObjectNug} instance, before it's returned and
   * cached.
   * 
   * @param nugget the object returned by the base nugget
   * 
   * @return {@code nugget} argument (as-is)
   */
  protected Nugget transformNugget(Nugget nugget) {
    return nugget;
  }

}
