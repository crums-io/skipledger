/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.bindle.util;


import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

import io.crums.sldg.bindle.LedgerId;
import io.crums.sldg.bindle.Bindle;
import io.crums.sldg.bindle.BindleFile;
import io.crums.sldg.bindle.Nugget;
import io.crums.sldg.bindle.ObjectNug;

/**
 * Simple {@linkplain Nugget} cache designed for lazy-loading
 * {@linkplain Bindle}s. The cache itself may optionally use
 * {@linkplain WeakReference}s, in which case cached
 * {@linkplain Nugget}s may be evicted by the garbage collector.
 * "Weak" versions of the are apt for large {@linkplain BindleFile}s;
 * for already loaded-in-memory (but lazy) bindles, there's little
 * benefit in using the weak references.
 * 
 * @see CachingBindle#CachingBindle(Bindle, boolean)
 * @see #isWeak()
 * @see #clearCache(LedgerId)
 * @see #clearCache()
 */
public class CachingBindle implements Bindle {
  
  
  /** Presumably lazy bindle whose return values are cached. */
  protected final Bindle base;
  
  /**
   *  Cached {@linkplain Nugget}. Either {@linkplain HashMap}, or
   *  {@linkplain WeakHashMap}.
   */
  protected final Map<Integer, Nugget> cache;
  
  
  /**
   * Constructs an ordinary (not weak) instance.
   * 
   * @param base  not empty, lazy bindle whose return values will be cached
   * 
   * @see CachingBindle#CachingBindle(Bindle, boolean)
   */
  public CachingBindle(Bindle base) {
    this(base, false);
  }

  /**
   * Full constructor.
   * 
   * @param base  not empty, lazy bindle whose return values will be cached
   * @param weak  if {@code true}, then the cache will use a {@linkplain WeakHashMap}
   * 
   * @see CachingBindle#CachingBindle(Bindle)
   */
  public CachingBindle(Bindle base, boolean weak) {
    this.base = base;
    this.cache = weak ? new WeakHashMap<>() : new HashMap<>();
    if (base.ids().isEmpty())
      throw new IllegalArgumentException("empty base bindle " + base);
  }
  
  /** Returns {@code true} iff this instance's cache uses weak references. */
  public final boolean isWeak() {
    return cache instanceof WeakHashMap;
  }

  
  @Override
  public final List<LedgerId> ids() {
    return base.ids();
  }
  
  
  

  /** @see #transformNugget(Nugget) */
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
   * Subclass hook for transforming the base bindle (for e.g., to
   * an {@linkplain ObjectNug} instance), before it's returned and
   * cached.
   * 
   * @param nugget the object returned by the base bindle
   * 
   * @return base implementation does not transform: {@code nugget} argument (as-is)
   */
  protected Nugget transformNugget(Nugget nugget) {
    return nugget;
  }

}
