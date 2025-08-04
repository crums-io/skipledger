/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.bindle;


import java.nio.ByteBuffer;
import java.util.function.Function;

import io.crums.io.buffer.Partition;

/**
 * Lazy-loading, unverified, {@linkplain Bindle}.
 * There are 2 concrete implementations. These are, in order of laziness:
 * <ol>
 * <li>In-memory: {@linkplain LazyBun}, marshalled from raw bytes</li>
 * <li>File-based: {@linkplain BindleFile}. Much like the in-memory version,
 * except that the raw bytes are read from file.</li>
 * </ol>
 * 
 * <h2>What's Verified</h2>
 * <p>
 * Altho unverified, a reference to an instance still implies certain
 * guarantees. In particular, it indicates the serialized instance is
 * structurally OK "on the outside"; details need to be checked "on the inside".
 * These structural guarantees include:
 * </p>
 * <ol>
 * <li>{@linkplain #ids()} are well-formed, both individually and collectively,
 * sorted in order of unique integral IDs ({@linkplain LedgerId#id()}). Aliases
 * ({@linkplain LedgerId#alias()}) and URIs ({@linkplain LedgerId#uri()} too are
 * unique.</li>
 * <li>Nugget data ({@linkplain Nug}s) is organized in a random-access structure
 * called a partition: the structure is loaded and verified to include sufficient
 * "parts".</li>
 * <li>{@linkplain #getNugget(LedgerId)} returns a structurally well-formed
 * nugget whose ID ({@linkplain Nugget#id()} is properly resolved. The returned
 * object too is otherwise lazy-loading.</li>
 * </ol>
 * <h2>What's <em>Not</em> Verifed</h2>
 * <p>
 * Pretty much everything else.
 * </p>
 * <h2>No Caching</h2>
 * <p>
 * This class performs no-caching.
 * </p>
 * 
 * @see #getNugget(LedgerId)
 * @see ObjectBundle
 */
public abstract class LazyBindle extends BundleBase {
  
  
  protected final Partition nuggets;
  
  /** Set to {@linkplain #idLookup()}. */
  protected final Function<Integer, LedgerId> idLookup;

  
  LazyBindle(LedgerId[] ids, Partition nuggets) {
    super(ids);
    verifyIds();
    this.nuggets = nuggets;
    this.idLookup = idLookup();
    
    assert ids.length <= nuggets.getParts();
  }
  

  /**
   * <p>
   * Returns a {@linkplain LazyNugget} instance with the given {@code id}.
   * </p><p>
   * Note, tho I prefer layering to class hierarchies, the returned type is
   * not narrowed (to type {@code LazyNugget}), so as not to get in the way
   * of subclassing.
   * </p>
   * 
   * @see #idLookup
   * @see LazyNugget#load(Function, ByteBuffer)
   */
  @Override
  public Nugget getNugget(LedgerId id)
      throws IllegalArgumentException, MalformedNuggetException {
    return LazyNugget.load(idLookup, nuggets.getPart(indexOf(id)));
  }

  

}




















