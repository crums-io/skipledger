/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.bindle;


import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.function.Function;

import io.crums.util.Lists;

/**
 * Base class for the {@linkplain Bindle}
 * implementations {@linkplain LazyBindle}, {@linkplain ObjectBundle},
 * and {@linkplain BindleFile}. This class implements the {@linkplain Bundle}
 * invariant that {@linkplain Bundle#ids() ids()} are sorted.
 * 
 * @see #ids()
 */
public abstract class BundleBase implements Bindle {

  private final LedgerId[] ids;
  

  /**
   * Builder constructor. ({@linkplain BindleBuilder#ids()}
   * are pre-validated, sorted by {@linkplain LedgerId#id()}).
   */
  BundleBase(BindleBuilder builder) {
    if (builder.isEmpty())
     throw new IllegalArgumentException("empty builder");
    var bIds = builder.ids();
    this.ids = bIds.toArray(new LedgerId[bIds.size()]);
  }
  
  
  /**
   * Copies and sorts ids.
   * 
   * @param ids not empty; no duplicate aliases, nor duplicate URIs.
   * @throws MalformedBindleException
   *         if any of the aliases, or URIs are duplicates
   */
  protected BundleBase(Collection<LedgerId> ids) throws MalformedBindleException {
    this.ids = ids.toArray(new LedgerId[ids.size()]);
    Arrays.sort(this.ids, Bundle.ID_COMP);
    verifyIds();
  }
  
  
  /** For subclass private constructors. Not validated */
  protected BundleBase(LedgerId[] ids) {
    this.ids = ids;
  }
  
  /** For copy constructor (if any). */
  protected BundleBase(BundleBase copy) {
    this.ids = copy.ids;
  }
  
  
  /**
   * Verifies the list of {@linkplain #ids()} is well-formed.
   * 
   * @throws MalformedBindleException
   *         if there are duplicate numeric {@linkplain LedgerId#id() id}s;
   *         if there are duplicate {@linkplain LedgerId#alias() alias}es;
   *         if there are duplicate {@linkplain LedgerId#uri() URI}s;
   *         
   */
  protected final void verifyIds() throws MalformedBindleException {
    if (ids.length == 0)
      throw new MalformedBindleException("empty bindle");
    
    HashSet<String> aliases = new HashSet<>(ids.length);
    HashSet<URI> uris = new HashSet<>(ids.length);
    
    for (int lastId = Integer.MIN_VALUE, index = 0;
        index < ids.length;
        ++index) {
      
      var id = ids[index];
      
      if (!aliases.add(id.alias()))
        throw new MalformedBindleException(
            "duplicate ledger ID aliases '%s': %s"
            .formatted(id.alias(), ids()));
      if (id.uri().isPresent() && !uris.add(id.uri().get()))
        throw new MalformedBindleException(
            "duplicate ledger ID URIs '%s': %s"
            .formatted(id.uri().get(), ids()));
      if (id.id() == lastId)    // recall, this.ids is sorted
        throw new MalformedBindleException(
            "duplicate numeric ledger IDs %d: %s and %s"
            .formatted(id.id(), ids[index - 1], id));
      
      lastId = id.id();
    }
  }
  
  
  /**
   * Returns the index of {@code id} in {@linkplain #ids()}.
   * 
   * @return &ge; 0
   * @throws IllegalArgumentException
   *         if the {@code id} value is not one {@linkplain #ids()}
   */
  protected final int indexOf(LedgerId id) throws IllegalArgumentException {
    int index = Arrays.binarySearch(ids, id, Bundle.ID_COMP);
    if (index < 0 || !ids[index].equals(id))
      throw new IllegalArgumentException("unknown id " + id);
    return index;
  }
  

  /**
   * Returns a validated list of IDS per the requirements
   * of the {@linkplain Bundle} sub-interface.
   * 
   * @return ordered by integral ids ({@linkplain LedgerId#id()})
   */
  @Override
  public final List<LedgerId> ids() {
    return Lists.asReadOnlyList(ids);
  }
  
  

  /**
   * Returns a {@linkplain LedgerId} look-up function for this instance's
   * {@linkplain #ids()}. The returned function throws
   * {@linkplain MalformedBindleException}s on look-up failure.
   */
  protected final Function<Integer, LedgerId> idLookup() {
    return idLookup(ids);
  }
  
  
  /**
   * Returns a {@linkplain LedgerId} look-up function using the given
   * sorted array of IDs. The returned function throws
   * {@linkplain MalformedBindleException}s on look-up failure.
   * 
   * @param ids not empty, sorted by {@linkplain LedgerId#id()}
   */
  static final Function<Integer, LedgerId> idLookup(LedgerId[] ids) {
    
    int[] idarray = new int[ids.length];
    for (int index = ids.length; index-- > 0; )
      idarray[index] = ids[index].id();
    
    return new Function<>() {
      @Override
      public LedgerId apply(Integer i) {
        int index = Arrays.binarySearch(idarray, i);
        if (index < 0)
          throw new MalformedBindleException(
              "unknown integral ledger id " + i);
        return ids[index];
      }
    };
  }
  


}
