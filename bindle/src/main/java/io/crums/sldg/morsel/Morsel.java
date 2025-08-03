/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.morsel;


import java.net.URI;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * A collection of entries from ledgers and their proofs.
 */
public interface Morsel {
  
  
  /**
   * Returns the ledger IDs.
   * 
   * @return not-empty list
   * 
   * @see #findIdByAlias(String)
   * @see #findIdByUri(URI)
   */
  List<LedgerId> ids();
  
  
  /**
   * Returns the ledger ID by its alias name. Each ID has a locally
   * unique alias.
   * 
   * @param alias the ledger's alias name
   * 
   * @throws NoSuchElementException
   *         if this morsel contains no such ledger by that {@code alias}
   * 
   * @see #findIdByAlias(String)
   * @see LedgerId#alias()
   */
  default LedgerId idByAlias(String alias) throws NoSuchElementException {
    return findIdByAlias(alias).get();
  }
  

  /**
   * Finds and returns the ledger ID with the given alias, if found.
   * 
   * @param alias the ledger's alias name
   * 
   * 
   * @see #idByAlias(String)
   * @see LedgerId#alias()
   */
  default Optional<LedgerId> findIdByAlias(String alias) {
    return
        ids().stream().filter(id -> id.info().alias().equals(alias))
        .findAny();
  }
  
  
  /**
   * Finds and returns the ledger ID with the given URI, if found. Not all
   * ledgers have URIs.
   * 
   * @see LedgerId#uri()
   */
  default Optional<LedgerId> findIdByUri(URI uri) {
    var asOpt = Optional.of(uri);
    return
        ids().stream().filter(id -> id.info().uri().equals(asOpt))
        .findAny();
  }
  
  
  
  /**
   * Returns the nugget of the ledger with given id.
   * 
   * @param id  one of the {@linkplain #ids()}
   * 
   * @throws IllegalArgumentException
   *         if {@code id} does not belong to this morsel (not one {@linkplain #ids()})
   */
  Nugget getNugget(LedgerId id) throws IllegalArgumentException;
  
  
  
  /**
   * Returns the nugget of the ledger with the given {@code alias}.
   * 
   * 
   * @param alias the ledger's alias name
   * 
   * @return {@code getNugget(idByAlias(alias))}
   * 
   * @throws NoSuchElementException
   *         if this morsel contains no such ledger by that {@code alias}
   * 
   * @see #ids()
   * @see LedgerId#alias()
   */
  default Nugget getNugget(String alias) throws NoSuchElementException {
    return getNugget(idByAlias(alias));
  }
  
  
  

}














