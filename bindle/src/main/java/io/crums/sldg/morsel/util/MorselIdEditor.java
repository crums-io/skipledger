/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.morsel.util;


import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.Predicate;

import io.crums.sldg.morsel.LedgerId;
import io.crums.sldg.morsel.LedgerInfo;
import io.crums.sldg.morsel.Morsel;
import io.crums.sldg.morsel.Mug;
import io.crums.sldg.morsel.Nugget;
import io.crums.util.Lists;

/**
 * Edits the {@linkplain LedgerId}s of an exisiting {@linkplain Morsel}.
 * <p>
 * This class does not implement {@linkplain Mug} because it does not guarantee
 * the order of the returned {@linkplain #ids()}.
 * </p>
 * 
 * @see #editAlias(LedgerId, String)
 * @see #editUri(LedgerId, URI)
 * @see #editDescription(LedgerId, String)
 * @see #edit(LedgerId, LedgerId)
 */
public class MorselIdEditor implements Morsel {
  
  /** Base-to-mapped ids. */
  private final HashMap<LedgerId, LedgerId> baseToMappedIds = new HashMap<>();
  /** Mapped-to-base ids. Invariant: same size as {@linkplain #baseToMappedIds}. */
  private final HashMap<LedgerId, LedgerId> mappedToBaseIds = new HashMap<>();
  
  
  protected final Morsel base;
  
  private boolean reordered;

  /**
   * Constructs an instance over the given {@code base} morsel.
   */
  public MorselIdEditor(Morsel base) {
    this.base = base;
    if (base.ids().isEmpty())
      throw new IllegalArgumentException("empty base morsel: " + base);
  }
  
  
  /**
   * Changes the given ledger ID's alias and returns the result
   * as a new ID (or this instance, if nothing changed).
   * 
   * @param id          existing ID (one of {@linkplain #ids()}
   * @param alias       new alias, not blank
   * 
   * @return a new ID, if changed; {@code this}, otherwide
   * @throws IllegalArgumentException
   *         if another {@code LedgerId} has the same alias
   */
  public LedgerId editAlias(LedgerId id, String alias)
      throws IllegalArgumentException {

    return editInfo(id, id.info().alias(alias));
  }
  
  /**
   * Changes the given ledger ID's alias and returns the result
   * as a new ID (or this instance, if nothing changed).
   * 
   * @param id          existing ID (one of {@linkplain #ids()}
   * @param uri         new URI, may be {@code null}
   * 
   * @return a new ID, if changed; {@code this}, otherwide
   * @throws IllegalArgumentException
   *         if another {@code LedgerId} has the same URI
   */
  public LedgerId editUri(LedgerId id, URI uri)
      throws IllegalArgumentException {
    
    return editInfo(id, id.info().uri(uri));
  }
  
  /**
   * Changes the given ledger ID's description and returns the result
   * as a new ID (or this instance, if nothing changed).
   * 
   * @param id          existing ID (one of {@linkplain #ids()}
   * @param desc        description ({@code null} or blank means none)
   * 
   * @return a new ID, if changed; {@code this}, otherwide
   */
  public LedgerId editDescription(LedgerId id, String desc)
      throws IllegalArgumentException {
    
    return editInfo(id, id.info().description(desc));
  }
  
  
  
  /**
   * Edits the given ID's info and returns the result.
   * 
   * @param id          existing ID (one of {@linkplain #ids()}
   * @param newInfo     new info (same as existing
   *                    {@linkplain LedgerInfo#type() type})
   * 
   * @return a new ID, if changed; {@code this}, otherwide
   * 
   * @see #edit(LedgerId, LedgerId)
   */
  public LedgerId editInfo(LedgerId id, LedgerInfo newInfo)
      throws IllegalArgumentException {
    
    if (id.info().equals(newInfo))
      return id;
    
    
    LedgerId to = new LedgerId(id.id(), newInfo);
    
    edit(id, to);
    
    return to;
  }
  
  
  /**
   * Edits (maps) an existing ID {@code from} to a new ID {@code to} and returns
   * {@code true} iff anything was changed. It's okay to re-edit an ID.
   * 
   * @param from        one of {@linkplain #ids()}
   * @param to          new (target) ID
   * 
   * @return {@code !from.equals(to)}
   * 
   * @throws IllegalArgumentException
   *         if {@code from} is not one of the elements returned
   *         by {@linkplain #ids()}
   */
  public boolean edit(LedgerId from, LedgerId to)
      throws IllegalArgumentException {
    
    if (from.equals(to))
      return false;
    
    from.verifyEdit(to);
    {
      boolean aliasChanged = !from.alias().equals(to.alias());
      boolean uriChanged = !from.uri().equals(to.uri());
      boolean intIdChanged = from.id() != to.id();
      
      Predicate<LedgerId> fp = (id) -> id.equals(from);
      if (aliasChanged)
        fp = fp.or((id) -> id.alias().equals(to.alias()));
      if (uriChanged)
        fp = fp.or((id) -> id.uri().equals(to.uri()));
      if (intIdChanged)
        fp = fp.or((id) -> id.id() == to.id());
      
      var checkList = ids().stream().filter(fp).toList();
      if (!checkList.contains(from))
        throw new IllegalArgumentException("unknown <from> argument " + from);
      if (checkList.size() > 1) {
        var conflicts = new ArrayList<LedgerId>(checkList);
        conflicts.remove(from);
        throw new IllegalArgumentException(
            "to-ID %s (from-ID %s) conflicts with 1 or more existing IDs: %s"
            .formatted(to, from, conflicts));
      }
      reordered |= intIdChanged;
    }
    
    LedgerId baseId;
    {
      var bid = mappedToBaseIds.remove(from);
      baseId = bid == null ? from : bid;
    }
    
    if (baseId.equals(to)) {
      baseToMappedIds.remove(baseId);
      if (baseToMappedIds.isEmpty())
        reordered = false;
    } else {
      baseToMappedIds.put(baseId, to);
      mappedToBaseIds.put(to, baseId);
    }
    
    return true;
  }
  

  /**
   * Returns the edited (mapped) IDs.
   */
  @Override
  public List<LedgerId> ids() {
    return Lists.map(base.ids(), this::baseToMapped);
  }
  

  /**
   * @return an {@linkplain IdMappedNugget}
   */
  @Override
  public Nugget getNugget(LedgerId id) throws IllegalArgumentException {
    
    return
        new IdMappedNugget(
            base.getNugget(mappedToBaseIds.getOrDefault(id, id)),
            this::baseToMapped);
  }
  
  
  /**
   * Returns {@code true} iff something is edited.
   */
  public final boolean isEdited() {
    return !baseToMappedIds.isEmpty();
  }
  
  
  /**
   * Returns {@code true} iff one or more IDs are renumbered.
   * 
   * @see LedgerId#id()
   */
  public final boolean isReordered() {
    if (!reordered)
      return false;
    
    for (var e : baseToMappedIds.entrySet())
      if (e.getKey().id() != e.getValue().id())
        return true;
    
    reordered = false;
    return false;
  }
  
  
  
  
  private LedgerId baseToMapped(LedgerId id) {
    return baseToMappedIds.getOrDefault(id, id);
  }

}
