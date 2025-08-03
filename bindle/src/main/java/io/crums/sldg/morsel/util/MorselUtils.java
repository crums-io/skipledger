/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.morsel.util;


import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import io.crums.sldg.morsel.ForeignRefs;
import io.crums.sldg.morsel.LedgerId;
import io.crums.sldg.morsel.MalformedMorselException;
import io.crums.sldg.morsel.Morsel;
import io.crums.sldg.morsel.MugBase;
import io.crums.sldg.morsel.MultiPath;
import io.crums.sldg.morsel.Nug;
import io.crums.sldg.morsel.Nugget;
import io.crums.sldg.morsel.tc.NotaryPack;
import io.crums.sldg.src.SourcePack;
import io.crums.sldg.src.SourcePackBuilder;

/**
 * 
 */
public class MorselUtils {
  
  
  
  public static Morsel mapIds(Morsel morsel, Map<LedgerId, LedgerId> mappings) {
    if (mappings.isEmpty())
      return Objects.requireNonNull(morsel, "null morsel");
    var editor =
        morsel instanceof MorselIdEditor e ? e : new MorselIdEditor(morsel);
    for (var entry : mappings.entrySet())
      editor.edit(entry.getKey(), entry.getValue());
    
    return editor;
  }
  
  
  
  
//  public record NuggetDiff(
//      LedgerId a, LedgerId b, long commonNo, long conflictNo) {
//    
//    public NuggetDiff {
//      Objects.requireNonNull(b, "b");
//      if (a.equals(b))
//        throw new IllegalArgumentException(
//            "ledger IDs a and b must not be equal: " + a);
//      if (commonNo < 0L)
//        throw new IllegalArgumentException("commonNo " + commonNo);
//      if (conflictNo < 0L)
//        throw new IllegalArgumentException("conflictNo " + conflictNo);
//      if (conflictNo != 0L && commonNo >= conflictNo)
//        throw new IllegalArgumentException(
//            "commonNo (%d) must be less than (non-zero) forkNo (%d)"
//            .formatted(commonNo, conflictNo));
//    }
//    
//    
//    
//    
//    
//    public boolean conflict() {
//      return conflictNo != 0L;
//    }
//    
//    public boolean forked() {
//      return conflictNo != 0L && commonNo != 0L;
//    }
//    
//    
//    public boolean isEmpty() {
//      return commonNo == 0L && conflictNo == 0L;
//    }
//    
//    
//  }
  
  
  
  
  /**
   * Collects and returns a reduced version of the given morsel containing
   * the given "principal" ledger IDs ({@code ids}) and their references.
   * 
   * <h4>Collected Nuggets</h4>
   * <p>
   * The nuggets for the given {@code ids} are collected as-is (no information
   * is removed). Any other nuggets these "principal" nuggets reference (via
   * {@linkplain Nugget#refIds()} or {@linkplain Nugget#notaryIds()}) are also
   * collected, but in <em>reduced form</em>: reduced nuggets do not contain any
   * references themselves, and their source packs, if present,
   * ({@linkplain Nugget#sourcePack()}) contain only the source rows referenced
   * from the "principal" nuggets: other source rows in the orginal nugget
   * are dropped.
   * </p>
   * <h4>No Validation</h4>
   * <p>
   * The returned {@linkplain Morsel} is guaranteed to be valid only if the
   * argument {@code morsel} is valid. What little validation performed is
   * structural in nature: the given morsel's proofs are not validated.
   * </p>
   * 
   * 
   * @param morsel      assumed well-formed and valid
   * @param ids         a selection of IDs (no duplicates) from
   *                    {@code morsel.}{@linkplain Morsel#ids() ids()}
   *                    
   * @return a tear-out of {@code morsel}
   * 
   * @throws MalformedMorselException
   *         if {@code morsel} is discovered to be malformed
   */
  public static Morsel reduce(Morsel morsel, Collection<LedgerId> ids)
      throws MalformedMorselException {
    
    if (ids.isEmpty())
      throw new IllegalArgumentException("empty ids");
    
    HashMap<LedgerId, Nugget> targets = new HashMap<>();
    for (var id : ids) {
      var nugget = morsel.getNugget(id);
      if (targets.put(nugget.id(), nugget) != null)
        throw new IllegalArgumentException(
            "duplicate ID %s in arguments %s".formatted(id, ids));
      
    }
    
    if (ids.size() == morsel.ids().size())
      return morsel;

    // gather the referenced ledger (nugget) IDs, plus any referenced
    // source row no.s. That is, if there are no source no.s, then
    // then the value is the empty set.
    HashMap<LedgerId, Set<Long>> referencedSources = new HashMap<>();
    
    for (var nugget : targets.values()) {
      
      for (var timechain : nugget.notaryIds())
        referencedSources.put(timechain, Set.of());
      
      for (ForeignRefs refs : nugget.refPacks()) {
        
        if (targets.containsKey(refs.foreignId()))
          continue;
        
        var foreignSrcNos = refs.foreignSourceNos();
        
        Set<Long> rowNos = referencedSources.get(refs.foreignId());
        
        if (foreignSrcNos.isEmpty()) {
          if (rowNos == null)
            referencedSources.put(refs.foreignId(), Set.of());
          continue;
        }
        
        assert !refs.foreignId().type().commitsOnly();
        
        if (rowNos == null || rowNos.isEmpty()) {
          rowNos = new HashSet<>();
          referencedSources.put(refs.foreignId(), rowNos);
        }
        
        rowNos.addAll(foreignSrcNos);
      }
    }
    
    HashMap<LedgerId, Nugget> referencedNuggets = new HashMap<>();
    
    for (var entry : referencedSources.entrySet()) {
      
      Nugget nugget = morsel.getNugget(entry.getKey());

      SourcePack sourcePack;
      var sourceNos = entry.getValue();
      
      if (sourceNos.isEmpty())
        sourcePack = null;
      
      else {
        
        var pack = nugget.sourcePack().orElseThrow(
            () -> new MalformedMorselException(
                "no source rows for ledger %s (referenced from ledger set %s)"
                .formatted(nugget.id(), ids)));
        
        final var packNos = pack.sourceNos();
        
        if (packNos.size() > sourceNos.size()) {
          
          var packBuilder = new SourcePackBuilder(pack.saltScheme());
          for (var no : sourceNos) {
            var srcRow = pack.findSourceByNo(no).orElseThrow(() ->
              new MalformedMorselException(
                "source row [%d] for ledger %s not found (referenced from ledger set %s)"
                .formatted(no, nugget.id(), ids)));
            
            packBuilder.add(srcRow);
          }
          sourcePack = packBuilder.build();
          
        } else {
          
          sourceNos.removeAll(packNos);
          if (!sourceNos.isEmpty())
            throw new MalformedMorselException(
              "source rows for ledger %s not found (referenced from ledger set %s): missing %s"
              .formatted(nugget.id(), ids, sourceNos));
          
          sourcePack = pack;
        }
      }
      
      referencedNuggets.put(nugget.id(), new ReducedNug(nugget, sourcePack));
      
    } // for (var entry
    
    var allNuggets = referencedNuggets;
    allNuggets.putAll(targets);
    
    return new MugBase(allNuggets.keySet()) {

      @Override
      public Nugget getNugget(LedgerId id) throws IllegalArgumentException {
        Nugget nugget = allNuggets.get(id);
        if (nugget == null)
          throw new IllegalArgumentException("unknown id " + id);
        return nugget;
      }
      
    };
  }
  
  
  private static class ReducedNug implements Nug {
    
    private final LedgerId id;
    private final MultiPath paths;
    private final SourcePack sourcePack;
    
    ReducedNug(Nugget nugget, SourcePack sourcePack) {
      this.id = nugget.id();
      this.paths = nugget.paths();
      this.sourcePack = sourcePack;
    }

    @Override
    public LedgerId id() {
      return id;
    }

    @Override
    public MultiPath paths() {
      return paths;
    }

    @Override
    public Optional<SourcePack> sourcePack() {
      return Optional.ofNullable(sourcePack);
    }

    @Override
    public List<NotaryPack> notaryPacks() {
      return List.of();
    }

    @Override
    public List<ForeignRefs> refPacks() {
      return List.of();
    }
    
  }

}
