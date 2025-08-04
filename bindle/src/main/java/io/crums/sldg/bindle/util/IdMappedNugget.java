/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.bindle.util;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import io.crums.sldg.bindle.ForeignRefs;
import io.crums.sldg.bindle.LedgerId;
import io.crums.sldg.bindle.MultiPath;
import io.crums.sldg.bindle.Nugget;
import io.crums.sldg.bindle.tc.NotaryPack;
import io.crums.sldg.src.SourcePack;
import io.crums.util.Lists;

/**
 * ID-mapped {@linkplain Nugget}. Used by {@linkplain BindleIdEditor}; seldom
 * used directly.
 * <p>
 * This maps not just {@linkplain Nugget#id()},
 * but also the {@linkplain LedgerId}s embedded in the return values of the
 * {@linkplain #notaryPacks()} and {@linkplain #refPacks()} methods. <em>All 
 * mappings are performed lazily</em>. In turn, this means modifications
 * to the mapping function {@linkplain #idMapper} are seen by existing
 * instances.
 * </p>
 * 
 * @see IdMappedNugget#IdMappedNugget(Nugget, Function)
 */
public class IdMappedNugget implements Nugget {
  
  /** Base nugget. */
  protected final Nugget base;
  /** {@linkplain LedgerId} mapping function. */
  protected final Function<LedgerId, LedgerId> idMapper;

  /**
   * Constructs a view of the given {@code base} instance using the
   * specified ID-mapping function.
   * 
   * @param base        nugget to be transformed
   * @param idMapper    {@linkplain LedgerId} mapping function
   */
  public IdMappedNugget(Nugget base, Function<LedgerId, LedgerId> idMapper) {
    this.base = Objects.requireNonNull(base, "null base");
    this.idMapper = Objects.requireNonNull(idMapper, "null idMapper");
  }

  @Override
  public LedgerId id() {
    return idMapper.apply(base.id());
  }

  @Override
  public MultiPath paths() {
    return base.paths();
  }

  @Override
  public Optional<SourcePack> sourcePack() {
    return base.sourcePack();
  }

  @Override
  public List<NotaryPack> notaryPacks() {
    return Lists.map(base.notaryPacks(), np -> np.editChainId(idMapper));
  }

  @Override
  public List<ForeignRefs> refPacks() {
    return Lists.map(base.refPacks(), fr -> fr.editForeignId(idMapper));
  }

}
