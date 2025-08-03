/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.morsel;


import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import io.crums.io.SerialFormatException;
import io.crums.sldg.HashConflictException;
import io.crums.sldg.Row;
import io.crums.sldg.morsel.tc.NotarizedRow;
import io.crums.sldg.morsel.tc.NotaryPack;
import io.crums.sldg.src.SourcePack;
import io.crums.sldg.src.SourceRow;

/**
 * A fully-resolved, validated {@linkplain Nugget}. Objects in a nugget
 * may refer to data in <em>other</em> ledger nuggets in a morsel. Some of
 * the information in a nugget, therefore, is context- (morsel) specific.
 * 
 * <h2>What's Validated</h2>
 * <p>
 * A reference to an instance of this class implies the following guarantees.
 * </p>
 * <ol>
 * <li>For every {@linkplain SourceRow}, there is a full skipledger [commitment]
 * row in {@linkplain #paths()} at the same row no.: the skipledger row's
 * <em>input</em>-hash equals the {@linkplain SourceRow#hash()}.
 * <li>For every <em>from</em>-cell {@linkplain Reference} in
 * {@linkplain #refPacks()}, the nugget contains the corresponding
 * {@code SourceRow} with the relevant cell values unredacted.
 * </li>
 * <li>The witnessed hash in every {@linkplain NotarizedRow}, is equal
 * to the row-hash (the skipledger commitment hash) at that row no. 
 * (as advertised by
 * {@linkplain #paths()}.{@linkplain MultiPath#rowHash(long) rowHash(..)}).
 * </li>
 * </ol>
 * <h3>What an Instance Does <em>Not</em> Guarantee</h3>
 * <p>
 * Individually, a reference to an instance of this class does not contain
 * sufficient information for validating cross-nugget references. Cross-nugget
 * validation is delegated to the container, the {@linkplain Morsel}-layer
 * implementation. The 2 categories of cross-nugget validation are:
 * </p>
 * <ol>
 * <li>{@linkplain ForeignRefs Foreign References}: these link the contents
 * of one row in a ledger to those of another ledger. To be fully validated,
 * a {@linkplain Reference}'s <em>to</em>-cell value in the foreign nugget
 * must be verified to equal the <em>from</em>-cell value[s] indicated in
 * the reference.
 * </li>
 * <li>{@linkplain #notaryPacks() Notaries}: these reference foreign
 * timechain nuggets. To be fully validated, each referenced timechain nugget
 * must contain sufficient information (the full row) establishing that the
 * {@linkplain NotarizedRow#cargoHash() cargo hash} matches the timechain's
 * input-hash at that block (row).
 * 
 * </li>
 * </ol>
 * <p>
 * The above missing guarantees <em>may</em> be provided by the {@code Morsel}
 * implementation class.
 * </p>
 * 
 * @see ObjectMug
 */
public class ObjectNug implements Nug {
  
  private final LedgerId id;
  private final MultiPath paths;
  private final SourcePack sources;
  private final List<NotaryPack> notaries;
  private final List<ForeignRefs> refs;
  
  
  /**
   * 
   * @param id
   * @param paths
   * @param sources
   * @param notaries
   * @param refs
   * @throws IllegalArgumentException
   * @throws HashConflictException
   */
  public ObjectNug(
      LedgerId id,
      MultiPath paths, SourcePack sources,
      List<NotaryPack> notaries, List<ForeignRefs> refs)
  
          throws IllegalArgumentException, HashConflictException {
    
    this.id = Objects.requireNonNull(id, "id");
    this.paths = Objects.requireNonNull(paths, "paths");
    this.sources = sources;
    this.notaries = List.copyOf(notaries);
    this.refs = List.copyOf(refs);
    verify();
  }
  
  
  
  /**
   * Interface copy-constructor. Properties are copied and validated.
   * 
   * @see #ObjectNug(ObjectNug)
   */
  public ObjectNug(Nugget copy) {
    this(
        copy.id(),
        copy.paths(),
        copy.sourcePack().orElse(null),
        copy.notaryPacks(),
        copy.refPacks());
  }
  
  
  /**
   * Copy constructor.
   */
  public ObjectNug(ObjectNug copy) {
    this.id = copy.id;
    this.paths = copy.paths;
    this.sources = copy.sources;
    this.notaries = copy.notaries;
    this.refs = copy.refs;
  }
  
  
  private void verify()
      throws MalformedNuggetException, HashConflictException  {
    
    if (sources != null) {
      
      if (id.type().commitsOnly())
        throw new IllegalArgumentException(
            id + ": ledger type " + id.type() + " cannot have source rows");
      
      for (var srcRow : sources.sources()) {
        final long rowNo = srcRow.no();
        
        Row row = paths.findRow(rowNo).orElseThrow(
            () -> new IllegalArgumentException(
                "%s: input (source-row) hash for %s not found in any commitment path"
                .formatted(id, srcRow)));
        
        if (!row.inputHash().equals(srcRow.hash()))
          throw new HashConflictException(
              "%s: source-row hash conflicts with committed input hash: %s"
              .formatted(id, srcRow));
      }
    }
    
    // verify notary ids are unique, and that each witnessed row
    // hash is in fact known to one of the paths (MultiPath)
    // (the existence of the timechain blockproofs is verified elsewhere)
    if (!notaries.isEmpty()) {
      Set<Integer> idSet = new HashSet<>();
      idSet.add(id.id());
      for (var notary : notaries) {
        if (!idSet.add(notary.chainId().id()))
          throw new MalformedNuggetException(
              "%s: duplicate notary chain id [%d]: %s"
              .formatted(id, notary.chainId().id(), notary.chainId()));
        for (NotarizedRow nr : notary.notarizedRows()) {
          if (!nr.rowHash().equals(paths.rowHash(nr.rowNo())))
            throw new HashConflictException(
              "%s: notarized row hash conflicts with existing hash for row [%d]"
              .formatted(id, nr.rowNo()));
        }
      }
    }
    
    // verify foreign refs
    if (!refs.isEmpty()) {
      
      // check allowed, in principle..
      
      if (id.info().type().commitsOnly())
        throw new MalformedNuggetException(
            "%s: ledger type %s cannot have foreign references (%d)"
            .formatted(id, id.type(), refs.size()));
      
      if (sources == null)
        throw new MalformedNuggetException(
            id + ": foreign references w/o source pack");

      
      for (ForeignRefs foreignRefs : refs) {
        // disallow self-references (for now)
        if (foreignRefs.foreignId().id() == id.id())
          throw new MalformedNuggetException(
              "%s: (foreign) ledger id [%d] is self-reference: %s"
              .formatted(id, foreignRefs.foreignId().id(), foreignRefs.foreignId()));
        
        // verify source-row exists and required content is not redacted
        
        Reference last = Reference.FIRST_KEY;
        boolean firstRefKeyUsed = false;
        
        for (Reference ref : foreignRefs.refs()) {
          
          if (last.compareTo(ref) >= 0) {
            if (last != Reference.FIRST_KEY || firstRefKeyUsed)
              throw new MalformedNuggetException(
                  "%s: out-of-order reference sequence; %s after %s"
                  .formatted(id, ref, last));
            firstRefKeyUsed = true;
          }
          
          last = ref;
          
          SourceRow srcRow = sources.findSourceByNo(ref.fromRow()).orElseThrow(
            () -> new MalformedNuggetException(
                "%s: source row [%d] for foreign ref %s:%s not found"
                .formatted(
                    id, ref.fromRow(), foreignRefs.foreignId(), ref.toRow())));
          
          if (ref.sameContent()) {
            if (srcRow.hasRedactions())
              throw new MalformedNuggetException(
                "%s: same-content reference to %s:%d from redacted source-row: %s"
                .formatted(id, foreignRefs.foreignId(), ref.toRow(), srcRow));
            
          } else if (ref.fromCol() >= srcRow.cells().size()) {
            throw new MalformedNuggetException(
              "%s: fromCol %d out of bounds (cc %d) for source-row %s: %s:%s"
              .formatted(
                  id, ref.fromCol(), srcRow.cells().size(), srcRow,
                  ref, foreignRefs.foreignId()));
          } else if (srcRow.cells().get(ref.fromCol()).isRedacted())
            throw new MalformedNuggetException(
              "%s: cell %d in %s is redacted: %s:%s"
              .formatted(
                  id, ref.fromCol(), srcRow, ref, foreignRefs.foreignId()));
        }
      }
    }
    
    
  }
  
  
  /**
   * Builder constructor.
   */
  public ObjectNug(NuggetBuilder builder) {
    this.id = builder.id();
    this.paths = builder.paths();
    this.sources = builder.sourcePack().orElse(null);
    this.notaries = builder.notaryPacks();
    this.refs = builder.refPacks();
  }
  
  

  @Override
  public final LedgerId id() {
    return id;
  }

  @Override
  public final MultiPath paths() {
    return paths;
  }
  
  @Override
  public final Optional<SourcePack> sourcePack() {
    return Optional.ofNullable(sources);
  }

  @Override
  public final List<NotaryPack> notaryPacks() {
    return notaries;
  }

  @Override
  public final List<ForeignRefs> refPacks() {
    return refs;
  }

  
  
  
  public static ObjectNug load(
      Function<Integer, LedgerId> idLookup, ByteBuffer in)
          throws BufferUnderflowException, SerialFormatException {
    
    var lazy = LazyNugget.load(idLookup, in);
    try {
      
      return new ObjectNug(lazy);
    
    } catch (Exception x) {
      throw new SerialFormatException(x);
    }
  }
  
  
  
}



















