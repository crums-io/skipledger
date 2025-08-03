/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.morsel;


import java.net.URI;
import java.util.Objects;
import java.util.Optional;

/**
 * Intra-morsel numeric ID for a ledger. Its main purpose is to provide a
 * <em>typed</em> number (the ID).
 * 
 * <p>
 * When a ledger entry cross-references a row in another ledger via a row no.
 * and a hash pointer, the hash reference is augmented with a locally unique
 * ledger no. indicating which ledger the referenced row belongs to. In
 * principle, this is unnecessary: if there are <em>k</em>-many ledgers recorded
 * in a morsel, then it would take at most <em>k</em>-many look-ups (the row no.
 * in the other ledger is usually known) to match the referenced hash (pointer).
 * </p>
 */
public class LedgerId {
  
  
  private final int id;
  
  private final LedgerInfo info;
  

  /**
   * Full constructor.
   * 
   * @param id          &ge; 0
   * @param info        not null
   */
  public LedgerId(int id, LedgerInfo info) {
    this.id = id;
    if (id < 0)
      throw new IllegalArgumentException("negative id: " + id);
    this.info = Objects.requireNonNull(info, "null info");
  }
  
  
  
  
  
  /**
   * Returns the intra-morsel numeric ID. Unique across a morsel's ledgers,
   * but internal. This number may not survive in re-written versions of
   * the morsel.
   * 
   * @return &ge; 0
   */
  public final int id() {
    return id;
  }
  
  
  /**
   * Returns the ledger type.
   * 
   * @return {@code info().type()}
   */
  public final LedgerType type() {
    return info.type();
  }
  
  
  /**
   * Returns the ledger's alias. Unique across a morsel.
   * 
   * @return {@code info().alias()}
   */
  public final String alias() {
    return info.alias();
  }
  
  
  /**
   * Returns the ledger's URI. When present, it is unique across the morsel;
   * depending on type, it may also be globally unique (for example, a URL).
   * 
   * @return {@code info().uri()}
   */
  public final Optional<URI> uri() {
    return info.uri();
  }
  
  
  
  
  
  /**
   * Returns the type-specific info.
   * 
   * @see LedgerInfo#description()
   */
  public final LedgerInfo info() {
    return info;
  }
  
  
  
  /**
   * Verifies this ID can be changed to (is compatible with) the given
   * new ID. This, in turn, involves ensuring the ledger
   * {@linkplain #type() type} is not changed, and that an any edits
   * to the type-specific {@linkplain #info() info} are allowed (e.g.
   * you can't change the chain params of an ID that refers to a timechain.)
   * 
   * @param newId the new ID
   * @throws IllegalArgumentException
   *         thrown by {@code info().verifyEdit(newId.info())}
   *         
   * @see LedgerInfo#verifyEdit(LedgerInfo)
   */
  public void verifyEdit(LedgerId newId) throws IllegalArgumentException {
    info.verifyEdit(newId.info);
  }
  
  
  

  
  /**
   * Equality is determined by {@linkplain #id() id} and {@linkplain #info()}.
   */
  @Override
  public final boolean equals(Object o) {
    return
        o == this ||
        o instanceof LedgerId other &&
        other.id == id &&
        other.info.equals(info);
  }
  
  
  /**
   * Consistent with {@linkplain #equals(Object)}.
   * @return {@linkplain #id()}
   */
  @Override
  public final int hashCode() {
    return id;
  }
  
  
  @Override
  public String toString() {
    return "[" + id + ": " + type() + ": " +  alias() + "]";
  }
  

}





















