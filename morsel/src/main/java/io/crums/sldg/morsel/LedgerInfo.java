/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.morsel;


import java.net.URI;
import java.util.Objects;
import java.util.Optional;

/**
 * Ledger meta information, not necessarily validated.
 * 
 */
public abstract class LedgerInfo {
  
  private final LedgerType type;
  
  private final String alias;
  
  private final URI uri;
  
  private final String desc;

  
  /**
   * 
   * @param type        not null
   * @param alias       locally unique name (trimmed)
   * @param uri         optional (may be {@code null})
   * @param desc        optional description ({@code null} or blank counts
   *                    for naught)
   */
  LedgerInfo(LedgerType type, String alias, URI uri, String desc) {
    this.type = Objects.requireNonNull(type, "null type");
    this.alias = alias.trim();
    if (this.alias.isEmpty())
      throw new IllegalArgumentException("blank or empty alias");
    this.uri = uri;
    this.desc = desc;
  }
  
  
  
  /**
   * Returns the ledger type.
   * 
   * @return not null
   */
  public final LedgerType type() {
    return type;
  }
  
  /**
   * Returns the locally unique name for this ledger.
   * 
   * @return neither empty or nor null
   */
  public final String alias() {
    return alias;
  }
  
  
  public Optional<URI> uri() {
    return Optional.ofNullable(uri);
  }
  
  public Optional<String> description() {
    return nonBlankOpt(desc);
  }
  
  
  static Optional<String> nonBlankOpt(String value) {
    return value != null && !value.isBlank() ? Optional.of(value) : Optional.empty();
  }

}







