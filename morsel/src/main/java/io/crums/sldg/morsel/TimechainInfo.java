/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.morsel;


import java.net.URI;
import java.util.Objects;
import java.util.Optional;

import io.crums.tc.ChainParams;
import io.crums.tc.client.RemoteChain;

/**
 * Information about a timechain.
 * 
 * @see #params()
 */
public class TimechainInfo extends LedgerInfo {
  
  
  private final ChainParams params;

  
  /**
   * Creates an instance with the given chain parameters.
   * 
   * @param params      chain parameters
   * @param alias       locally unique name
   * @param origin      optional, but recommended (may have to be {@code null},
   *                    if the timechain is private)
   * @param desc        optional description ({@code null} or blank counts
   *                    for naught)
   * @throws IllegalArgumentException
   *         if {@code origin} is not null and is malformed (per the timechain
   *         protocol, it must be of the form {@code http[s]://hostname[:port]})
   */
  public TimechainInfo(ChainParams params, String alias, URI origin, String desc) {
    super(LedgerType.TIMECHAIN, alias, origin, desc);
    this.params = Objects.requireNonNull(params, "null params");
    if (origin != null)
      RemoteChain.checkHostUri(origin);
  }
  
  
  /** Returns the chain parameters (inception, block resolution, etc). */
  public final ChainParams params() {
    return params;
  }
  
  
  /**
   * Returns the timechain host URI, if known.
   * 
   * @return {@linkplain #uri()}
   */
  public final Optional<URI> origin() {
    return uri();
  }

}







