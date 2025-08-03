/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.morsel;


import java.net.URI;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;

import io.crums.sldg.morsel.LedgerInfo.StdProps;
import io.crums.tc.ChainParams;
import io.crums.tc.client.RemoteChain;

/**
 * Information about a timechain.
 * 
 * @see #params()
 */
public final class TimechainInfo extends LedgerInfo {
  
  
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
    super(new StdProps(LedgerType.TIMECHAIN, alias, origin, desc));
    this.params = Objects.requireNonNull(params, "null params");
    if (origin != null)
      RemoteChain.checkHostUri(origin);
  }
  
  
  
  public TimechainInfo(ChainParams params, StdProps props) {
    super(props);
    this.params = Objects.requireNonNull(params, "null params");
    
    if (props.type() != LedgerType.TIMECHAIN)
      throw new IllegalArgumentException(
          "expected type %s; actual given is %s"
          .formatted(LedgerType.TIMECHAIN, props.type()));
    if (props.uri() != null)
      RemoteChain.checkHostUri(props.uri());
  }
  
  
  
  
  
  @Override
  LedgerInfo edit(StdProps props) {
    return new TimechainInfo(params, props);
  }



  /** Returns the chain parameters (inception, block resolution, etc). */
  public final ChainParams params() {
    return params;
  }
  
  
  /**
   * @return {@linkplain #params()}
   */
  @Override
  protected Object otherProperties() {
    return params;
  }
  
  
  /**
   * Returns the timechain host URI, if known.
   * 
   * @return {@linkplain #uri()}
   */
  public Optional<URI> origin() {
    return uri();
  }


  @Override
  public int serialSize() {
    return props.serialSize() + params.serialSize();
  }


  @Override
  public ByteBuffer writeTo(ByteBuffer out) throws BufferOverflowException {
    props.writeTo(out);
    params.writeTo(out);
    return out;
  }
  
  
  static TimechainInfo load(StdProps props, ByteBuffer in) {
    var params = ChainParams.load(in);
    return new TimechainInfo(params, props);
  }



  @Override
  public boolean isValidEdit(LedgerInfo newInfo) {
    return
        newInfo.type().isTimechain() &&
        ((TimechainInfo) newInfo).params().equalParams(params);
  }



  @Override
  public void verifyEdit(LedgerInfo newInfo) throws IllegalArgumentException {
    super.verifyEdit(newInfo);
    var otherParams = ((TimechainInfo) newInfo).params();
    if (!this.params.equalParams(otherParams))
      throw new IllegalArgumentException(
          "attempt to edit timechain params: from %s to %s"
          .formatted(this.params, otherParams));
  }

}







