/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.bindle;


import java.net.URI;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;


/**
 * Meta info about a log.
 */
public final class LogInfo extends LedgerInfo {
  
  

  /**
   * 
   * 
   * @param alias       locally unique name
   * @param uri         optional (may be {@code null})
   * @param desc        optional description ({@code null} or blank counts
   *                    for naught)
   */
  public LogInfo(String alias, URI uri, String desc) {
    this(new StdProps(LedgerType.LOG, alias, uri, desc));
  }
  
  
  public LogInfo(StdProps props) {
    super(props);
    if (props.type() != LedgerType.LOG)
      throw new IllegalArgumentException(
          "expected type %s; actual %s -- props: %s"
          .formatted(LedgerType.LOG, props.type(), props));
  }
  


  @Override
  LedgerInfo edit(StdProps props) {
    return new LogInfo(props);
  }


  @Override
  public int serialSize() {
    return props.serialSize();
  }


  @Override
  public ByteBuffer writeTo(ByteBuffer out) throws BufferOverflowException {
    props.writeTo(out);
    return out;
  }
  
  
  /**
   * Stubbed out in case more there's more to read in future versions
   * Presently does not read anything.
   */
  static LogInfo load(StdProps props, ByteBuffer in) {
    return new LogInfo(props);
  }
  

}
