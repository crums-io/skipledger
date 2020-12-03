/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.util.ledger;


import java.security.MessageDigest;

import io.crums.util.hash.Digests;

/**
 * Layers in <tt>MessageDigest</tt> re-use via a thread-local. Unit tests
 * indicate this is quite useless. Leaving this here as PoC.
 */
public final class Sha256ThreadLocalLedger extends FilterLedger {
  
  private final static ThreadLocal<MessageDigest> WORK_DIGEST =
      new ThreadLocal<>() {
        @Override
        protected MessageDigest initialValue() {
          return Digests.SHA_256.newDigest();
        }
      };

      
  
  
  /**
   * Constructs a wrapped instance.
   * 
   * @param ledger
   */
  public Sha256ThreadLocalLedger(SkipLedger ledger) {
    super(ledger);
    if (!ledger.hashAlgo().equals(Digests.SHA_256.hashAlgo()))
      throw new IllegalArgumentException(
          "base ledger hash algo must be SHA-256; actual is " + ledger.hashAlgo());
  }




  /**
   * Returns a thread-local SHA-256 digest.
   */
  @Override
  protected MessageDigest digest() {
    return WORK_DIGEST.get();
  }

}
