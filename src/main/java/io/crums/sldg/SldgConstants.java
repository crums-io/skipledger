/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg;

import io.crums.client.repo.TrailRepo;
import io.crums.util.hash.Digest;
import io.crums.util.hash.Digests;

/**
 * Library constants.
 */
public class SldgConstants {
  

  
  /**
   * Digest used by the libary is staticly defined here. Currently SHA-256.
   * 
   * @see Digests#SHA_256.
   */
  public final static Digest DIGEST = Digests.SHA_256;
  
  /**
   * Digest hash width in bytes (32). Derived from {@linkplain #DIGEST DIGEST.hashWidth()}.
   */
  public static final int HASH_WIDTH = DIGEST.hashWidth();
  

  
  public final static String DB_EXT = ".sldg";
  
  public final static String DB_LEDGER = "ledger";
  
  public final static String DB_CT_IDX = TrailRepo.IDX_FILE;
  
  public final static String DB_CT_BLOB = TrailRepo.BLOB_FILE;

  public final static int DEF_TOOTHED_WIT_COUNT = 8;

  public final static int MAX_WITNESS_EXPONENT = 62;

  public final static int MAX_BLOCK_WITNESS_COUNT = 65;

  private SldgConstants() {  }

}
