/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.util.ledger;


import io.crums.client.repo.TrailRepo;
import io.crums.util.hash.Digest;

/**
 * 
 */
public class Constants {
  
  private Constants() {  }
  
  
  
  public final static Digest DEF_DIGEST = SkipLedger.DEF_DIGEST;
  
  
  public final static String DB_EXT = ".sldg";
  
  public final static String DB_LEDGER = "ledger";
  
  public final static String DB_CT_IDX = TrailRepo.IDX_FILE;
  
  public final static String DB_CT_BLOB = TrailRepo.BLOB_FILE;

}
