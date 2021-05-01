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

  public static final String MRSL_EXT = ".mrsl";
  
  public final static String DB_LEDGER = "ledger";
  
  public final static String DB_CT_IDX = TrailRepo.IDX_FILE;
  
  public final static String DB_CT_BLOB = TrailRepo.BLOB_FILE;

  public final static int DEF_TOOTHED_WIT_COUNT = 8;

  public final static int MAX_WITNESS_EXPONENT = 62;

  public final static int MAX_BLOCK_WITNESS_COUNT = 65;
  
  /**
   * Version byte used in file headers.
   */
  public final static byte VERSION_BYTE = 1;
  
  /**
   * Version number used in json files.
   */
  public final static int VERSION = VERSION_BYTE;
  
  /**
   * Used in json.
   */
  public final static String VERSION_TAG = "version";

  
  /**
   * Nugget binary file extension (includes the dot).
   */
  public final static String NUG_EXT = ".nug";
  
  
  /**
   * Path binary file extension (includes the dot).
   */
  public final static String SPATH_EXT = ".spath";
  
  
  /**
   * JSON file extension (includes the dot). The JSON version of an entity
   * has this extension appended to its binary-version extension.
   */
  public final static String JSON_EXT = ".json";
  

  public static final String NUG_JSON_EXT = NUG_EXT + JSON_EXT;
  

  public static final String SPATH_JSON_EXT = SPATH_EXT + JSON_EXT;
  
  

  /**
   * Logger name.
   */
  public static final String LOGGER_NAME = "skipledger";
  
  
  
  
  
  
  

  private SldgConstants() {  }

}

















