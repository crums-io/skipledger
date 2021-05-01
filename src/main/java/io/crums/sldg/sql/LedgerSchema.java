/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.sql;

import io.crums.sldg.Ledger;
import io.crums.util.Base64_32;

/**
 * Schema for SQL TABLEs as backing storage for a {@linkplain Ledger}.
 * 
 * <h1>Schema</h1>
 * 
 * <h2>Ledger table</h2>
 * <pre>
 * {@code CREATE TABLE} <em>ledgerTable</em>
 *  {@code (row_num BIGINT NOT NULL,
 *   src_hash CHAR(43) NOT NULL,
 *   row_hash CHAR(43) NOT NULL,
 *   PRIMARY KEY (row_num)
 *  )}</pre>
 * <h3>Rationale</h3>
 * <p>
 * No reason to be married to this schema, but here are the reasons for this design:
 * <ol>
 * <li>BIGINT row_num type. The hash columns take 43 bytes each, an extra 4 bytes doesn't hurt.
 * Maybe the database can handle this many rows, who knows.</li>
 * <li>row_num <em>not</em> AUTO_INCREMENT. We want the to support trimming the ledger;
 * if they do (for whatever reason), resetting the counter so the {@code row_num}s have no
 * gaps is not possible in a standard way.</li>
 * <li>Hash columns as CHAR(43). This is more readable than the BINARY(32) data type. It's encoded
 * in a variant of the Base-64 encoding scheme tailored for 32-byte sequences. See {@linkplain Base64_32}.</li>
 * </ol>
 * </p>
 * 
 * <h2>Beacon table</h2>
 * <pre>
 * {@code CREATE TABLE} <em>beaconTable</em>
 *  {@code (row_num BIGINT NOT NULL,
 *   utc BIGINT NOT NULL,
 *   PRIMARY KEY (row_num),
 *   FOREIGN KEY (row_num) REFERENCES <em>ledgerTable</em>(row_num)
 *  )}</pre>
 * 
 * 
 * <h2>Trail chain table</h2>
 * <p>
 * The Merkle proof in a crumtrail (witness record) contains a proof-chain. This proof chain
 * contains a variable number of SHA-256 hash-nodes. Each hash-node in a proof takes one row
 * in this table.
 * </p>
 * <pre>
 * {@code CREATE TABLE} <em>chainTable</em>
 *  {@code (nid BIGINT NOT NULL,
 *   n_hash CHAR(43) NOT NULL,
 *   PRIMARY KEY (nid)
 *  )}</pre>
 * 
 * <h2>Trail table</h2>
 * <pre>
 * {@code CREATE TABLE} <em>trailTable</em>
 *  {@code (row_num BIGINT NOT NULL,
 *   utc BIGINT NOT NULL,
 *   mrkl_idx INT NOT NULL,
 *   mrkl_cnt INT NOT NULL,
 *   chain_len INT NOT NULL,
 *   nid BIGINT NOT NULL,
 *   PRIMARY KEY (row_num),
 *   FOREIGN KEY (row_num) REFERENCES <em>ledgerTable</em>(row_num),
 *   FOREIGN KEY (nid) REFERENCES <em>chainTable</em>(nid),
 *  )}</pre>
 * 
 * <p>
 * Note {@code chain_len} is redundant, since it can be calculated from {@code mrkl_idx} and {@code mrkl_cnt}:
 * we include it here so a DBA doesn't have to calculate it. Also, it could have been a TINYINT, since the chain
 * length never nears anything close to 128.
 * </p>
 * 
 */
public class LedgerSchema {
  
  public final static String ROW_NUM = "row_num";
  
  public final static String SRC_HASH = "src_hash";
  
  public final static String ROW_HASH = "row_hash";
  
  
  
  public final static String NID = "nid";
  public final static String N_HASH = "n_hash";
  
  public final static String MRKL_IDX = "mrkl_idx";
  public final static String MRKL_CNT = "mrkl_cnt";
  public final static String CHAIN_LEN = "chain_len";
  
  
  public final static String UTC = "utc";
  
  
  
  public final static String BIGINT_TYPE = "BIGINT";
  
  /**
   * {@linkplain Base64_32} encoding.
   */
  public final static String BASE64_TYPE = "CHAR(43)";
  
  
  
  
  private final static String SOFT_TAB = "  ";
  
  /**
   * Creates and returns the create-table SQL statement for the given ledger table name.
   * 
   * @see LedgerSchema schema
   */
  public static String createLedgerTableStatement(String ledgerTable) {
    return
        "CREATE TABLE " + ledgerTable + " (\n" +
        SOFT_TAB +    ROW_NUM + ' ' + BIGINT_TYPE + " NOT NULL,\n" +
        SOFT_TAB +    SRC_HASH + ' ' + BASE64_TYPE + " NOT NULL,\n" +
        SOFT_TAB +    ROW_HASH + ' ' + BASE64_TYPE + " NOT NULL,\n" +
        SOFT_TAB +    "PRIMARY KEY (" + ROW_NUM + ")\n)";
  }
  
  
  
  
  public static String createBeaconTableStatement(String beaconTable, String ledgerTable) {
    return
        "CREATE TABLE " + beaconTable + " (\n" +
        SOFT_TAB +    ROW_NUM + ' ' + BIGINT_TYPE + " NOT NULL,\n" +
        SOFT_TAB +    UTC + ' ' + BIGINT_TYPE + " NOT NULL,\n" +
        SOFT_TAB +    "PRIMARY KEY (" + ROW_NUM + "),\n" +
        SOFT_TAB +    "FOREIGN KEY (" + ROW_NUM + ") REFERENCES " + ledgerTable + "(" + ROW_NUM + ")\n)";
  }
  
  
  public static String createChainTableStatement(String chainTable) {
    return
        "CREATE TABLE " + chainTable + " (\n" +
        SOFT_TAB +    NID + ' ' + BIGINT_TYPE + " NOT NULL,\n" +
        SOFT_TAB +    N_HASH + ' ' + BASE64_TYPE + " NOT NULL,\n" +
        SOFT_TAB +    "PRIMARY KEY (" + NID + ")\n)";
  }
  
  
  public static String createTrailTableStatement(String trailTable, String ledgerTable, String chainTable) {
    return
        "CREATE TABLE " + trailTable + " (\n" +
        SOFT_TAB +    ROW_NUM + ' ' + BIGINT_TYPE + " NOT NULL,\n" +
        SOFT_TAB +    NID + ' ' + BIGINT_TYPE + " NOT NULL,\n" +
        SOFT_TAB +    N_HASH + ' ' + BASE64_TYPE + " NOT NULL,\n" +
        SOFT_TAB +    "PRIMARY KEY (" + ROW_NUM + "),\n" +
        SOFT_TAB +    "FOREIGN KEY (" + ROW_NUM + ") REFERENCES " + ledgerTable + "(" + ROW_NUM + "),\n" +
        SOFT_TAB +    "FOREIGN KEY (" + NID + ") REFERENCES " + chainTable + "(" + NID + ")\n)";
  }
  
  
  
  
  
  
  public final static String BCN_TBL_EXT = "_beacon";
  
  public final static String CHN_TBL_EXT = "_chain";
  
  public final static String TRL_TBL_EXT = "_trail";
  

}
  
  
  
  
  
  
  
  
  
  
  
