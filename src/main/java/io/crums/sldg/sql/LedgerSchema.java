/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.sql;

import java.util.HashSet;
import java.util.Objects;

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
 * <h2>Trail chain table</h2>
 * <p>
 * The Merkle proof in a crumtrail (witness record) contains a proof-chain. This proof chain
 * contains a variable number of SHA-256 hash-nodes. Each hash-node in a proof takes one row
 * in this table.
 * </p>
 * <pre>
 * {@code CREATE TABLE} <em>chainTable</em>
 *  {@code (chn_id INT NOT NULL,
 *   n_hash CHAR(43) NOT NULL,
 *   PRIMARY KEY (chn_id)
 *  )}</pre>
 * 
 * <h2>Trail table</h2>
 * <pre>
 * {@code CREATE TABLE} <em>trailTable</em>
 *  {@code (trl_id INT NOT NULL,
 *   row_num BIGINT NOT NULL,
 *   utc BIGINT NOT NULL,
 *   mrkl_idx INT NOT NULL,
 *   mrkl_cnt INT NOT NULL,
 *   chain_len INT NOT NULL,
 *   chn_id BIGINT NOT NULL,
 *   PRIMARY KEY (trl_id),
 *   FOREIGN KEY (row_num) REFERENCES <em>ledgerTable</em>(row_num),
 *   FOREIGN KEY (chn_id) REFERENCES <em>chainTable</em>(chn_id),
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
  
  

  public final static String BCN_ID = "bcn_id";
  public final static String CHN_ID = "chn_id";
  public final static String TRL_ID = "trl_id";
  public final static String N_HASH = "n_hash";
  
  public final static String MRKL_IDX = "mrkl_idx";
  public final static String MRKL_CNT = "mrkl_cnt";
  public final static String CHAIN_LEN = "chain_len";
  
  
  public final static String UTC = "utc";
  
  
  
  public final static String BIGINT_TYPE = "BIGINT";
  public final static String INT_TYPE = "INT";
  
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
        SOFT_TAB +    ROW_NUM  + ' ' + BIGINT_TYPE + " NOT NULL,\n" +
        SOFT_TAB +    SRC_HASH + ' ' + BASE64_TYPE + " NOT NULL,\n" +
        SOFT_TAB +    ROW_HASH + ' ' + BASE64_TYPE + " NOT NULL,\n" +
        SOFT_TAB +    "PRIMARY KEY (" + ROW_NUM + ")\n)";
  }
  
  
  
  
  
  
  
  public static String createChainTableStatement(String chainTable) {
    return
        "CREATE TABLE " + chainTable + " (\n" +
        SOFT_TAB +    CHN_ID + ' ' + INT_TYPE + " NOT NULL,\n" +
        SOFT_TAB +    N_HASH + ' ' + BASE64_TYPE + " NOT NULL,\n" +
        SOFT_TAB +    "PRIMARY KEY (" + CHN_ID + ")\n)";
  }
  
  
  public static String createTrailTableStatement(String trailTable, String ledgerTable, String chainTable) {
    return
        "CREATE TABLE " + trailTable + " (\n" +
        SOFT_TAB +    TRL_ID    + ' ' + INT_TYPE + " NOT NULL,\n" +
        SOFT_TAB +    ROW_NUM   + ' ' + BIGINT_TYPE + " NOT NULL,\n" +
        SOFT_TAB +    UTC       + ' ' + BIGINT_TYPE + " NOT NULL,\n" +
        SOFT_TAB +    MRKL_IDX  + ' ' + INT_TYPE + " NOT NULL,\n" +
        SOFT_TAB +    MRKL_CNT  + ' ' + INT_TYPE + " NOT NULL,\n" +
        SOFT_TAB +    CHAIN_LEN + ' ' + INT_TYPE + " NOT NULL,\n" +
        SOFT_TAB +    CHN_ID    + ' ' + INT_TYPE + " NOT NULL,\n" +
        SOFT_TAB +    "PRIMARY KEY (" + TRL_ID + "),\n" +
        SOFT_TAB +    "FOREIGN KEY (" + ROW_NUM + ") REFERENCES " + ledgerTable + "(" + ROW_NUM + "),\n" +
        SOFT_TAB +    "FOREIGN KEY (" + CHN_ID  + ") REFERENCES " + chainTable + "(" + CHN_ID + ")\n)";
  }
  
  
  
  /**
   * Default extension for skip ledger table.
   */
  public final static String LEDGER_TBL_EXT = "_sldg";


  /**
   * Default extension for [trail] chains table.
   */
  public final static String CHAIN_TBL_EXT = LEDGER_TBL_EXT + "_chains";
  /**
   * Default extension for trails table.
   */
  public final static String TRAIL_TBL_EXT = LEDGER_TBL_EXT + "_trails";
  
  
  
  
  
  
  
  private final String sourceTable;
  private final String ledgerTable;
  private final String chainsTable;
  private final String trailsTable;
  
  
  /**
   * Creates a new instance using the default naming scheme to derive ledger table names
   * from the source table's name.
   * 
   * @param sourceTable   the name of the source table (for which we're building a ledger)
   * 
   * @see #LEDGER_TBL_EXT
   * @see #BEACON_TBL_EXT
   * @see #CHAIN_TBL_EXT
   * @see #TRAIL_TBL_EXT
   */
  public LedgerSchema(String sourceTable) {
    this(
        sourceTable,
        sourceTable + LEDGER_TBL_EXT,
        sourceTable + CHAIN_TBL_EXT,
        sourceTable + TRAIL_TBL_EXT);
  }

  /**
   * Creates a new instance with the given table names. <em>Not recommended</em> for ordinary
   * use, since it's easy to get the arguments mixed up.
   * 
   * @param sourceTable   the name of the source table (for which we're building a ledger)
   * @param ledgerTable   the name of the ledger table
   * @param beaconsTable  the name of the beacons table
   * @param chainsTable   the name of the chains table (merkle proof chains)
   * @param trailsTable   the name of the trails table (crumtrails)
   */
  protected LedgerSchema(String sourceTable, String ledgerTable, String chainsTable, String trailsTable) {
    this.sourceTable = Objects.requireNonNull(sourceTable, "null sourceTable").trim();
    this.ledgerTable = Objects.requireNonNull(ledgerTable, "null ledgerTable").trim();
    this.chainsTable = Objects.requireNonNull(chainsTable, "null chainsTable").trim();
    this.trailsTable = Objects.requireNonNull(trailsTable, "null trailsTable").trim();
    
    checkForm(this.sourceTable, "sourceTable");
    checkForm(this.ledgerTable, "ledgerTable");
    checkForm(this.chainsTable, "chainsTable");
    checkForm(this.trailsTable, "trailsTable");
    
    checkNoDupNames();
  }




  private void checkForm(String table, String descName) {
    if (table.length() < 2)
      throw new SqlLedgerException("table name for " + descName + " too short: '" + table + "'");
  }
  
  
  
  private void checkNoDupNames() {
    HashSet<String> names = new HashSet<>();
    names.add(sourceTable);
    if (!names.add(ledgerTable))
      throw dupTableX(ledgerTable, "ledgerTable");
    if (!names.add(chainsTable))
      throw dupTableX(chainsTable, "chainsTable");
    if (!names.add(trailsTable))
      throw dupTableX(trailsTable, "trailsTable");
    names.clear();
  }
  
  
  private SqlLedgerException dupTableX(String dupName, String tableCategory) {
    return new SqlLedgerException("table name '" + dupName + "' for " + tableCategory + " is already used");
  }
  

  
  

  
  
  
  /**
   * Returns the table name.
   */
  public final String getSourceTable() {
    return sourceTable;
  }

  /**
   * Returns the table name.
   */
  public final String getLedgerTable() {
    return ledgerTable;
  }

  /**
   * Returns the table name.
   */
  public final String getChainsTable() {
    return chainsTable;
  }

  /**
   * Returns the table name.
   */
  public final String getTrailsTable() {
    return trailsTable;
  }
  
  
  
  
  
  
  
  
  
  
  
  

}
  
  
  
  
  
  
  
  
  
  
  
