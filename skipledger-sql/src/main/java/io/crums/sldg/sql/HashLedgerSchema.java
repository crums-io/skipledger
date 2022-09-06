/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.sql;


import java.util.Objects;

import io.crums.sldg.HashLedger;
import io.crums.util.Base64_32;

/**
 * Schema for SQL TABLEs as backing storage for a {@linkplain HashLedger}.
 * The motivation here is to define a <em>portable</em> schema that hopefully
 * works on most any SQL database. That in turn dumbs it down for now (e.g. no
 * CASCADE ON DELETE or other vendor-specific niceties).
 * 
 * <h2>Schema</h2>
 * <p>
 * There are 3 tables in this design.
 * </p>
 * <ol>
 * <li><em>skip table</em>. Standalone table recording a sequence of hashes computed
 *     from some user defined table. It models the skip ledger data structure.</li>
 * <li><em>chain table</em>. Sequences of hashes recording merkle proofs in crumtrails.</li>
 * <li><em>trail table</em>. Crumtrail (witness) records. Each row references both of
 *     the above tables via FOREIGN KEYs.</li>
 * </ol>
 * <p>
 * Their individual schemas are detailed below.
 * </p>
 * 
 * <h3>Skip table</h3>
 * <pre>
 * 
 * {@code CREATE TABLE} <em>skipTable</em>
 *  {@code (row_num BIGINT NOT NULL,
 *   src_hash CHAR(43) NOT NULL,
 *   row_hash CHAR(43) NOT NULL,
 *   PRIMARY KEY (row_num)
 *  )}
 * </pre>
 * <h4>Rationale</h4>
 * <p>
 * No reason to be married to this schema, but here are the reasons for this design:
 * </p>
 * <ol>
 * <li>BIGINT row_num type. The hash columns take 43 bytes each, an extra 4 bytes doesn't hurt.
 * Maybe the database can handle this many rows, who knows.</li>
 * <li>row_num <em>not</em> AUTO_INCREMENT. We want the to support trimming the ledger;
 * if they do (for whatever reason), resetting the counter so the {@code row_num}s have no
 * gaps is not possible in a standard way.</li>
 * <li>Hash columns as CHAR(43). This is more readable than the BINARY(32) data type. It's encoded
 * in a variant of the Base-64 encoding scheme tailored for 32-byte sequences. See {@linkplain Base64_32}.</li>
 * </ol>
 * 
 * <h3>Trail Chain table</h3>
 * <p>
 * The Merkle proof in a crumtrail (witness record) contains a proof-chain. This proof chain
 * contains a variable number of SHA-256 hash-nodes. Each hash-node in a proof takes one row
 * in this table.
 * </p>
 * <pre>
 * 
 * {@code CREATE TABLE} <em>chainTable</em>
 *  {@code (chn_id INT NOT NULL,
 *   n_hash CHAR(43) NOT NULL,
 *   PRIMARY KEY (chn_id)
 *  )}</pre>
 * 
 * <h3>Trail table</h3>
 * <pre>
 * 
 * {@code CREATE TABLE} <em>trailTable</em>
 *  {@code (trl_id INT NOT NULL,
 *   row_num BIGINT NOT NULL, // psst, you might wanna index this
 *   utc BIGINT NOT NULL,
 *   mrkl_idx  INT NOT NULL,
 *   mrkl_cnt  INT NOT NULL,
 *   chain_len INT NOT NULL,
 *   chn_id    INT NOT NULL,
 *   PRIMARY KEY (trl_id),
 *   FOREIGN KEY (row_num) REFERENCES} <em>ledgerTable</em> {@code (row_num),
 *   FOREIGN KEY (chn_id) REFERENCES} <em>chainTable</em> {@code (chn_id),
 *  )}</pre>
 * 
 * <p>
 * Note {@code chain_len} is redundant, since it can be calculated from {@code mrkl_idx} and {@code mrkl_cnt}:
 * we include it here so a DBA doesn't have to calculate it. Also, it could have been a TINYINT, since the chain
 * length never nears anything close to 128.
 * </p>
 * 
 * <h3>Primary Keys</h3>
 * <p>
 * The primary key column values in each of the above tables range from 1 thru {@code count(*)}.
 * In other words, they're a stand-in for the SQL {@code ROW_NUMBER()} function. This constraint
 * is managed here at the application level, not by the DB.
 * </p>
 * 
 * <h3>No Indexes</h3>
 * <p>
 * Primary keys aside, no SQL indexes are defined here. Since these are performance related,
 * and there maybe vendor-specific ways of creating them, we kick this can down the road. A DBA
 * can always add an index later. (E.g. maybe a unique index on {@code trailTable.row_num}
 * if there are very many crumtrails recorded.)
 * </p>
 * 
 * <h3>Customizations</h3>
 * <p>
 * Besides their table names (their prefixes, actually), the exact table definitions may be tweaked
 * in one of two ways without any code changes.
 * </p>
 * <ol>
 * <li>By editing a configuration file.</li>
 * <li>By creating the tables directly on the database.</li>
 * </ol>
 * <p>
 * Regardless how its done, while you may experiment with their SQL type defs, the column
 * names must obviously remain the same. Also, note the exact SQL syntax only figures at creation
 * time; once the tables are setup, the application cares little about the details. 
 * </p>
 * 
 * @see Config
 * @see ConfigFileBuilder
 */
public class HashLedgerSchema {
  
  public final static String ROW_NUM = "row_num";
  
  public final static String SRC_HASH = "src_hash";
  
  public final static String ROW_HASH = "row_hash";
  
  

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
  
  
  
  public final static String PROTO_AUTOINCREMENT = "AUTO_INCREMENT";
  
  
  
  
  private final static String SOFT_TAB = "  ";
  
  /**
   * 
   */
  public static String defaultSkipTableSchema(String skipTable) {
    return
        "CREATE TABLE " + skipTable + " ( " +
        SOFT_TAB +    ROW_NUM  + ' ' + BIGINT_TYPE + " NOT NULL," +
        SOFT_TAB +    SRC_HASH + ' ' + BASE64_TYPE + " NOT NULL," +
        SOFT_TAB +    ROW_HASH + ' ' + BASE64_TYPE + " NOT NULL," +
        SOFT_TAB +    "PRIMARY KEY (" + ROW_NUM + ") )";
  }


  
  public static String protoSkipTableSchema(String tablePrefix) {
    return defaultSkipTableSchema(getSkipTable(tablePrefix));
  }
  
  
  public static String protoTrailTableSchema(String tablePrefix) {
    String trailTable = getTrailTable(tablePrefix);
    String skipTable = getSkipTable(tablePrefix);
    String chainTable = getChainTable(tablePrefix);
    return
        "CREATE TABLE " + trailTable + " (" +
        SOFT_TAB +    TRL_ID    + ' ' + INT_TYPE + " NOT NULL," +
        SOFT_TAB +    ROW_NUM   + ' ' + BIGINT_TYPE + " NOT NULL," +
        SOFT_TAB +    UTC       + ' ' + BIGINT_TYPE + " NOT NULL," +
        SOFT_TAB +    MRKL_IDX  + ' ' + INT_TYPE + " NOT NULL," +
        SOFT_TAB +    MRKL_CNT  + ' ' + INT_TYPE + " NOT NULL," +
        SOFT_TAB +    CHAIN_LEN + ' ' + INT_TYPE + " NOT NULL," +
        SOFT_TAB +    CHN_ID    + ' ' + INT_TYPE + " NOT NULL," +
        SOFT_TAB +    "PRIMARY KEY (" + TRL_ID + ")," +
        SOFT_TAB +    "FOREIGN KEY (" + ROW_NUM + ") REFERENCES " + skipTable + "(" + ROW_NUM + ")," +
        SOFT_TAB +    "FOREIGN KEY (" + CHN_ID + ") REFERENCES " + chainTable + "(" + CHN_ID + ") )";
  }
  
  
  public static String protoChainTableSchema(String tablePrefix) {
    String chainTable = getChainTable(tablePrefix);
    return
        "CREATE TABLE " + chainTable + " (" +
        SOFT_TAB +    CHN_ID + ' ' + INT_TYPE + " NOT NULL," +
        SOFT_TAB +    N_HASH + ' ' + BASE64_TYPE + " NOT NULL," +
        SOFT_TAB +    "PRIMARY KEY (" + CHN_ID + ")  )";

  }
  
  
  /**
   * Default extension for skip ledger table.
   */
  public final static String SKIP_TBL_EXT = "_sldg";

  /**
   * Default extension for trail[s] table.
   */
  public final static String TRAIL_TBL_EXT = SKIP_TBL_EXT + "_tr";

  /**
   * Default extension for [trail] chain[s] table.
   */
  public final static String CHAIN_TBL_EXT = SKIP_TBL_EXT + "_ch";
  
  
  
  public static String getSkipTable(String tablePrefix) {
    checkPrefix(tablePrefix);
    return tablePrefix + SKIP_TBL_EXT;
  }
  
  public static String getChainTable(String tablePrefix) {
    checkPrefix(tablePrefix);
    return tablePrefix + CHAIN_TBL_EXT;
  }
  
  public static String getTrailTable(String tablePrefix) {
    checkPrefix(tablePrefix);
    return tablePrefix + TRAIL_TBL_EXT;
  }
  
  private static void checkPrefix(String tablePrefix) {
    if (tablePrefix == null || tablePrefix.isBlank())
      throw new IllegalArgumentException("tablePrefix: " + tablePrefix);
  }
  
  
//  private final String sourceTable;
  private final Config config;
  
  private final String tablePrefix;
  private final String skipTable;
  private final String chainTable;
  private final String trailTable;
  
  
  /**
   * Creates a new instance using the default naming scheme to derive ledger table names
   * from the source table's name.
   * 
   * @param tablePrefix   the name of the source table (for which we're building a ledger)
   * 
   * @see #SKIP_TBL_EXT
   * @see #CHAIN_TBL_EXT
   * @see #TRAIL_TBL_EXT
   */
  public HashLedgerSchema(String tablePrefix) {
    this.config = null;
    this.tablePrefix = tablePrefix;
    this.skipTable = getSkipTable(tablePrefix);
    this.chainTable = getChainTable(tablePrefix);
    this.trailTable = getTrailTable(tablePrefix);
  }
  
  
  public HashLedgerSchema(Config config) {
    this.config = Objects.requireNonNull(config, "null config");
    
    this.tablePrefix = config.getHashTablePrefix();
    this.skipTable = getSkipTable(tablePrefix);
    this.chainTable = getChainTable(tablePrefix);
    this.trailTable = getTrailTable(tablePrefix);
    
  }

  
  


  /**
   * Returns the table name.
   */
  public final String getSkipTable() {
    return skipTable;
  }

  /**
   * Returns the table name.
   */
  public final String getChainTable() {
    return chainTable;
  }

  /**
   * Returns the table name.
   */
  public final String getTrailTable() {
    return trailTable;
  }
  
  
  
  
  public String getSkipTableSchema() {
    String sql = config == null ? null : config.getSkipTableSchema();
    if (sql == null)
      sql = defaultSkipTableSchema(skipTable);
    return sql;
  }
  
  public String getTrailTableSchema() {
    String sql = config == null ? null : config.getTrailTableSchema();
    if (sql == null)
      sql = protoTrailTableSchema(tablePrefix);
    return sql;
  }
  
  
  public String getChainTableSchema() {
    String sql = config == null ? null : config.getChainTableSchema();
    if (sql == null)
      sql = protoChainTableSchema(tablePrefix);
    return sql;
  }
  

}
  
  
  
  
  
  
  
  
  
  
  
