/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Objects;

import io.crums.sldg.HashLedger;
import io.crums.util.Base64_32;

/**
 * Schema for SQL TABLEs as backing storage for a {@linkplain HashLedger}.
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
  
  
  
  
  private final static String SOFT_TAB = "\n  ";
  
  /**
   * Creates and returns the create-table SQL statement for a skip ledger table
   * with the given name.
   * 
   * @param skipTable
   * 
   * @see HashLedgerSchema schema
   */
  public static String createSkipLedgerTableSql(String skipTable) {
    return
        "CREATE TABLE " + skipTable + " (\n" +
        SOFT_TAB +    ROW_NUM  + ' ' + BIGINT_TYPE + " NOT NULL," +
        SOFT_TAB +    SRC_HASH + ' ' + BASE64_TYPE + " NOT NULL," +
        SOFT_TAB +    ROW_HASH + ' ' + BASE64_TYPE + " NOT NULL," +
        SOFT_TAB +    "PRIMARY KEY (" + ROW_NUM + ")\n)";
  }

  
  
  
  
  public static String createTrailTableSql(String trailTable, String skipTable) {
    return
        "CREATE TABLE " + trailTable + " (\n" +
        SOFT_TAB +    TRL_ID    + ' ' + INT_TYPE + " NOT NULL AUTO_INCREMENT," +
        SOFT_TAB +    ROW_NUM   + ' ' + BIGINT_TYPE + " NOT NULL," +
        SOFT_TAB +    UTC       + ' ' + BIGINT_TYPE + " NOT NULL," +
        SOFT_TAB +    MRKL_IDX  + ' ' + INT_TYPE + " NOT NULL," +
        SOFT_TAB +    MRKL_CNT  + ' ' + INT_TYPE + " NOT NULL," +
        SOFT_TAB +    CHAIN_LEN + ' ' + INT_TYPE + " NOT NULL," +
        SOFT_TAB +    "PRIMARY KEY (" + TRL_ID + ")," +
        SOFT_TAB +    "FOREIGN KEY (" + ROW_NUM + ") REFERENCES " + skipTable + "(" + ROW_NUM + ")\n)";
  }
  
  
  
  /**
   * Returns a 5-parameter prepared insert statement. The parameter values are (in order)
   * <ol>
   * <li>row_num</li>
   * <li>utc</li>
   * <li>mrkl_idx</li>
   * <li>mrkl_cnt></li>
   * <li>chain_len</li>
   * </ol>
   */
  public static PreparedStatement insertTrailPrepStmt(Connection con, String trailTable) throws SQLException {
    Objects.requireNonNull(con, "null con");
    checkForm(trailTable, "trailTable");
    
    String sql =
        "INSERT INTO " + trailTable +
        " (" + ROW_NUM + ", " + UTC + ", " + MRKL_IDX + ", " + MRKL_CNT + ", " + CHAIN_LEN +
        ") VALUES ( ?, ?, ?, ?, ?)";
    
    return con.prepareStatement(sql);
  }
  
  

  
  /**
   * Returns a single-parameter prepared select statement. The look-up key (parameter)
   * is the {@code row_num}.
   */
  public static PreparedStatement selectTrailIdPrepStmt(Connection con, String trailTable) throws SQLException {
    Objects.requireNonNull(con, "null con");
    checkForm(trailTable, "trailTable");
    
    String sql =
        "SELECT " + TRL_ID + " FROM " + trailTable + " WHERE " + ROW_NUM + " = ?";
    
    return con.prepareStatement(sql);
  }
  
  
  public static String createChainTableSql(String chainTable, String trailTable) {
    checkForm(chainTable, "chainTable");
    checkForm(trailTable, "trailTable");
    return
        "CREATE TABLE " + chainTable + " (" +
        SOFT_TAB +    CHN_ID + ' ' + INT_TYPE + " NOT NULL AUTO_INCREMENT," +
        SOFT_TAB +    TRL_ID    + ' ' + INT_TYPE + " NOT NULL," +
        SOFT_TAB +    N_HASH + ' ' + BASE64_TYPE + " NOT NULL," +
        SOFT_TAB +    "PRIMARY KEY (" + CHN_ID + ")," +
        SOFT_TAB +    "FOREIGN KEY (" + TRL_ID + ") REFERENCES " + trailTable + "(" + TRL_ID + ")\n)";
  }
  
  
  


  private static void checkForm(String table, String descName) {
    Objects.requireNonNull(table, "null " + descName);
    if (table.length() < 2)
      throw new SqlLedgerException("table name for " + descName + " too short: '" + table + "'");
  }
  
  
  
  /**
   * Default extension for skip ledger table.
   */
  public final static String SKIP_TBL_EXT = "_sldg";


  /**
   * Default extension for [trail] chain[s] table.
   */
  public final static String CHAIN_TBL_EXT = SKIP_TBL_EXT + "_chain";
  /**
   * Default extension for trail[s] table.
   */
  public final static String TRAIL_TBL_EXT = SKIP_TBL_EXT + "_trail";
  
  
  
  
  
  
  
  private final String sourceTable;
  private final String skipTable;
  private final String chainTable;
  private final String trailTable;
  
  
  /**
   * Creates a new instance using the default naming scheme to derive ledger table names
   * from the source table's name.
   * 
   * @param sourceTable   the name of the source table (for which we're building a ledger)
   * 
   * @see #SKIP_TBL_EXT
   * @see #BEACON_TBL_EXT
   * @see #CHAIN_TBL_EXT
   * @see #TRAIL_TBL_EXT
   */
  public HashLedgerSchema(String sourceTable) {
    this(
        sourceTable,
        sourceTable + SKIP_TBL_EXT,
        sourceTable + CHAIN_TBL_EXT,
        sourceTable + TRAIL_TBL_EXT);
  }

  /**
   * Creates a new instance with the given table names. <em>Not recommended</em> for ordinary
   * use, since it's easy to get the arguments mixed up.
   * 
   * @param sourceTable   the name of the source table (for which we're building a ledger)
   * @param skipTable   the name of the skip ledger table
   * @param trailTable   the name of the trails table (crumtrails)
   * @param chainTable   the name of the chains table (merkle proof chains)
   */
  protected HashLedgerSchema(String sourceTable, String skipTable, String trailTable, String chainTable) {
    this.sourceTable = Objects.requireNonNull(sourceTable, "null sourceTable").trim();
    this.skipTable = Objects.requireNonNull(skipTable, "null skipTable").trim();
    this.chainTable = Objects.requireNonNull(chainTable, "null chainTable").trim();
    this.trailTable = Objects.requireNonNull(trailTable, "null trailTable").trim();
    
    checkForm(this.sourceTable, "sourceTable");
    checkForm(this.skipTable, "skipTable");
    checkForm(this.chainTable, "chainsTable");
    checkForm(this.trailTable, "trailsTable");
    
    checkNoDupNames();
  }


  
  
  
  private void checkNoDupNames() {
    HashSet<String> names = new HashSet<>();
    names.add(sourceTable);
    if (!names.add(skipTable))
      throw dupTableX(skipTable, "skipTable");
    if (!names.add(chainTable))
      throw dupTableX(chainTable, "chainsTable");
    if (!names.add(trailTable))
      throw dupTableX(trailTable, "trailsTable");
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
  
  
  
  
  public String createSkipLedgerTableSql() {
    return createSkipLedgerTableSql(skipTable);
  }
  
  
  public String createChainTableSql() {
    return createChainTableSql(chainTable, trailTable);
  }
  
  
  public String createTrailTableSql() {
    return createTrailTableSql(trailTable, skipTable);
  }
  
  

  /**
   * Returns a 5-parameter prepared insert statement. The parameter values are (in order)
   * <ol>
   * <li>row_num</li>
   * <li>utc</li>
   * <li>mrkl_idx</li>
   * <li>mrkl_cnt></li>
   * <li>chain_len</li>
   * </ol>
   */
  public PreparedStatement insertTrailPrepStmt(Connection con) throws SQLException {
    return insertTrailPrepStmt(con, trailTable);
  }
  
  /**
   * Returns a single-parameter prepared select statement. The look-up key (parameter)
   * is the {@code row_num}.
   */
  public PreparedStatement selectTrailIdPrepStmt(Connection con) throws SQLException {
    return selectTrailIdPrepStmt(con, trailTable);
  }

}
  
  
  
  
  
  
  
  
  
  
  
