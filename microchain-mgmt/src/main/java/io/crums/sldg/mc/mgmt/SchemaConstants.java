/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.mc.mgmt;

/**
 * 
 */
public class SchemaConstants {
  
  // no body calls
  private SchemaConstants() {  }
  

  /** Chain ID/infos table-name suffix. */
  public final static String CHAIN_INFOS = "chain_infos";
  /** Ledger-salts table-name suffix. */
  public final static String LEDGER_SALTS = "ledger_salts";
  /** Microchains definition table-name suffix. */
  public final static String MICROCHAINS = "microchains";
  
  
//  public enum StateCode {
//    OK(0),
//    DELETED(1);
//    
//    
//    private final int code;
//    
//    private StateCode(int code) {
//      this.code = code;
//    }
//    
//    public int code() {
//      return code;
//    }
//    
//  }
  
  
  public final static String CREATE_CHAIN_INFOS_TABLE =
      """
      CREATE TABLE IF NOT EXISTS %schain_infos (
        chain_id INT NOT NULL AUTO_INCREMENT,
        name VARCHAR(128) NOT NULL,
        desc VARCHAR(4096),
        uri  VARCHAR(512),
        deleted INT NOT NULL,
        PRIMARY KEY (chain_id) )""";
    
  
  public final static String SELECT_CHAIN_INFO_ALL =
      "SELECT chain_id, name, desc, uri FROM %schain_infos WHERE deleted = 0";

  public final static String SELECT_CHAIN_INFO_SANS_DESC_ALL =
      "SELECT chain_id, name, uri FROM %schain_infos WHERE deleted = 0";
  
  public final static String SELECT_CHAIN_INFO_BY_ID =
      SELECT_CHAIN_INFO_ALL + " AND chain_id = ?";
  
  public final static String SELECT_CHAIN_INFO_BY_NAME =
      SELECT_CHAIN_INFO_ALL + " AND name = ?";
  
  public final static String SELECT_CHAIN_INFO_BY_URI =
      SELECT_CHAIN_INFO_ALL + " AND uri = ?";
  
  public final static String INSERT_CHAIN_INFO =
      "INSERT INTO %schain_infos (name, desc, uri, deleted) VALUES (?, ?, ?, 0)";
  
  public final static String UPDATE_CHAIN_INFO =
      "UPDATE %schain_infos SET name = ?, desc = ?, uri = ? WHERE chain_id = ?";
  
  public final static String DELETE_CHAIN_INFO_BY_ID =
      "UPDATE %schain_infos SET deleted = 1 WHERE chain_id = ? AND deleted = 0";
  

  
  // SALT TABLE
  
  public final static String CREATE_LEDGER_SALTS_TABLE =
      """
      CREATE TABLE IF NOT EXISTS %sledger_salts (
        salt_id INT NOT NULL AUTO_INCREMENT,
        chain_id INT NOT NULL,
        seed CHAR(43) NOT NULL,
        seed_start BIGINT NOT NULL,
        deleted INT NOT NULL,
        PRIMARY KEY (salt_id),
        FOREIGN KEY (chain_id) REFERENCES %schain_infos (chain_id) )""";
  
  
  public final static String SELECT_LEDGER_SALTS_BY_CHAIN_ID =
      """
      SELECT salt_id, seed, seed_start FROM %sledger_salts \
      WHERE chain_id = ? AND deleted = 0""";
  
  
  public final static String SELECT_LEDGER_SALTS_SANS_SEED =
      """
      SELECT salt_id, seed_start FROM %sledger_salts \
      WHERE chain_id = ? AND deleted = 0""";
  
  
  public final static String INSERT_LEDGER_SALT =
      """
      INSERT INTO %sledger_salts (chain_id, seed, seed_start, deleted) \
      VALUES (?, ?, ?, 0)""";
  
  public final static String DELETE_LEDGER_SALT_BY_SALT_ID =
      "UPDATE %sledger_salts SET deleted = 1 WHERE salt_id = ? AND deleted = 0";

  
  
  
  
  
  // LEDGER DEF TABLE
  
  public final static String CREATE_MICROCHAINS_TABLE =
      """
      CREATE TABLE IF NOT EXISTS %smicrochains (
        mc_id INT NOT NULL AUTO_INCREMENT,
        chain_id INT NOT NULL,
        by_no_query VARCHAR(4096) NOT NULL,
        row_count_query VARCHAR(1024) NOT_NULL,
        salt_code INT NOT NULL,
        salt_indices VARCHAR(4096),
        commit_table VARCHAR(256),
        deleted INT NOT NULL,
        PRIMARY KEY (def_id),
        FOREIGN KEY (chain_id) REFERENCES %schain_infos (chain_id) )""";
  
  
  public final static String SELECT_MICROCHAIN_BY_CHAIN_ID =
      """
      SELECT \
      mc_id, by_no_query, row_count_query, salt_code, salt_indices, commit_table \
      FROM %smicrochains WHERE chain_id = ? AND deleted = 0""";
  
  public final static String INSERT_MICROCHAIN =
      """
      INSERT INTO %smicrochains ( \
      chain_id, by_no_query, row_count_query, salt_code, salt_indices, commit_table, deleted) \
      VALUES (?, ?, ?, ?, ?, ?, 0)""";
  
  public final static String DELETE_MICROCHAIN_BY_MC_ID =
      "UPDATE %smicrochains SET deleted = 1 WHERE mc_id = ? AND deleted = 0";
  
  
  
  
  
  // MICOCHAINS TABLE
  
//  public final static String CREATE_MICROCHAINS_TABLE =
//      """
//      CREATE TABLE IF NOT EXISTS %smicrochains (
//        mc_id INT NOT NULL AUTO_INCREMENT,
//        chain_id INT NOT NULL,
//        source_url VARCHAR(512),
//        commit_url VARCHAR(512),
//        commit_table VARCHAR(256),
//        deleted INT NOT NULL,
//        PRIMARY KEY (mc_id),
//        FOREIGN KEY (chain_id) REFERENCES %schain_infos (chain_id) )""";
//  
//
//  public final static String SELECT_MICROCHAIN_BY_CHAIN_ID =
//      """
//      SELECT mc_id, salt_code, salt_indices, source_url, commit_url, commit_table \
//      FROM %smicrochains \
//      WHERE chain_id = ? AND deleted = 0""";
//  
//  public final static String INSERT_MICROCHAIN =
//      """
//      INSERT INTO %smicrochains \
//      (chain_id, by_no_query, row_count_query, def_start, deleted) \
//      VALUES (?, ?, ?, ?, 0)""";
//  
//  
//  public final static String DELETE_MICROCHAIN_BY_MC_ID =
//      "UPDATE %smicrochains SET deleted = 1 WHERE mc_id = ? AND deleted = 0";
  
  
  
  
  // COMMIT TABLE
  
  
  
  
  public final static String CREATE_COMMIT_TABLE =
      """
      CREATE TABLE IF NOT EXISTS %s (
        row_idx BIGINT NOT NULL,
        src_hash CHAR(43) NOT NULL,
        row_hash CHAR(43) NOT NULL,
        PRIMARY KEY (row_num) )""";
  
  
  public final static String SELECT_COMMIT_BY_ROW_INDEX =
      "SELECT src_hash, row_hash FROM %s WHERE row_idx = ?";
  
  
  public final static String SELECT_MAX_COMMIT_IDX =
      "SELECT MAX(row_idx) FROM %s";
  

  
  public final static String SELECT_COMMIT_COUNT =
      "SELECT COUNT(row_idx) FROM %s";
  
  
  public final static String INSERT_COMMIT =
      "INSERT INTO %s (row_idx, src_hash, row_hash) VALUES (?, ?, ?)";
  
  
  public final static String TRIM_COMMIT =
      "DELETE FROM %s WHERE row_idx >= ";
  
  
  
}

























