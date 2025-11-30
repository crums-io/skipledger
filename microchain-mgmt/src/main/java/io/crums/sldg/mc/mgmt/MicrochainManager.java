/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.mc.mgmt;


import static io.crums.sldg.mc.mgmt.SchemaConstants.CHAIN_INFOS;
import static io.crums.sldg.mc.mgmt.SchemaConstants.LEDGER_SALTS;
import static io.crums.sldg.mc.mgmt.SchemaConstants.MICROCHAINS;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import io.crums.sldg.mc.mgmt.ChainDefMgr.ChainDef;
import io.crums.sldg.mc.mgmt.ChainIdMgr.ChainId;
import io.crums.util.TaskStack;

/**
 * 
 */
public class MicrochainManager implements AutoCloseable {
  
  
  
  /**
   * Creates the backing microchain configuration tables (3), if they don't
   * exist, and returns an instance of this class in read/write mode. The
   * system table names begin with given user-defined {@code tablePrefix}.
   * 
   * @param tablePrefix      microchain system tables prefix
   */
  public static MicrochainManager ensureInstance(
      String tablePrefix, HikariDataSource dataSource)
          throws ChainManagementException {
    
    return
        new MicrochainManager(
            DbEnv.DEFAULT.tablePrefix(tablePrefix).readWrite(true),
            dataSource,
            true);
  }
  
  
  /**
   * Creates the backing microchain configuration tables (3), if they don't
   * exist, and returns an instance of this class in read/write mode. The
   * system table names begin with the standard {@code sldg_} prefix.
   * 
   */
  public static MicrochainManager ensureInstance(HikariDataSource dataSource)
      throws ChainManagementException {
    
    return
        new MicrochainManager(
            DbEnv.DEFAULT.readWrite(true),
            dataSource,
            true);
  }
  
  
  

  /**
   * Loads and returns an existing instance from the database, if found.
   * This loads by using database schema look-ups.
   * 
   * @param env              environment opened in
   * @param hikariProperties database connection settings
   * 
   * @see #load(DbEnv, Properties, boolean)
   */
  public static Optional<MicrochainManager> load(
      DbEnv env, Properties hikariProperties)
          throws ChainManagementException {
    
    return load(env, hikariProperties, false);
  }
  
  
  /**
   * Loads and returns an existing instance from the database, if found.
   * 
   * @param env              environment opened in
   * @param hikariProperties database connection settings
   * @param force            if {@code true}, then best effort is made to load;
   *                         any exceptions on loading are suppressed and
   *                         result in an empty return value
   */
  public static Optional<MicrochainManager> load(
      DbEnv env, Properties hikariProperties, boolean force)
          throws ChainManagementException {
    
    HikariDataSource dataSource = new HikariDataSource(new HikariConfig(hikariProperties));
    return force ? loadForced(env, dataSource) : loadSoft(env, dataSource);
  }
  
  
  
  /**
   * Loads and returns an existing instance from the database, if found.
   * 
   * @param env              environment opened in
   * @param dataSource       hikari data source
   * @param force            if {@code true}, then best effort is made to load;
   *                         any exceptions on loading are suppressed and
   *                         result in an empty return value
   */
  public static Optional<MicrochainManager> load(
      DbEnv env, HikariDataSource dataSource, boolean force)
          throws ChainManagementException {
    
    return force ? loadForced(env, dataSource) : loadSoft(env, dataSource);
  }
  
  
  
//  private static Optional<MicrochainManager> loadSoft(
//      DbEnv env, HikariConfig config) {
//  
//    return loadSoft(env, new HikariDataSource(config));
//    HikariDataSource dataSource;
//    try (var onFail = new TaskStack()) {
//      
//      dataSource = onFail.push(new HikariDataSource(config));
//      Connection con = dataSource.getConnection();
//      var prefix = env.tablePrefix();
//      String[] systemTables = {
//          prefix + CHAIN_INFOS,
//          prefix + LEDGER_SALTS,
//          prefix + MICROCHAINS
//      };
//      for (var table : systemTables)
//        if (!tableExists(con, table))
//          return Optional.empty();
//      
//      con.close();
//      onFail.clear();
//      
//      
//    } catch (SQLException sx) {
//      throw new ChainManagementException(sx);
//    }
//    
//    return Optional.of(new MicrochainManager(env, dataSource, false));
//  }
  
  
  private static Optional<MicrochainManager> loadSoft(
      DbEnv env, HikariDataSource dataSource) {
  
    try (Connection con = dataSource.getConnection()) {
      
      var prefix = env.tablePrefix();
      String[] systemTables = {
          prefix + CHAIN_INFOS,
          prefix + LEDGER_SALTS,
          prefix + MICROCHAINS
      };
      for (var table : systemTables)
        if (!tableExists(con, table))
          return Optional.empty();
      
      
    } catch (SQLException sx) {
      throw new ChainManagementException(sx);
    }
    
    return Optional.of(new MicrochainManager(env, dataSource, false));
  }
  
  
  
  

  private static boolean tableExists(Connection con, String tableName)
      throws ChainManagementException {
    
    if (tableName == null || tableName.isBlank())
      return false;
    
    try (ResultSet rs = con.getMetaData().getTables(
          null, null,
          tableName.toUpperCase(), new String[] {"TABLE"})) {
      
      return rs.next();
    
    } catch (SQLException sx) {
      throw new ChainManagementException(sx);
    }
  }
  
//  private static Optional<MicrochainManager> loadForced(
//      DbEnv env, HikariConfig config) {
//    return loadForced(env, new HikariDataSource(config));
//    try {
//      return Optional.of(new MicrochainManager(env, config, false));
//    } catch (Exception x) {
//      return Optional.empty();
//    }
//  }
  
  
  private static Optional<MicrochainManager> loadForced(
      DbEnv env, HikariDataSource dataSource) {
    try {
      return Optional.of(new MicrochainManager(env, dataSource, false));
    } catch (Exception x) {
      return Optional.empty();
    }
  }
  
  
  
  public final static class ChainConfigState {
    
    public final static int ID_FLAG = 1;
    public final static int LEDGER_DEF_FLAG = 2;
    public final static int SALT_FLAG = 4;
    public final static int COMMIT_TABLE_FLAG = 8;
    
    
    public final static ChainConfigState NONE = new ChainConfigState(0);
    public final static ChainConfigState ID =
        new ChainConfigState(ID_FLAG);
    public final static ChainConfigState LEDGER_DEFINED =
        new ChainConfigState(ID_FLAG + LEDGER_DEF_FLAG);
    public final static ChainConfigState SALT_COMPLETE =
        new ChainConfigState(ID_FLAG + SALT_FLAG);
    public final static ChainConfigState COMMIT_TABLE_CREATED =
        new ChainConfigState(ID_FLAG + LEDGER_DEF_FLAG + COMMIT_TABLE_FLAG);
    public final static ChainConfigState COMPLETE =
        new ChainConfigState(-1 + COMMIT_TABLE_FLAG * 2);
    
    
    private final int state;
    
    
    
    private ChainConfigState(int state) {
      this.state = state;
    }
    
    @Override
    public boolean equals(Object o) {
      return
          o == this ||
          o instanceof ChainConfigState other &&
          other.state == state;
    }

    @Override
    public int hashCode() {
      return state;
    }
    
    
    private boolean flag(int code) {
      return (state & code) != 0;
    }
    
    
    
    
    
    public boolean id() {
      return flag(ID_FLAG);
    }
    
    public ChainConfigState id(boolean exists) {
      if (!exists)
        return NONE;
      return id() ? this : ID;
    }
    
    
    
    public boolean ledgerDefined() {
      return flag(LEDGER_DEF_FLAG);
    }
    
    public ChainConfigState ledgerDefined(boolean yes) {
      if (ledgerDefined() == yes)
        return this;
      return yes ? LEDGER_DEFINED : this;
    }
    
    
    
    public boolean saltComplete() {
      return flag(SALT_FLAG);
    }
    
    public ChainConfigState saltComplete(boolean yes) {
      if (saltComplete() == yes)
        return this;
      
      int newState = yes ?
          (SALT_COMPLETE.state | state) :
            state & ~SALT_FLAG;
      return new ChainConfigState(newState);
    }
    
    
    
    public boolean commitTableExists() {
      return flag(COMMIT_TABLE_FLAG);
    }
    
    public ChainConfigState commitTableExists(boolean yes) {
      if (commitTableExists() == yes)
        return this;
      
      int newState = yes ?
          (COMMIT_TABLE_CREATED.state | state) :
            state & ~COMMIT_TABLE_FLAG;
      
      return new ChainConfigState(newState);
            
    }
    
    
    public boolean isComplete() {
      return state == COMPLETE.state;
    }
    
    
  }
  
  
  
  
  
  //       - - -   I N S T A N C E   M E M B E R S   - - -
  
  
  private final DbEnv env;
  private final HikariDataSource dataSource;
  
  private final ChainIdMgr idManager;
  private final ChainDefMgr defManager;
  private final SaltMgr saltManager;
  
  
  
  
  
  

  /**
   * 
   */
//  private MicrochainManager(DbEnv env, HikariConfig config, boolean create)
//      throws ChainManagementException {
//      
//    this(env, new HikariDataSource(config), create);
//  }
  
  
  private MicrochainManager(DbEnv env, HikariDataSource dataSource, boolean create)
      throws ChainManagementException {
      
    this.env = env;
    
    try (var onFail = new TaskStack()) {
      
      this.dataSource = onFail.push(dataSource);
      if (create) {
        this.idManager = onFail.push(ChainIdMgr.ensureInstance(env, con()));
        this.defManager = onFail.push(ChainDefMgr.ensureInstance(env, con()));
        this.saltManager = onFail.push(SaltMgr.ensureInstance(env, con()));
      
      } else {
        this.idManager = onFail.push(new ChainIdMgr(env, con()));
        this.defManager = onFail.push(new ChainDefMgr(env, con()));
        this.saltManager = onFail.push(new SaltMgr(env, con()));
      }
      
      onFail.clear();
      
    } catch (SQLException sx) {
      throw new ChainManagementException(sx);
    }
  }
  
  private Connection con() throws SQLException {
    return dataSource.getConnection();
  }

  /**
   * Closes the instance and 
   */
  @Override
  public void close() {
    dataSource.close();
  }
  
  
  
  public List<ChainInfo> list() throws ChainManagementException {
    return idManager.list();
  }
  
  
  
  public ChainConfigState getConfigState(ChainId chainId)
      throws ChainManagementException {
    
    var state =
        defManager.findChainDef(chainId)
        .map(def ->
            ChainConfigState.LEDGER_DEFINED
            .saltComplete(!def.saltScheme().hasSalt())
            .commitTableExists(tableExists(defManager.con, def.commitTable())))
        .orElse(ChainConfigState.ID);
    
    return
        state.saltComplete() ? state :
          state.saltComplete(saltManager.saltConfigured(chainId));
    
  }
  

}

















