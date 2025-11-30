/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.mc.mgmt;


import static io.crums.sldg.mc.mgmt.McMgmtConstants.getLogger;
import static io.crums.sldg.mc.mgmt.SchemaConstants.CREATE_MICROCHAINS_TABLE;
import static io.crums.sldg.mc.mgmt.SchemaConstants.DELETE_MICROCHAIN_BY_MC_ID;
import static io.crums.sldg.mc.mgmt.SchemaConstants.INSERT_MICROCHAIN;
import static io.crums.sldg.mc.mgmt.SchemaConstants.SELECT_MICROCHAIN_BY_CHAIN_ID;

import java.lang.System.Logger.Level;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.StringTokenizer;

import io.crums.sldg.mc.mgmt.ChainIdMgr.ChainDepKey;
import io.crums.sldg.mc.mgmt.ChainIdMgr.ChainId;
import io.crums.sldg.src.SaltScheme;
import io.crums.util.TaskStack;

/**
 * Microchain definitions manager.
 */
public class ChainDefMgr extends DeletablesMgr {
  
  
  /**
   * Primary key.
   */
  public final static class DefId extends ChainDepKey {

    private DefId(int id, ChainId chainId) {
      super(id, chainId);
    }
    
    @Override
    public boolean equals(Object o) {
      return
          o == this ||
          o instanceof DefId other &&
          other.no() == this.no();
    }
    
  }
  
  
  
  
  
  
  
  public final static class ChainDef {
    
    private final DefId id;
    private final String rowByNoQuery;
    private final String rowCountQuery;
    private final SaltScheme saltScheme;
    private final String commitTable;
    
    
    private ChainDef(
        DefId id, String rowByNoQuery, String rowCountQuery, SaltScheme saltScheme,
        String commitTable) {
      this.id = id;
      this.rowByNoQuery = checkNull(rowByNoQuery, "rowByNoQuery");
      this.rowCountQuery = checkNull(rowCountQuery, "rowCountQuery");
      this.saltScheme = checkNull(saltScheme, "saltScheme");
      this.commitTable = checkNull(commitTable, "commitTable");
    }
    
    private <T> T checkNull(T member, String name) {
      if (member == null)
        throw new ChainManagementException(
            "internal error: null '%s' on constructing EpochDef %s"
            .formatted(name, id));
      return member;
    }
    
    
    public DefId id() {
      return id;
    }
    
    
    public String rowByNoQuery() {
      return rowByNoQuery;
    }
    
    public String rowCountQuery() {
      return rowCountQuery;
    }
    
    
    public SaltScheme saltScheme() {
      return saltScheme;
    }
    
    
    public String commitTable() {
      return commitTable;
    }
    

    /** Equality is solely based on {@linkplain #id()}. */
    @Override
    public boolean equals(Object o) {
      return
          o == this ||
          o instanceof ChainDef other &&
          other.id.no() == id.no();
    }

    @Override
    public int hashCode() {
      return id.no();
    }
    
  }
  
  
  public static ChainDefMgr ensureInstance(DbEnv env, Connection con) {
    return new ChainDefMgr(env, con, true);
  }
  
  
  // - - -  M E M B E R S - - -
  
  
  
  private final PreparedStatement defsByChainId;
  private final PreparedStatement insertStmt;
  
  

  /**
   * @param env
   * @param con
   */
  public ChainDefMgr(DbEnv env, Connection con) {
    this(env, con, false);
  }
  
  private ChainDefMgr(DbEnv env, Connection con, boolean create) {
    super(env, con);
    
    try (var closeOnFail = new TaskStack()) {
      
      if (create && env.readWrite()) {
        var sql = env.applyTablePrefix(CREATE_MICROCHAINS_TABLE);
        getLogger().log(Level.INFO, "Executing SQL DDL:%n%s".formatted(sql));
        executeDdl(sql);
      }
      
      this.defsByChainId =
          prepareStmt(SELECT_MICROCHAIN_BY_CHAIN_ID, closeOnFail);
      
      this.insertStmt = env.readOnly() ? null :
        prepareStmt(INSERT_MICROCHAIN);
      
      closeOnFail.clear();
    } catch (SQLException sx) {
      throw new ChainManagementException(sx);
    }
  }

  @Override
  protected final String deleteByIdSql() {
    return DELETE_MICROCHAIN_BY_MC_ID;
  }

  @Override
  public void close() {
    try (var closer = new TaskStack()) {
      closer.pushClose(defsByChainId);
    }
  }
  
  
  /**
   * Finds and returns the chain-definition ({@linkplain ChainDef}) for the
   * specified chain, if found.
   * 
   * @param chainId     chain identifier
   *
   * @throws ChainManagementException
   *         if an internal error occurs
   */
  public synchronized Optional<ChainDef> findChainDef(ChainId chainId)
      throws ChainManagementException {
    
    try (var closer = new TaskStack()) {
      
      defsByChainId.setInt(1, chainId.no());
      ResultSet rs = defsByChainId.executeQuery();
      closer.pushClose(rs);
      
      if (!rs.next())
        return Optional.empty();

      DefId defId = new DefId(rs.getInt(1), chainId);
      String rowByNoQuery = rs.getString(2);
      String rowCountQuery = rs.getString(3);
      boolean saltIndicesPositive = (rs.getInt(4) & SALT_INDICES_INCLUDE) != 0;
      int[] saltIndices = decodeSaltIndices(rs.getString(5), defId);
      SaltScheme saltScheme = SaltScheme.of(saltIndices, saltIndicesPositive);
      String commitTable = rs.getString(6);
      
      ChainDef chainDef = new ChainDef(
          defId, rowByNoQuery, rowCountQuery, saltScheme, commitTable);
      
      if (rs.next())
        throw new ChainManagementException(
            "internal error. Duplicate chain definitions for chain ID " + chainId);
      
      return Optional.of(chainDef) ;
      
    } catch (SQLException sx) {
      throw new ChainManagementException(sx);
    }
  }
  
  
  public synchronized ChainDef createChainDef(
      ChainId chainId,
      String rowByNoQuery, String rowCountQuery,
      SaltScheme saltScheme)
          throws
          IllegalArgumentException,
          ChainManagementException,
          UnsupportedOperationException {
    
    
    checkWrite();
    if (rowByNoQuery.isBlank())
      throw new IllegalArgumentException("empty rowByNoQuery");
    if (rowCountQuery.isBlank())
      throw new IllegalArgumentException("empty rowByNoQuery");
    
    saltScheme = SaltScheme.wrap(saltScheme);
    String saltIndices = encodeSaltIndices(saltScheme.cellIndices());
    String commitTable = env.defaultCommitTable(chainId);
    {
      var def = findChainDef(chainId);
      if (def.isPresent())
        throw new IllegalArgumentException(
            "ChainDef %s is already defined".formatted(def.get().id()));
    }
    
    try {
      
      insertStmt.setInt(1, chainId.no());
      insertStmt.setString(2, rowByNoQuery);
      insertStmt.setString(3, rowCountQuery);
      insertStmt.setInt(4, saltScheme.isPositive() ?
          SALT_INDICES_INCLUDE : SALT_INDICES_EXCLUDE);
      insertStmt.setString(5, saltIndices);
      insertStmt.setString(6, commitTable);
      {
        int count = insertStmt.executeUpdate();
        assert count == 1;
      }
      
      
    } catch (SQLException sx) {
      throw new ChainManagementException(
          "internal error on attempt to create chain def %s; caused by %s"
          .formatted(chainId, sx), sx);
    }
    
    return verifyExpected(chainId, rowByNoQuery, rowCountQuery, saltScheme);
  }
  
  
  
  
  private ChainDef verifyExpected(
      ChainId chainId,
      String rowByNoQuery, String rowCountQuery, SaltScheme saltScheme) {
    
    var chainDef = findChainDef(chainId).orElseThrow(
        () -> new ChainManagementException(
            "internal error. Attempt to create chain def %s fails read-back"
            .formatted(chainId)));
    
    if (!chainDef.rowByNoQuery().equals(rowByNoQuery))
      failUnexpected(chainDef, "rowByNoQuery");

    if (!chainDef.rowCountQuery().equals(rowCountQuery))
      failUnexpected(chainDef, "rowCountQuery");
    
    if (!chainDef.saltScheme().equals(saltScheme))
      failUnexpected(chainDef, "saltScheme");
    
    return chainDef;
  }
  
  private void failUnexpected(ChainDef def, String argName) {
    throw new ChainManagementException(
        "internal error. Created chain def %s fails read-back of '%s' argument"
        .formatted(def.id(), argName));
  }

  private String encodeSaltIndices(int[] indices) {
    final int count = indices.length;
    if (count == 0)
      return "";
    if (count * 2 >= MAX_ENCODED_INDICES_LEN)
      throw new IllegalArgumentException(
          "saltScheme indices (%d) do not fit in table".formatted(count));
    
    var s = new StringBuilder().append(indices[0]);
    for (int index = 1; index < count; ++index)
      s.append(',').append(indices[index]);
    
    return s.toString();
  }
  
  private final static int MAX_ENCODED_INDICES_LEN = 4096;
  
  public final static int SALT_INDICES_EXCLUDE = 0;
  public final static int SALT_INDICES_INCLUDE = 1;
  
  
  private int[] decodeSaltIndices(String indices, DefId id) {
    if (indices == null || indices.isBlank())
      return new int[0];
    
    List<Integer> saltIndices = new ArrayList<>();
    
    try {
      for (var tokenizer = new StringTokenizer(indices, ",\t\n\r\f ");
           tokenizer.hasMoreTokens(); )
        
        saltIndices.add(Integer.parseInt(tokenizer.nextToken()));
      
    } catch (Exception x) {
      throw failSaltIndices(indices, id).fillInStackTrace();
    }
    
    saltIndices = saltIndices.stream().sorted().toList();
    
    int prev = saltIndices.get(0);
    if (prev < 0)
      throw failSaltIndices(indices, id).fillInStackTrace();
    
    for (int index = 1, count = saltIndices.size(); index < count; ++index) {
      int next = saltIndices.get(index);
      if (next == prev)
        throw failSaltIndices(indices, id).fillInStackTrace();
    }
    
    int[] array = new int[saltIndices.size()];
    for (int index = array.length; index-- > 0; )
      array[index] = saltIndices.get(index);
    
    return array;
  }
  
  
  private ChainManagementException failSaltIndices(String indices, DefId id) {
    return
        new ChainManagementException(
            "internal error. Bad salt indices encoding in LedgerDef %s: %s"
            .formatted(id, indices));
  }
  
}






















