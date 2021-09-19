/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.sql;


import static io.crums.sldg.sql.HashLedgerSchema.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

import io.crums.model.Crum;
import io.crums.model.CrumTrail;
import io.crums.sldg.HashLedger;
import io.crums.sldg.SkipLedger;
import io.crums.sldg.time.TrailedRow;
import io.crums.sldg.time.WitnessRecord;
import io.crums.util.Base64_32;
import io.crums.util.mrkl.Proof;

/**
 * A {@linkplain HashLedger} that lives in an SQL database.
 */
public class SqlHashLedger implements HashLedger {
  
  
  /**
   * Declares a new instance (doesn't yet exist in the database) using the given
   * <em>proto-name</em>. This becomes the prefix of the name of various tables
   * created to back the hash-ledger.
   * 
   * @param con db connection
   * @param protoName proto-name. Designed to be the same as the source-table
   */
  public static SqlHashLedger declareNewInstance(Connection con, String protoName) {
    return declareNewInstance(con, new HashLedgerSchema(protoName));
  }
  
  public static SqlHashLedger declareNewInstance(Connection con, HashLedgerSchema schema) {
    Objects.requireNonNull(schema, "null schema");
    try {
      if (con.getAutoCommit())
        con.setAutoCommit(false);
      
      Statement stmt = con.createStatement();
      
      stmt.execute(schema.getSkipTableSchema());
      stmt.execute(schema.getTrailTableSchema());
      stmt.execute(schema.getChainTableSchema());
      
      con.commit();
      
      return new SqlHashLedger(schema, con);
      
    } catch (SQLException sqx) {
      throw new SqlLedgerException("on declareNewInstance(" + schema + "): " + sqx, sqx);
    }
  }
  
  
  
  
  private final Object lock = new Object();
  
  private final HashLedgerSchema schema;
  private final Connection con;
  
  private final SqlSkipLedger skipLedger;
  
  private final PreparedStatement selectTrailIdStmt;
  
  private final PreparedStatement trailCountStmt;
  
  private final PreparedStatement selectTrailByIndexStmt;
  
  private final PreparedStatement selectChainByTrailId;
  
  private final PreparedStatement selectNearestTrailStmt;
  
  private final PreparedStatement selectLastTrailedRnStmt;
  
  private PreparedStatement insertTrailStmt;
  
  
  /**
   * On demand initialization. Implemented this way, so that you can pass in a
   * read-only database connection at construction. There's no analagous prepared
   * statement for the chain-table insert, since the number of rows inserted is not
   * fixed.
   */
  private PreparedStatement getInsertTrailStmt() throws SQLException {
    synchronized (lock) {
      if (insertTrailStmt == null)
        insertTrailStmt = schema.insertTrailPrepStmt(con);
      return insertTrailStmt;
    }
  }
  
  
  

  /**
   * Creates a new instance from an already existing schema on the backing database
   * 
   * @param protoName the proto (prefix) name from which other table names are inferred
   * @param con       the database the tables live in
   */
  public SqlHashLedger(String protoName, Connection con) {
    this(new HashLedgerSchema(protoName), con);
  }
  
  
  

  /**
   * Creates a new instance from an already existing schema on the backing database
   * 
   * @param schema    describes the existing backing hash-ledger tables
   * @param con       the database the tables live in
   */
  public SqlHashLedger(HashLedgerSchema schema, Connection con) {
    this.schema = Objects.requireNonNull(schema, "null schema");
    this.con = Objects.requireNonNull(con, "null con");
    try {
      if (!con.isValid(5))
        throw new IllegalArgumentException("connection not valid: " + con);
      
      if (!con.isReadOnly() && con.getAutoCommit())
        con.setAutoCommit(false);
      
      this.skipLedger = new SqlSkipLedger(con, schema.getSkipTable());
      this.selectTrailIdStmt = schema.selectTrailIdPrepStmt(con);
      this.trailCountStmt = con.prepareStatement(
          "SELECT count(*) FROM " + schema.getTrailTable() + " AS count");
      
      this.selectTrailByIndexStmt = con.prepareStatement(
          "SELECT * FROM (\n" +
          "  SELECT ROW_NUMBER() OVER (ORDER BY " + TRL_ID + " ASC) AS row_index, " +
          TRL_ID + ", " + ROW_NUM + ", " + UTC + ", " + MRKL_IDX + ", " + MRKL_CNT + ", " + CHAIN_LEN +
          " FROM " + schema.getTrailTable() + ") AS snap WHERE row_index = ?");
      
      this.selectChainByTrailId = con.prepareStatement(
          "SELECT " + CHN_ID + ", " + N_HASH + " FROM " + schema.getChainTable() +
          " WHERE " + TRL_ID + " = ? ORDER BY " + CHN_ID);
      
      this.selectNearestTrailStmt = con.prepareStatement(
          "SELECT " + TRL_ID + ", " + ROW_NUM + ", " + UTC + ", " + MRKL_IDX + ", " + MRKL_CNT + ", " + CHAIN_LEN +
          " FROM " + schema.getTrailTable() +
          " WHERE " + ROW_NUM + " >= ? ORDER BY " + ROW_NUM + " LIMIT 1");
      
      this.selectLastTrailedRnStmt = con.prepareStatement(
          "SELECT " + TRL_ID + ", " + ROW_NUM + " FROM " + schema.getTrailTable() +
          " ORDER BY " + TRL_ID + " DESC LIMIT 1");
      
    } catch (SQLException sqx) {
      throw new SqlLedgerException("on <init>: " + sqx, sqx);
    }
  }

  @Override
  public void close() {
    synchronized (lock) {
      try {
        con.close();
      } catch (SQLException x) {
        throw new SqlLedgerException("on close(): " + x, x);
      }
    }
  }

  @Override
  public SkipLedger getSkipLedger() {
    return skipLedger;
  }

  @Override
  public boolean addTrail(WitnessRecord trailedRecord) {
    if (!Objects.requireNonNull(trailedRecord, "null trailedRecord").isTrailed())
      throw new IllegalArgumentException("not trailed: " + trailedRecord);

    final long rowNum = trailedRecord.rowNum();
    CrumTrail trail = trailedRecord.record().trail();
    
    synchronized (lock) {
      try {
        PreparedStatement trailInsert = getInsertTrailStmt();
        trailInsert.setLong(1, rowNum);
        trailInsert.setLong(2, trailedRecord.utc());
        trailInsert.setInt(3, trail.leafIndex());
        trailInsert.setInt(4, trail.leafCount());
        trailInsert.setInt(5, trail.hashChain().size());
        
        trailInsert.executeUpdate();
        
        PreparedStatement selectTrailId = selectTrailIdStmt;
        selectTrailId.setLong(1, rowNum);
        ResultSet rs = selectTrailId.executeQuery();
        if (!rs.next()) {
          con.rollback();
          throw new SqlLedgerException("expected INSERT was ineffective: " + trailedRecord);
        }
        
        final long trailId = rs.getLong(1);
        
        if (rs.next()) {
          con.rollback();
          return false;
        }
        
        StringBuilder sql =
            new StringBuilder("INSERT INTO " ).append(schema.getChainTable())
              .append(" (").append(TRL_ID).append(", ").append(N_HASH).append(") VALUES");
        
        for (byte[] cHash : trail.hashChain())
          sql.append("\n(").append(trailId).append(", '").append(Base64_32.encode(cHash)).append("'),");
        
        // remove the last comma
        sql.setLength(sql.length() - 1);
        
        Statement stmt = con.createStatement();
        stmt.execute(sql.toString());
        
        int updateCount = stmt.getUpdateCount();
        
        if (updateCount != trail.hashChain().size()) {
          con.rollback();
          throw new SqlLedgerException(
              "INSERT did not yield expected updateCount " + trail.hashChain().size() +
              "; actual was " + updateCount + "; SQL:\n" + sql);
        }
        
        con.commit();
        return true;
        
        
      } catch (SQLException sqx) {
        boolean rb;
        try {
          con.rollback();
          rb = true;
        } catch (SQLException sqx2) {
          rb = false;
        }
        String msg = "on addTrail " + trailedRecord;
        if (!rb)
          msg += " (rollback failed!)";
        msg += " -- " + sqx;
        throw new SqlLedgerException(msg, sqx);
      }
    }
  }

  @Override
  public int getTrailCount() {
    synchronized (lock) {
      try {
        ResultSet rs = trailCountStmt.executeQuery();
        if (rs.next())
          return (int) rs.getLong(1);
        
        throw new SqlLedgerException("empty result set on getTrailCount(): " + rs);
        
      } catch (SQLException sqx) {
        throw new SqlLedgerException("on getTrailCount(): " + sqx, sqx);
      }
    }
  }

  @Override
  public TrailedRow getTrailByIndex(int index) {
    if (index < 0)
      throw new IllegalArgumentException("index " + index);
    
    synchronized (lock) {
      try {
        
        selectTrailByIndexStmt.setInt(1, index + 1);
        ResultSet rs = selectTrailByIndexStmt.executeQuery();
        
        if (!rs.next()) {
          if (index >= getTrailCount())
            throw new IndexOutOfBoundsException(index);
          else
            throw new SqlLedgerException("empty result set on getTrailByIndex(" + index + "): " + rs);
        }
        
        return getTrailedRow(rs, 1);
        
      } catch (SQLException sqx) {
        throw new SqlLedgerException("on getTrailByIndex(" + index + "): " + sqx, sqx);
      }
    }
  }
  
  
  
  private TrailedRow getTrailedRow(ResultSet rs, int colOffset) throws SQLException {
    
    final int tid = rs.getInt(colOffset + 1);
    final long rowNumber = rs.getLong(colOffset + 2);
    final long utc = rs.getLong(colOffset + 3);
    final int leafIndex = rs.getInt(colOffset + 4);
    final int leafCount = rs.getInt(colOffset + 5);
    final int chainLen = rs.getInt(colOffset + 6);
    
    if (chainLen != Proof.chainLength(leafCount, leafIndex))
      throw new SqlLedgerException(
          "chain-length assertion failed: chainLen=" + chainLen +
          ", leafCount=" + leafCount + ", leafIndex=" + leafIndex);
    
    if (rs.next())
      throw new SqlLedgerException(
          "multiple rows in result set: tid=" + tid + ", rowNumber=" + rowNumber);
    
    selectChainByTrailId.setInt(1, tid);
    rs = selectChainByTrailId.executeQuery();
    
    byte[][] chain = new byte[chainLen][];
    
    int lastLinkId = -1;
    for (int ci = 0; ci < chainLen; ++ci) {
      
      if (!rs.next())
        throw new SqlLedgerException(
            "expected hash-chain of length " + chainLen + "; actual in db is " + ci);
      
      int linkId = rs.getInt(1);
      
      if (linkId <= lastLinkId)
        throw new SqlLedgerException(
            "assertion failed: lastLinkId=" + lastLinkId + "; linkId=" + linkId +
            "; tid=" + tid);
      
      String encoded = rs.getString(2);
      chain[ci] = Base64_32.decode(encoded);
    }
    
    Crum crum = new Crum(skipLedger.rowHash(rowNumber), utc);
    
    CrumTrail trail = new CrumTrail(leafCount, leafIndex, chain, crum);
    
    return new TrailedRow(rowNumber, trail);
  }
  
  

  @Override
  public TrailedRow nearestTrail(long rowNumber) {
    
    SkipLedger.checkRealRowNumber(rowNumber);
    
    synchronized (lock) {
      try {
        selectNearestTrailStmt.setLong(1, rowNumber);
        ResultSet rs = selectNearestTrailStmt.executeQuery();
        
        return rs.next() ? getTrailedRow(rs, 0) : null;
        
      } catch (SQLException sqx) {
        throw new SqlLedgerException("on nearestTrail(" + rowNumber + "): " + sqx, sqx);
      }
    }
  }

  @Override
  public long lastWitnessedRowNumber() {
    synchronized (lock) {
      try {
        ResultSet rs = selectLastTrailedRnStmt.executeQuery();
        if (!rs.next())
          return 0;
        
        long witnessedRn = rs.getLong(2);
        
        if (witnessedRn < 1)
          throw new SqlLedgerException("nonsense witnessed row number " + witnessedRn);
        
        return witnessedRn;
        
      } catch (SQLException sqx) {
        throw new SqlLedgerException("on lastWitnessedRowNumber(): " + sqx, sqx);
      }
    }
  }

}








