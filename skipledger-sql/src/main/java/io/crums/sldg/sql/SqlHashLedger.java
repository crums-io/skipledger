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
import java.util.Date;
import java.util.Objects;
import java.util.logging.Logger;

import io.crums.model.Crum;
import io.crums.model.CrumTrail;
import io.crums.sldg.HashLedger;
import io.crums.sldg.SkipLedger;
import io.crums.sldg.time.TrailedRow;
import io.crums.sldg.time.WitnessRecord;
import io.crums.util.Base64_32;
import io.crums.util.TaskStack;
import io.crums.util.mrkl.Proof;

/**
 * A {@linkplain HashLedger} that lives in an SQL database.
 * 
 * @see HashLedgerSchema
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
      stmt.execute(schema.getChainTableSchema());
      stmt.execute(schema.getTrailTableSchema());
      
      con.commit();
      
      return new SqlHashLedger(schema, con);
      
    } catch (SQLException sqx) {
      throw new SqlLedgerException("on declareNewInstance(" + schema + "): " + sqx, sqx);
    }
  }
  
  
  
  
  
  
  
  //   I N S T A N C E    M E M B E R S
  
  
  private final Object lock = new Object();
  
  private final HashLedgerSchema schema;
  private final Connection con;
  
  private final SqlSkipLedger skipLedger;
  
  private final PreparedStatement trailCountStmt;
  
  private final PreparedStatement selectTrailByIndexStmt;
  
  private final PreparedStatement selectChainByChainId;
  
  private final PreparedStatement selectNearestTrailStmt;
  
  private final PreparedStatement selectLastTrailedRnStmt;

  private final PreparedStatement chainTableCountStmt;
  
  private PreparedStatement insertTrailStmt;
  

  /**
   * On demand initialization returns a 7-parameter prepared insert statement.
   * The parameter values are (in order)
   * <ol>
   * <li>trl_id</li>
   * <li>row_num</li>
   * <li>utc</li>
   * <li>mrkl_idx</li>
   * <li>mrkl_cnt></li>
   * <li>chain_len</li>
   * <li>chn_id</li>
   * </ol>
   * <p>
   * Implemented this way, so that you can pass in a
   * read-only database connection at construction. There's no analagous prepared
   * statement for the chain-table insert, since the number of rows inserted is not
   * fixed.
   * </p>
   */
  private PreparedStatement getInsertTrailStmt() throws SQLException {
    synchronized (lock) {
      if (insertTrailStmt == null) {
        String sql =
            "INSERT INTO " + schema.getTrailTable() +
            " (" + TRL_ID + ", " + ROW_NUM + ", " + UTC + ", " + MRKL_IDX + ", " + MRKL_CNT + ", " + CHAIN_LEN + ", " + CHN_ID +
            ") VALUES ( ?, ?, ?, ?, ?, ?, ?)";
        
        insertTrailStmt = con.prepareStatement(sql);
      }
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
//      this.selectTrailIdStmt = schema.selectTrailIdPrepStmt(con);
      this.trailCountStmt = con.prepareStatement(
          "SELECT count(*) FROM " + schema.getTrailTable() + " AS rcount");
      
      this.selectTrailByIndexStmt = con.prepareStatement(
          "SELECT " + TRL_ID + ", " + ROW_NUM + ", " + UTC + ", " + MRKL_IDX + ", " + MRKL_CNT + ", " +
          CHAIN_LEN + ", " + CHN_ID +
          " FROM " + schema.getTrailTable() +
          " WHERE " + TRL_ID + " = ?");
      
      this.selectChainByChainId = con.prepareStatement(
          "SELECT " + CHN_ID + ", " + N_HASH + " FROM " + schema.getChainTable() +
          " WHERE " + CHN_ID + " >= ? ORDER BY " + CHN_ID + " LIMIT ?");
      
      this.selectNearestTrailStmt = con.prepareStatement(
          "SELECT " + TRL_ID + ", " + ROW_NUM + ", " + UTC + ", " + MRKL_IDX + ", " + MRKL_CNT + ", " +
          CHAIN_LEN + ", " + CHN_ID +
          " FROM " + schema.getTrailTable() +
          " WHERE " + ROW_NUM + " >= ? ORDER BY " + ROW_NUM + " LIMIT 1");
      
      this.selectLastTrailedRnStmt = con.prepareStatement(
          "SELECT " + TRL_ID + ", " + ROW_NUM + " FROM " + schema.getTrailTable() +
          " ORDER BY " + TRL_ID + " DESC LIMIT 1");
      
      this.chainTableCountStmt = con.prepareStatement(
          "SELECT count(*) FROM " + schema.getChainTable() + " AS rcount");
      
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

      final int size = this.getTrailCount();
      if (size > 0) {
        var lastTrailedRow = getTrailByIndex(size - 1);
        // if the new trail is for a row number less than the last row witnessed,
        // discard it
        if (rowNum <= lastTrailedRow.rowNumber())
          return false;
        
        // if this higher row number was witnessed before the last
        // recorded row witnessed, balk. (Otherwise, the invariant
        // that the witness times be non-decreasing would be broken.)
        
        // this is unfortunate: it shouldn't happen along normal usage
        // however. The solution is to manually remove the trails from
        // the tables.
        if (trail.crum().utc() < lastTrailedRow.utc()) {
          var logger = Logger.getLogger(SqlHashLedger.class.getSimpleName());
          logger.warning(
              "row [" + lastTrailedRow.rowNumber() +
              "] is already recorded as being witnessed after (!) row [" + rowNum +
              "]: " + new Date(lastTrailedRow.utc()) + " v. " + new Date(trail.crum().utc()) +
              ". Not adding crumtrail for row [" + rowNum + "] as this would violate model invariants.");
          return false;
        }
      }
      
      try {
        
        var rs = this.chainTableCountStmt.executeQuery();
        if (!rs.next())
          throw new SqlLedgerException("failed to execute COUNT query on chain table");
        
        final int nextChainId = rs.getInt(1) + 1;
        
        if (nextChainId < 1)
          throw new SqlLedgerException(
              "nonsensical COUNT query on chain table " + (nextChainId - 1));
        

        StringBuilder sql =
            new StringBuilder("INSERT INTO " ).append(schema.getChainTable())
              .append(" (").append(CHN_ID).append(", ").append(N_HASH).append(") VALUES");
        
        var hashChain = trail.hashChain();
        int chainId = nextChainId;
        for (byte[] cHash : hashChain)
          sql.append("\n(").append(chainId++).append(", '").append(Base64_32.encode(cHash)).append("'),");
        
        // remove the last comma
        sql.setLength(sql.length() - 1);
        
        Statement stmt = con.createStatement();
        stmt.execute(sql.toString());
        
        int updateCount = stmt.getUpdateCount();
        
        if (updateCount != hashChain.size()) {
          con.rollback();
          throw new SqlLedgerException(
              "INSERT did not yield expected updateCount " + trail.hashChain().size() +
              "; actual was " + updateCount + "; SQL:\n" + sql);
        }
        
        
        final int trailId = size + 1;
        PreparedStatement trailInsert = getInsertTrailStmt();

        trailInsert.setInt(1, trailId);
        trailInsert.setLong(2, rowNum);
        trailInsert.setLong(3, trailedRecord.utc());
        trailInsert.setInt(4, trail.leafIndex());
        trailInsert.setInt(5, trail.leafCount());
        trailInsert.setInt(6, hashChain.size());
        trailInsert.setInt(7, nextChainId);
        
        trailInsert.executeUpdate();
        
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
        
        return getTrailedRow(rs);
        
      } catch (SQLException sqx) {
        throw new SqlLedgerException("on getTrailByIndex(" + index + "): " + sqx, sqx);
      }
    }
  }
  
  
  
  private TrailedRow getTrailedRow(ResultSet rs) throws SQLException {
    
    final int tid = rs.getInt(1);
    final long rowNumber = rs.getLong(2);
    final long utc = rs.getLong(3);
    final int leafIndex = rs.getInt(4);
    final int leafCount = rs.getInt(5);
    final int chainLen = rs.getInt(6);
    final int chainId = rs.getInt(7);
    
    if (chainLen != Proof.chainLength(leafCount, leafIndex))
      throw new SqlLedgerException(
          "chain-length assertion failed: chainLen=" + chainLen +
          ", leafCount=" + leafCount + ", leafIndex=" + leafIndex);
    
    if (rs.next())
      throw new SqlLedgerException(
          "multiple rows in result set: tid=" + tid + ", rowNumber=" + rowNumber);
    
    selectChainByChainId.setInt(1, chainId);
    selectChainByChainId.setInt(2, chainLen);
    
    rs = selectChainByChainId.executeQuery();
    
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
      
      lastLinkId = linkId;
      
      String encoded = rs.getString(2);
      chain[ci] = Base64_32.decode(encoded);
    }
    
    // sanity check
    if (lastLinkId != chainId + chainLen - 1)
      throw new SqlLedgerException(
            "assertion failed: lastLinkId=" + lastLinkId + "; chain_id=" + chainId +
            "; chain_len=" + chainLen +
            "; tid=" + tid);
    
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
        
        return rs.next() ? getTrailedRow(rs) : null;
        
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

  
  /**
   * {@inheritDoc}
   * 
   * <h4>Implementation Note</h4>
   * <p>
   * The SQL implementation performs this in 2 steps. The crumtrails are deleted in the
   * 1st transaction; the skip ledger rows are deleted in a 2nd transaction. It's punishment
   * for modularizing skipledger as its own standalone component :/
   * </p>
   */
  @Override
  public void trimSize(long newSize) {
    
    if (newSize < 0)
      throw new IllegalArgumentException("newSize: " + newSize);
    
    synchronized (lock) {
      
      try (var closer = new TaskStack()) {
        
        selectNearestTrailStmt.setLong(1, newSize);
        ResultSet rs = selectNearestTrailStmt.executeQuery();
        closer.pushClose(rs);
        
        if (rs.next()) {
          final int tid = rs.getInt(1);
          final long rowNumber = rs.getLong(2);
          final int chainLen = rs.getInt(6);
          final int chainId = rs.getInt(7);
          
          assert rowNumber >= newSize;
          
          boolean include = rowNumber > newSize;
          
          String trailDelSql =
              "DELETE FROM " + schema.getTrailTable() + " WHERE " + TRL_ID +
              (include ? " >= " : " > ") + tid;
          
          String chainDelSql =
              "DELETE FROM " + schema.getChainTable() + " WHERE " + CHN_ID + " >= " +
              (include ? chainId : chainId + chainLen);
          
          Statement stmt = con.createStatement();
          closer.pushClose(stmt);
          
          stmt.executeUpdate(trailDelSql);
          stmt.executeUpdate(chainDelSql);
        }
        
        con.commit();
        
        this.skipLedger.trimSize(newSize);
        
      } catch (SQLException sqx) {
        throw new SqlLedgerException("on lastWitnessedRowNumber(): " + sqx, sqx);
      }
    }
    
  }

}








