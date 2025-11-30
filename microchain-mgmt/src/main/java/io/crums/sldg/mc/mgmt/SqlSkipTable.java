/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.mc.mgmt;


import static io.crums.sldg.mc.mgmt.McMgmtConstants.getLogger;
import static io.crums.sldg.mc.mgmt.SchemaConstants.*;
import static io.crums.sldg.SldgConstants.HASH_WIDTH;

import java.lang.System.Logger.Level;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import io.crums.io.buffer.BufferUtils;
import io.crums.sldg.HashConflictException;
import io.crums.sldg.SkipTable;
import io.crums.util.Base64_32;
import io.crums.util.TaskStack;

/**
 * Skip table in a relational database. The skip table is also
 * called the <em>commit</em> table. This uses a conservative design.
 * Hashes are Base64-32 encoded and each committed row is estimated
 * to incur about 100 bytes of storage.
 * 
 * @see SkipTable
 */
public class SqlSkipTable extends AbstractMgr
    implements SkipTable {
  
  

  /**
   * Loads an instance from the database, optionally creating the backing
   * table, if it doesn't already exist <em>and</em> if {@code env} is
   * in read/write mode ({@linkplain DbEnv#readWrite()}.
   * 
   * @param env   environment / context
   * @param con   database connection
   */
  public SqlSkipTable ensureInstance(DbEnv env, Connection con, String commitTable)
      throws IllegalArgumentException, ChainManagementException {
    
    return new SqlSkipTable(env, con, commitTable, true);
  }
  
  
  
  private final String commitTable;
  
  private final PreparedStatement rowByIndex;
  private final PreparedStatement selectMaxIdx;
  private final PreparedStatement selectCount;
  private final PreparedStatement insertCommit;
  private int checkPeriod = 10;
  private int checkCountdown;
  private long lastSize;
  
  
  

  /**
   * Constructs a new instance. The backing table is assumed to
   * already exist.
   * 
   * @param env   environment / context
   * @param con   database connection
   * 
   * @throws ChainManagementException
   *         if an internal error occurs
   */
  public SqlSkipTable(DbEnv env, Connection con, String commitTable)
      throws IllegalArgumentException, ChainManagementException {
    
    this(env, con, commitTable, false);
  }

  /**
   * 
   */
  private SqlSkipTable(
      DbEnv env, Connection con, String commitTable, boolean create) {
    super(env, con);
    this.commitTable = commitTable;
    if (commitTable.isBlank())
      throw new IllegalArgumentException("blank commitTable");
    
    try (var closeOnFail = new TaskStack()) {
      
      if (create && env.readWrite()) {
        var sql =
            env.applyWildCard(CREATE_COMMIT_TABLE, commitTable);
        getLogger().log(Level.INFO, "Executing SQL DDL:%n%s".formatted(sql));
        executeDdl(sql);
      }
      
      this.rowByIndex = prepareStmt(SELECT_COMMIT_BY_ROW_INDEX, closeOnFail);
      this.selectMaxIdx = prepareStmt(SELECT_MAX_COMMIT_IDX, closeOnFail);
      this.selectCount = prepareStmt(SELECT_COMMIT_COUNT, closeOnFail);
      
      this.insertCommit = env.allowCommit() ?
          prepareStmt(INSERT_COMMIT, closeOnFail) : null;
      
      verifySize();

      closeOnFail.clear();
    } catch (SQLException sx) {
      throw new ChainManagementException(sx);
    }
  }
  
  
  /** Note, <em>not synchronized</em>. */
  @Override
  public void close() {
    var closer = new TaskStack();
    closer.pushClose(rowByIndex, selectMaxIdx, selectCount);
    if (insertCommit != null)
      closer.pushClose(insertCommit);
    closer.close();
  }
  
  
  public synchronized void setSizeCheckPeriod(int period) {
    if (period < 0)
      throw new IllegalArgumentException("negative period: " + period);
    this.checkPeriod = period;
  }
  
  
  public final int getSizeCheckPeriod() {
    return checkPeriod;
  }
  
  
  
  
  private void verifySize()
      throws SQLException, ChainManagementException {
    long count;
    try (ResultSet rs = selectCount.executeQuery()) {
      if (!rs.next())
        throw new ChainManagementException(
            "internal error. Count query for table '%s' returned no result set"
            .formatted(commitTable));
      
      count = rs.getLong(1);
    }
    if (count != lastSize)
      throw new ChainManagementException(
          """
          internal error. Expected size (via MAX index) %d; \
          actual (via COUNT) was %d""".formatted(lastSize, count));
  }




  @Override
  protected PreparedStatement prepareStmt(String sql) throws SQLException {
    return con.prepareStatement(env.applyWildCard(sql, commitTable));
  }



  @Override
  protected void ensureConnection() throws ChainManagementException {
    if (!env.allowCommit())
      return;
    try {
      con.setReadOnly(false);
      con.setAutoCommit(false);
    } catch (SQLException sx) {
      throw new ChainManagementException(sx);
    }
  }
  

  /**
   * 
   * @throws ChainManagementException
   *         if an internal error occurs; illegal arguments are
   *         also system errors and are thrown as this type
   *         (in lieu of {@code IllegalArgumentException})
   * @throws HashConflictException
   *         if a referenced row has already been written (maybe because of
   *         a race), <em>and</em> the given argument conflicts with the
   *         already-committed value
   */
  @Override
  public synchronized long writeRows(ByteBuffer rows, long index)
      throws ChainManagementException, HashConflictException {
    
    checkCommit();
    
    final int count = rows.remaining() / ROW_WIDTH;
    if (count == 0 || count * ROW_WIDTH != rows.remaining())
      throw new ChainManagementException(
          "remaining bytes not a positive multiple of row-width (%d): %s"
          .formatted(ROW_WIDTH, rows));
    
    // the happy path..
    if (index == lastSize) try {
      do {
        writeOneRow(BufferUtils.slice(rows, ROW_WIDTH), index);
        ++lastSize;
        ++index;
      } while (rows.hasRemaining());
      con.commit();
      
      return lastSize;
    
    } catch (SQLException sx) {
      throw new ChainManagementException(
          "error on write [%d] to table '%s': %s"
          .formatted(index, commitTable, sx), sx);
    }
    
    // huh.. sanity-check the index
    if (index < 0L)
      throw new ChainManagementException(
          "internal error. Negative index [%d] invoked on write to table '%s'"
          .formatted(index, commitTable));
    if (index > lastSize && index > size())
      throw new ChainManagementException(
          """
          internal error. Out-of-bounds index [%d] (size=%d) \
          invoked on write to table '%s'"""
          .formatted(index, lastSize, commitTable));
    
    // verify the inputs match (cuz we're idempotent)
    do {
      var existingRow = readRow(index);
      var argRow = BufferUtils.slice(rows, ROW_WIDTH);
      if (!existingRow.equals(argRow))
        throw new HashConflictException(
            "argument %s conflicts with record at index [%d] in table '%s'"
            .formatted(rows, index, commitTable));
    } while (++index < lastSize && rows.hasRemaining());
    
    
    return rows.hasRemaining() ? writeRows(rows, index) : lastSize;
  }
  
  
  private void writeOneRow(ByteBuffer row, long index) throws SQLException {
    insertCommit.setLong(1, index);
    insertCommit.setString(2,
        Base64_32.encode(BufferUtils.slice(row, HASH_WIDTH)));
    insertCommit.setString(3, Base64_32.encode(row));
    int updateCount = insertCommit.executeUpdate();
    assert updateCount == 1;
  }

  @Override
  public synchronized ByteBuffer readRow(long index) {
    if (index < 0L || index >= lastSize && index >= size())
      throw new ChainManagementException(
          "out-of-bounds index [%d] on table '%s'; current size is %d"
          .formatted(index, commitTable, lastSize));
    try (var closer = new TaskStack()) {

      rowByIndex.setLong(1, index);
      ResultSet rs = executeQuery(rowByIndex, closer);
      if (!rs.next())
        throw makeReadException(index, "empty result set").fillInStackTrace();
      
      byte[] row = new byte[ROW_WIDTH];
      Base64_32.decode(rs.getString(1), row, 0);
      Base64_32.decode(rs.getString(2), row, HASH_WIDTH);
      
      return ByteBuffer.wrap(row).asReadOnlyBuffer();
      
    } catch (SQLException sx) {
      throw makeReadException(index, sx).fillInStackTrace();
    }
  }
  
  private ChainManagementException makeReadException(long index, Object detail) {
    var msg = "error on reading index [%d] from table %s -- detail: %s"
        .formatted(index, commitTable, detail);
    
    return detail instanceof Exception cause ?
        new ChainManagementException(msg, cause) :
          new ChainManagementException(msg);
  }

  @Override
  public synchronized long size() throws ChainManagementException {
    lastSize = 0L;
    try (var closer = new TaskStack()) {
      
      ResultSet rs = executeQuery( selectMaxIdx, closer);
      
      final long sz;
      if (rs.next()) {
        long maxIdx = rs.getLong(1);
        if (maxIdx < 0L)
          throw new ChainManagementException(
            "internal error. Size (MAX) query for table '%s' returned %d"
            .formatted(commitTable, maxIdx));
        sz = maxIdx + 1L;       // since index is zero-based
      } else
        sz = 0L;
      
      if (lastSize > sz)
        getLogger().log(
          Level.WARNING,
          "Table '%s' rolled back by outside process or thread (%d -> %d)"
          .formatted(commitTable, lastSize, sz));
      
      lastSize = sz;
      if (checkCountdown-- <= 0) {
        verifySize();
        checkCountdown = checkPeriod;
      }
      return sz;
      
    } catch (SQLException sx) {
      throw makeException(sx).fillInStackTrace();
    }
  }
  
  

  @Override
  public synchronized void trimSize(long newSize) {
    if (!env.allowRollback())
      throw new UnsupportedOperationException("rollback not supported: " + env);
    
    if (newSize < 0L)
      throw new IllegalArgumentException("negative newSize: " + newSize);
    
    if (newSize > size())
      throw new IllegalArgumentException(
          "newSize %d > size %d".formatted(newSize, lastSize));
    
    getLogger().log(
        Level.WARNING,
        "Rolling back commit table %s: %d -> %d"
        .formatted(commitTable, lastSize, newSize));
    
    try (var stmt = con.createStatement()) {
      
      var sql = env.applyWildCard(TRIM_COMMIT, commitTable) + newSize;
      stmt.execute(sql);
      con.commit();
    
    } catch (SQLException sx) {
      throw makeException(sx).fillInStackTrace();
    }
  }
  
  
  private ChainManagementException makeException(SQLException sx) {
    return new ChainManagementException(
        "internal error. Commit table '%s': %s".formatted(commitTable, sx),
        sx);
  }
  
  
  
  private void checkCommit() {
    if (env.allowCommit())
      return;
    throw new UnsupportedOperationException(
        "commit not supported: " + env);
  }

}

























