/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.mc.mgmt;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

import io.crums.util.TaskStack;

/**
 * Base class for the system's object-managers.
 */
public abstract class AbstractMgr implements AutoCloseable {
  
  protected final DbEnv env;
  
  protected final Connection con;

  /**
   * 
   */
  AbstractMgr(DbEnv env, Connection con) {
    this.env = Objects.requireNonNull(env, "null env");
    this.con = Objects.requireNonNull(con, "null con");
    ensureConnection();
  }
  
  
  protected void ensureConnection()
      throws ChainManagementException {

    if (env.readOnly())
      return;
    
    try {
      con.setAutoCommit(true);
      con.setReadOnly(false);
    } catch (SQLException sx) {
      throw new ChainManagementException(sx);
    }
  }
  
  
  /**
   * Check performed for write operations.
   * 
   * @throws UnsupportedOperationException
   *         if {@linkplain #isReadOnly()} returns {@code true}
   */
  protected final void checkWrite() throws UnsupportedOperationException {
    if (env.readOnly())
      throw new UnsupportedOperationException(
          "operation not supported without write-privileges");
  }
  
  /**
   * Check performed for deletes.
   * 
   * @throws UnsupportedOperationException
   *         if {@code env.}{@linkplain DbEnv#allowDelete() allowDelete()}
   *         returns {@code false}
   */
  protected final void checkDelete() throws UnsupportedOperationException {
    if (!env.allowDelete())
      throw new UnsupportedOperationException(
          "operation not supported; DbEnv does not allow deletes");
  }
  
  /** Executes ({@linkplain Statement#executeUpdate(String)}) sans substitutions. */
  protected void executeDdl(String sql) throws SQLException {
    var ddlStmt = con.createStatement();
    ddlStmt.executeUpdate(sql);
    ddlStmt.close();
  }
  
  
  protected PreparedStatement prepareStmt(String sql, TaskStack closer)
      throws SQLException {
    var stmt = prepareStmt(sql);
    closer.pushClose(stmt);
    return stmt;
  }
  
  /** @return {@code this.con.prepareStatement(env.applyTablePrefix(sql))} */
  protected PreparedStatement prepareStmt(String sql) throws SQLException {
    return con.prepareStatement(env.applyTablePrefix(sql));
  }
  
  
  protected ResultSet executeQuery(PreparedStatement stmt, TaskStack closer)
      throws SQLException {
    
    ResultSet rs = stmt.executeQuery();
    closer.pushClose(rs);
    return rs;
  }
  
  /**
   * Returns {@code true} if in read-only mode.
   * 
   * @return {@code env.readOnly()}
   */
  public final boolean isReadOnly() {
    return env.readOnly();
  }

  /** Closes the instance. No checked exceptions. */
  public abstract void close();
  
  
  

}
