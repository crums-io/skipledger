/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.mc.mgmt;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 
 */
abstract class DeletablesMgr extends AbstractMgr {

  protected PreparedStatement deleteById;
  
  DeletablesMgr(DbEnv env, Connection con) {
    super(env, con);
  }
  
  
  protected PreparedStatement deleteById() throws SQLException {
    if (deleteById == null)
      deleteById = con.prepareStatement(
          env.applyTablePrefix(deleteByIdSql()));
    
    return deleteById;
  }
  
  
  /** Implementations are marked <strong>final</strong>. */
  protected abstract String deleteByIdSql();

}
