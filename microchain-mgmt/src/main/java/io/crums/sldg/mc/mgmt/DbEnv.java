/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.mc.mgmt;


import java.util.Objects;

import io.crums.sldg.mc.mgmt.ChainIdMgr.ChainId;

/**
 * Encapsulates database conventions and context (permisssions) instances
 * of the system operate in.
 */
public record DbEnv(
    String tablePrefix,
    boolean readOnly,
    boolean allowDelete,
    boolean allowCommit,
    boolean allowRollback) {
  
  
  
  public final static DbEnv DEFAULT = new DbEnv();
  
  
  public DbEnv {
    tablePrefix = tablePrefix.trim();
    if (allowDelete)
      readOnly = false;
    if (!allowCommit)
      allowRollback = false;
  }
  
  
  /**
   * The default instance is read-only.
   * 
   * @see McMgmtConstants#SLDG_PREFIX
   */
  public DbEnv() {
    this(McMgmtConstants.SLDG_PREFIX, true, false, false, false);
  }
  
  /**
   * Returns {@code true} if in read-only mode.
   */
  public boolean readOnly() {
    return readOnly;
  }

  /**
   * Sets the property and returns a new instance, if changed.
   */
  public DbEnv readOnly(boolean readOnly) {
    if (this.readOnly == readOnly)
      return this;
    return new DbEnv(tablePrefix, readOnly, allowDelete, allowCommit, allowRollback);
  }
  
  
  /**
   * Returns {@code true} if in read/write mode.
   * 
   * @return {@code !readOnly()}
   */
  public boolean readWrite() {
    return !readOnly;
  }

  /**
   * Sets the property and returns a new instance, if changed.
   */
  public DbEnv readWrite(boolean write) {
    return readOnly(!write);
  }
  
  
  /**
   * Returns the table-name prefix for system tables created by the
   * library.
   */
  public String tablePrefix() {
    return tablePrefix;
  }
  

  /**
   * Sets the property and returns a new instance, if changed.
   */
  public DbEnv tablePrefix(String tablePrefix) {
    if (this.tablePrefix.equals(tablePrefix))
      return this;
    return new DbEnv(tablePrefix, readOnly, allowDelete, allowCommit, allowRollback);
  }
  
  
  /**
   * Returns {@code true} if deletes are allowed. This is a permission
   * <em>on top of</em> read/write. That is, if {@code true}, then
   * {@linkplain #readWrite()} is also {@code true}.
   */
  public boolean allowDelete() {
    return allowDelete;
  }
  
  /**
   * Sets the property and returns a new instance, if changed.
   */
  public DbEnv allowDelete(boolean allow) {
    if (allowDelete == allow)
      return this;
    // logically
    //   return new DbEnv(tablePrefix, false, allow);
    // but this is less cognitive-load ..
    return new DbEnv(tablePrefix, readOnly, allow, allowCommit, allowRollback);
  }
  
  

  /**
   * Returns {@code true} if new ledger commitments are allowed. This permission
   * is <em>indepedent of</em> read/write permissions.
   */
  public boolean allowCommit() {
    return allowCommit;
  }

  /**
   * Sets the property and returns a new instance, if changed.
   */
  public DbEnv allowCommit(boolean allow) {
    if (allowCommit == allow)
      return this;
    return new DbEnv(tablePrefix, readOnly, allowDelete, allow, allowRollback);
  }
  
  /**
   * Returns {@code true} if ledger commitment-rollbacks are allowed.
   * If so, then {@linkplain #allowCommit()} also returns {@code true}.
   * This permission is <em>indepedent of</em> read/write permissions.
   */
  public boolean allowRollback() {
    return allowRollback;
  }

  /**
   * Sets the property and returns a new instance, if changed.
   */
  public DbEnv allowRollback(boolean allow) {
    if (allowRollback == allow)
      return this;
    boolean newAllowCommit = allow ? true : allowCommit;
    return new DbEnv(tablePrefix, readOnly, allowDelete, newAllowCommit, allow);
  }
  
  
  /**
   * Applies table-prefix substitutions encoded as {@code "%s"} in the given
   * (assumed SQL) string and returns it.
   * 
   * 
   */
  public String applyTablePrefix(String sql) {
    return applyWildCard(sql, tablePrefix);
  }
  
  
  public String applyWildCard(String sql, String wildCardValue) {
    
    Objects.requireNonNull(wildCardValue, "null wildCardValue");
    int substitutions = 0;
    
    for (String search = sql; !search.isEmpty(); ) {
      int index = search.indexOf("%s");
      if (index == -1)
        break;
      
      ++substitutions;
      search = search.substring(index + 2);
    }
    
    if (substitutions == 0)
      return sql;
    
    Object[] fArgs = new Object[substitutions];
    for (int index = fArgs.length; index-- > 0; )
      fArgs[index] = wildCardValue;
    
    return sql.formatted(fArgs);
    
  }
  
  
  /** Default commit table name for {@linkplain ChainId}. */
  public String defaultCommitTable(ChainId chainId) {
    var s =
        new StringBuilder(tablePrefix)
        .append(McMgmtConstants.COMMIT_TABLE_PRE_SUBFIX);
    if (chainId.no() < 10)
      s.append('0');
    s.append(chainId.no());
    return s.toString();
  }
  
  
  

}
