/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.mc.mgmt;


import static io.crums.sldg.mc.mgmt.McMgmtConstants.*;
import static io.crums.sldg.mc.mgmt.SchemaConstants.*;

import java.lang.System.Logger.Level;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

import io.crums.sldg.mc.mgmt.Key.IntKey;
import io.crums.util.TaskStack;

/**
 * Manages the {@code chain_info} table.
 * 
 * @see SchemaConstants#CREATE_CHAIN_INFOS_TABLE
 */
public class ChainIdMgr extends DeletablesMgr {
  
  /**
   * Neutral/sentinel/initial .
   * {@code INIT_ID.}{@linkplain ChainId#isSet() isSet()} returns {@code false}.
   * 
   * @see ChainId#isSet()
   */
  public final static ChainId INIT_ID = new ChainId(0);
  
  /**
   * A chain is identified by this typed number. Excepting
   * {@linkplain ChainIdMgr#INIT_ID}, <em>all instances</em> are
   * created by this class, and thus encapsulate values read from the
   * database.
   * 
   * @see ChainIdMgr#INIT_ID
   */
  public final static class ChainId extends IntKey {
    
    private ChainId(int id) {
      super(id);
      assert id >= 0;
    }
    
    /**
     * Return {@code true}, if this value came from the database.
     */
    public boolean isSet() {
      return no() != 0;
    }
    
    @Override
    public boolean equals(Object o) {
      return o == this ||
          o instanceof ChainId other &&
          other.no() == this.no();
    }
    
  }
  
  
  public static abstract class ChainDepKey extends IntKey {
    
    private final ChainId chainId;

    ChainDepKey(int no, ChainId chainId) {
      super(no);
      this.chainId = Objects.requireNonNull(chainId, "null chainId");
    }
    
    
    public final ChainId chainId() {
      return chainId;
    }
    
    
    /** @return {@code "(" + no() + ":" + chainId().no() + ")"} */
    @Override
    public String toString() {
      return "(" + no() + ":" + chainId.no() + ")";
    }
    
  }
  
  
  public record InfoSansDesc(ChainId id, String name, Optional<String> uri) {
    public InfoSansDesc {
      Objects.requireNonNull(id, "null id");
      name = name.trim();
      if (name.isEmpty())
        throw new IllegalArgumentException("name is blank");
      uri = normalize(uri);
      checkUri(uri);
    }
    public InfoSansDesc(ChainId id, String name, String uri) {
      this(id, name, Optional.ofNullable(uri));
    }
  }
  
  
  /**
   * Loads an instance from the database, optionally creating the backing
   * table, if it doesn't already exist <em>and</em> if {@code env} is
   * in read/write mode ({@linkplain DbEnv#readWrite()}.
   * 
   * @param env   environment / context
   * @param con   database connection
   */
  public static ChainIdMgr ensureInstance(DbEnv env, Connection con)
      throws IllegalArgumentException, ChainManagementException {
    
    return new ChainIdMgr(env, con, true);
  }
  
  
  private final PreparedStatement selectById;
  private final PreparedStatement selectByName;
  private final PreparedStatement selectByUri;
  private final PreparedStatement insertStmt;
  private final PreparedStatement updateStmt;
  
  
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
  public ChainIdMgr(DbEnv env, Connection con)
      throws ChainManagementException {
    this(env, con, false);
  }
  

  private ChainIdMgr(DbEnv env, Connection con, boolean create)
      throws ChainManagementException {
    super(env, con);
    
    try (var closeOnFail = new TaskStack()) {
      // con argument NOT closed on failure
      
      if (create && env.readWrite()) {
        var sql = env.applyTablePrefix(CREATE_CHAIN_INFOS_TABLE);
        getLogger().log(Level.INFO, "Executing SQL DDL:%n%s".formatted(sql));
        executeDdl(sql);
      }
      
      this.selectById =
          prepareStmt(SELECT_CHAIN_INFO_BY_ID, closeOnFail);
      this.selectByName =
          prepareStmt(SELECT_CHAIN_INFO_BY_NAME, closeOnFail);
      this.selectByUri =
          prepareStmt(SELECT_CHAIN_INFO_BY_URI, closeOnFail);
      
      if (env.readOnly()) {
        this.insertStmt = null;
        this.updateStmt = null;
        
      } else {
        this.insertStmt = prepareStmt(INSERT_CHAIN_INFO, closeOnFail);
        this.updateStmt = prepareStmt(UPDATE_CHAIN_INFO);
      }
      closeOnFail.clear();
      
    } catch (SQLException sx) {
      throw new ChainManagementException(sx);
    }
  }


  protected final String deleteByIdSql() {
    return DELETE_CHAIN_INFO_BY_ID;
  }

  @Override
  public void close() {
    try (var closer = new TaskStack()) {
      closer.pushClose(con, selectById, selectByName, selectByUri);
      if (!env.readOnly())
        closer.pushClose(insertStmt, updateStmt);
      if (deleteById != null)
        closer.pushClose(deleteById);
    }
  }


  
  /**
   * Creates (adds) a entry for a new microchain in the system and returns
   * the result. 
   * 
   * @param args an "args"-instance
   * 
   * @return a {@code ChainInfo} with set key
   * 
   * @throws IllegalArgumentException
   *         if not an "args" instance; if the name is not available; if
   *         the URI is not available
   * @throws ChainManagementException
   *         if an internal error occurs
   * @throws ConcurrentChainModificationException
   *         if a concurrent modification (by another instance of this
   *         class) causes a name/uri uniqueness-violation, then the
   *         row is (marked) deleted and this exception is thrown
   * @throws UnsupportedOperationException
   *         if {@linkplain #isReadOnly()} returns {@code true}
   */
  public synchronized ChainInfo newChainInfo(ChainInfo args)
      throws
      IllegalArgumentException,
      ChainManagementException,
      UnsupportedOperationException {
    
    checkWrite();
    requireArgsInstance(args);
    
    try (var closer = new TaskStack()) {
      
      verifyNameAvailable(args);
      verifyUriAvailable(args);
      
      insertStmt.setString(1, args.name());
      setString(insertStmt, 2, args.description());
      setString(insertStmt, 3, args.uri());
      {
        int count = insertStmt.executeUpdate();
        assert count == 1;
      }
      
      selectByName.setString(1, args.name());
      ResultSet rs = executeQuery(selectByName, closer);
      
      List<ChainInfo> matches = toChainInfoList(rs);
      
      final ChainInfo created;
      
      switch (matches.size()) {
      case 0: throw new ChainManagementException(
          "internal error: query-by-name fails after insert: " + args);
      case 1:
        created = matches.get(0);
        break;
      default:
        rollbackInsert(matches, args);
        throw new ConcurrentChainModificationException(
            "race on creating new entry for %s: name conflict");
      }
      
      if (!created.equalsIgnoringId(args))
        throw new ChainManagementException(
          "internal error: insert failed. Expected %s (ignoring id); actual %s"
          .formatted(args, created));
      
      if (args.uri().isPresent()) {
        
        selectByUri.setString(1, args.uri().get());
        rs = executeQuery(selectByUri, closer);
        
        matches = toChainInfoList(rs);
        
        switch (matches.size()) {
        case 0: throw new ChainManagementException(
            "internal error: query-by-URI fails after insert: " + args);
        case 1:
          if (!matches.get(0).equals(created))
            throw new ChainManagementException(
                """
                internal error: query-by_URI fails after insert. \
                Expected %s; actual %s"""
                .formatted(created, matches.get(0)));
          break;
        default:
          rollbackInsert(matches, args);
          throw new ConcurrentChainModificationException(
              "race on creating new entry for %s: URI conflict");
        }
      }
      
      return created;
      
    } catch (SQLException sx) {
      throw new ChainManagementException(sx);
    } 
  }
  
  
  
  /**
   * <em>Deletes</em> the specified microchain.
   * 
   * @return {@code false}, if already deleted
   * 
   * @throws UnsupportedOperationException
   *         if environment does not have delete privileges
   */
  public synchronized boolean deleteChain(ChainId id)
      throws ChainManagementException, UnsupportedOperationException {
    
    checkDelete();
    checkIdSet(id);
    
    try {
      boolean deleted = deleteById(id.no());
      getLogger().log(
          Level.INFO,
          "chain [%d] %sDELETED".formatted(id.no(), deleted ? "" : "already "));
      return deleted;
    
    } catch (SQLException sx) {
      throw new ChainManagementException(sx);
    }
  }
  
  
  /**
   * Changes the name, description, or URI of the specified microchain.
   * 
   * @param id    chain identifier
   * @param args  an "args"-instance
   * 
   * @return the updated entry
   * 
   * @throws ChainManagementException
   *         if an internal error occurs
   * @throws ConcurrentChainModificationException
   *         if a concurrent modification (by another instance of this
   *         class) causes a race
   * @throws UnsupportedOperationException
   *         if the instance {@linkplain #isReadOnly()}
   */
  public synchronized ChainInfo update(ChainId id, ChainInfo args)
      throws ChainManagementException, UnsupportedOperationException {
    checkWrite();
    checkIdSet(id);
    requireArgsInstance(args);
    
    try (var closer = new TaskStack()) {
      selectById.setInt(1, id.no());
      ResultSet rs = executeQuery(selectById, closer);
      if (!rs.next())
        throw new IllegalArgumentException(
            "No such id (%s); could it be deleted?".formatted(id));
      
      final var existing = toChainInfo(rs);
      if (existing.equalsIgnoringId(args))
        return existing;
      
      closer.pop();
      
      final boolean nameChange = !existing.name().equals(args.name());
      if (nameChange)
        verifyNameAvailable(args);
      
      final boolean uriChange = !existing.uri().equals(args.uri());
      if (uriChange)
        verifyUriAvailable(args);
      
      updateStmt.setString(1, args.name());
      setString(updateStmt, 2, args.description());
      setString(updateStmt, 3, args.uri());
      updateStmt.setInt(4, id.no());
      
      updateStmt.executeUpdate();
      

      selectById.setInt(1, id.no());
      rs = executeQuery(selectById, closer);
      
      if (!rs.next())
        throw new ConcurrentChainModificationException(
            "chain [%s] disappeared midflight; was it deleted?"
            .formatted(id));
      final var updatedInfo = toChainInfo(rs);
      
      // sanity check-up, then return
      if (updatedInfo.equalsIgnoringId(args))
        return updatedInfo;
      

      // sorry, we have bad news..
      
      if (updatedInfo.equals(existing))
        throw new ChainManagementException(
            """
            internal error. Update of chain [%s] was ineffective: \
            args %s; after update %s"""
            .formatted(id, args, updatedInfo));
      else
        throw new ConcurrentChainModificationException(
            """
            chain [%s] appears to have been updated ouside this thread \
            of execution: expected set to %s; actual was %s"""
            .formatted(id, args, updatedInfo));
      
    } catch (SQLException sx) {
      throw new ChainManagementException(sx);
      
    }
  }
  
  
  /**
   * Lists the {@linkplain ChainInfo} entries sans descriptions.
   * 
   * @return immutable, not {@code null}
   * 
   * @throws ChainManagementException
   *         if an internal error occurs
   */
  public synchronized List<InfoSansDesc> listNameUris()
      throws ChainManagementException {
    
    try (var stmt = con.createStatement()) {
      
      ResultSet rs = stmt.executeQuery(
          env.applyTablePrefix(SELECT_CHAIN_INFO_SANS_DESC_ALL));
      
      ArrayList<InfoSansDesc> list = new ArrayList<>();
      while (rs.next()) {
        ChainId id = new ChainId( rs.getInt(1));
        String name = rs.getString(2);
        String uri = rs.getString(3);
        list.add(new InfoSansDesc(id, name, uri));
      }
      
      return list.isEmpty() ? List.of() : Collections.unmodifiableList(list);
      
    } catch (SQLException sx) {
      throw new ChainManagementException(sx);
    }
  }
  
  
  /**
   * Lists <em>all</em> the {@linkplain ChainInfo} entries.
   * 
   * @throws ChainManagementException
   *         if an internal error occurs
   */
  public synchronized List<ChainInfo> list() throws ChainManagementException {
    try (ResultSet rs =
          con.createStatement().executeQuery(
          env.applyTablePrefix(SELECT_CHAIN_INFO_ALL))) {
      
      var list = toChainInfoList(rs);
      return list.isEmpty() ? List.of() : Collections.unmodifiableList(list);
      
    } catch (SQLException sx) {
      throw new ChainManagementException(sx);
    }
  }
  
  

  /**
   * Finds and returns the entry with the specified name.
   * 
   * @param name   not blank
   * 
   * @throws ChainManagementException
   *         if an internal error occurs
   */
  public synchronized Optional<ChainInfo> findByName(String name)
      throws ChainManagementException {
    
    if (name.isBlank())
      throw new IllegalArgumentException("blank name");
    
    return findByImpl(selectByName, name);
  }
  
  
  /**
   * Finds and returns the entry with the specified URI.
   * 
   * @param uri   well-formed URI
   * 
   * @throws ChainManagementException
   *         if an internal error occurs
   */
  public synchronized Optional<ChainInfo> findByUri(String uri)
      throws ChainManagementException {
    
    checkUri(uri);
    return findByImpl(selectByUri, uri);
  }
  
  
  private Optional<ChainInfo> findByImpl(PreparedStatement query, String value) {
    try (var closer = new TaskStack()) {
      
      query.setString(1, value);
      ResultSet rs = query.executeQuery();
      closer.pushClose(rs);
      
      return rs.next() ?
          Optional.of( toChainInfo(rs) ) :
            Optional.empty();
    
    } catch (SQLException sx) {
      throw new ChainManagementException(sx);
    }
  }
  
  
  /**
   * Returns the entry with the specified {@code id}.
   * 
   * @param id  chain ID (&gt; 0)
   * 
   * @return
   * @throws IllegalArgumentException
   *         if {@code id <= 0}
   * @throws NoSuchElementException
   *         if no entry with the chain is found
   */
  public synchronized ChainInfo getById(int id)
      throws IllegalArgumentException, NoSuchElementException, ChainManagementException {
    
    if (id <= 0)
      throw new IllegalArgumentException("positive id expected; actual was " + id);
    
    try (var closer = new TaskStack()) {
      
      selectById.setInt(1, id);
      ResultSet rs = selectById.executeQuery();
      closer.pushClose(rs);
      
      if (!rs.next())
        throw new NoSuchElementException("id: " + id);
      
      return toChainInfo(rs);
      
      
    } catch (SQLException sx) {
      throw new ChainManagementException(sx);
    }
    
  }
  
  
  
  
  
  
  
  
  
  
  
  private void requireArgsInstance(ChainInfo arg) {
    if (!arg.isArgs())
      throw new IllegalArgumentException(
          "\"args\"-instance expected; actual given: " + arg);
  }
  
  
  private void checkIdSet(ChainId id) throws IllegalArgumentException {
    if (!id.isSet())
      throw new IllegalArgumentException("default id (0) not allowed");
  }
  
  
  private void setString(
      PreparedStatement stmt, int col, Optional<String> value)
          throws SQLException {
    if (value.isEmpty())
      stmt.setNull(col, Types.VARCHAR);
    else
      stmt.setString(col, value.get());
  }
  
  private void rollbackInsert(List<ChainInfo> list, ChainInfo arg)
      throws SQLException {
    assert list.size() > 1;
    ChainId lastId =
        list.stream()
        .filter(arg::equalsIgnoringId)
        .map(ChainInfo::id)
        .max(ChainId.COMPARATOR)
        .orElseThrow(() -> new ChainManagementException(
            "internal error: arg %s (ignoring id) not found post insert"
            .formatted(arg)));
    if (!deleteById(lastId.no()))
      getLogger().log(
          Level.WARNING,
          "race-on-race: ignoring race on deleting last created ChainInfo [%d]"
          .formatted(lastId.no()));
      
  }
  
  private boolean deleteById(int chainId) throws SQLException {
    deleteById().setInt(1, chainId);
    int count = deleteById.executeUpdate();
    assert count < 2;
    return count == 1;
  }
  

  private boolean verifyNameAvailable(ChainInfo chainInfo)
      throws SQLException, IllegalArgumentException {
    return verifyNameAvailable(chainInfo, true);
  }
  
  private boolean verifyNameAvailable(ChainInfo chainInfo, boolean fail)
      throws SQLException, IllegalArgumentException {
    
    return
        verifyValueAvailable(
            chainInfo.name(),
            selectByName,
            chainInfo,
            "name",
            fail);
  }
  
  
  
  private boolean verifyUriAvailable(ChainInfo chainInfo)
      throws SQLException, IllegalArgumentException {
    return verifyUriAvailable(chainInfo, true);
  }
  
  private boolean verifyUriAvailable(ChainInfo chainInfo, boolean fail)
      throws SQLException, IllegalArgumentException {
    
    return
        chainInfo.uri().isEmpty() ||
        verifyValueAvailable(
            chainInfo.uri().get(),
            selectByUri,
            chainInfo,
            "URI",
            fail);
  }
  
  private boolean verifyValueAvailable(
      String value, PreparedStatement query, ChainInfo chainInfo, String argName, boolean fail)
          throws SQLException, IllegalArgumentException {
    query.setString(1, value);
    try (ResultSet rs = query.executeQuery()) {
      boolean found = rs.next();
      if (found && fail)
        throw new IllegalArgumentException(
            "%s '%s' already exists: argument %s; existing %s"
            .formatted(argName, value, chainInfo, toChainInfo(rs)));
      return !found;
    }
  }
  
  
  private List<ChainInfo> toChainInfoList(ResultSet rs) throws SQLException {
    List<ChainInfo> list = new ArrayList<>();
    while (rs.next())
      list.add(toChainInfo(rs));
    return list;
  }
  
  private ChainInfo toChainInfo(ResultSet rs) throws SQLException {
    ChainId chainId = new ChainId( rs.getInt(1) );
    if (chainId.no() <= 0)
      throw new ChainManagementException(
          "internal error: Positive chain_id invariant violated: " + chainId);
    String name = rs.getString(2);
    String desc = rs.getString(3);
    String uri = rs.getString(4);
    try {
      return new ChainInfo(chainId, name, desc, uri);
    } catch (Exception x) {
      throw new ChainManagementException(x);
    }
  }
  

  
  

}







