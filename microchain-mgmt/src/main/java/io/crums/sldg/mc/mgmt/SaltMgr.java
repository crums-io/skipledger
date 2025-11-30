/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.mc.mgmt;


import static io.crums.sldg.mc.mgmt.McMgmtConstants.getLogger;
import static io.crums.sldg.mc.mgmt.SchemaConstants.*;

import java.lang.System.Logger.Level;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import io.crums.sldg.mc.mgmt.ChainIdMgr.ChainDepKey;
import io.crums.sldg.mc.mgmt.ChainIdMgr.ChainId;
import io.crums.sldg.salt.EpochedTableSalt;
import io.crums.sldg.salt.TableSalt;
import io.crums.util.Base64_32;
import io.crums.util.Lists;
import io.crums.util.TaskStack;

/**
 * Manages table salt used for ledger row commitments.
 * <p>
 * Ledger <em>salts are secrets</em>. Therefore, access to this information
 * must somehow be managed.
 * </p>
 */
public class SaltMgr extends DeletablesMgr {
  
  
  /**
   * Primary key.
   */
  public final static class SaltId extends ChainDepKey {
    
    private SaltId(int id, ChainId chainId) {
      super(id, chainId);
    }
    
    @Override
    public boolean equals(Object o) {
      return
          o == this ||
          o instanceof SaltId other &&
          other.no() == this.no();
    }
  }
  
  
  /**
   * Table salt seeds are defined at a starting row no. This class
   * encapsulates the seed/start-row tuple.
   */
  final static class EpochSeed extends EpochedTableSalt.EpochSeed {
    
    private final SaltId saltId;
    
    private EpochSeed(SaltId saltId, long startRow, String base64Seed) {
      super(startRow, Base64_32.decode(base64Seed));
      this.saltId = saltId;
    }
    
    public SaltId saltId() {
      return saltId;
    }
    
  }
  
  /**
   * {@linkplain EpochSeed}, sans seed.
   */
  record Epoch(SaltId saltId, long startRow) implements Comparable<Epoch> {
    Epoch {
      if (startRow < 1L)
        throw new ChainManagementException(
            "startRow positive invariant violated: " + startRow);
      
    }

    /** Ordered by {@linkplain #startRow()}. */
    @Override
    public int compareTo(Epoch o) {
      return Long.compare(startRow, o.startRow);
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
  public static SaltMgr ensureInstance(DbEnv env, Connection con) {
    return new SaltMgr(env, con, true);
  }
  
  
  
  // - -  I N S T A N C E   M E M B E R S - -
  
  
  private PreparedStatement epochSeedsByChainId;
  private PreparedStatement epochsSansSeed;
  private PreparedStatement insertStmt;
  
  
  
  

  /**
   * Constructs a new instance. The backing table is assumed to
   * already exist.
   * 
   * @param env   environment / context
   * @param con   database connection
   * 
   * @throws ChainManagementException
   *         if an internal error occurs
   *         
   * @see #ensureInstance(DbEnv, Connection)
   */
  public SaltMgr(DbEnv env, Connection con) {
    this(env, con, false);
  }

  
  private SaltMgr(DbEnv env, Connection con, boolean create) {
    super(env, con);
    
    try (var closeOnFail = new TaskStack()) {
      
      closeOnFail.push(con);
      
      if (create && env.readWrite()) {
        var sql = env.applyTablePrefix(CREATE_LEDGER_SALTS_TABLE);
        getLogger().log(Level.INFO, "Executing SQL DDL:%n%s".formatted(sql));
        executeDdl(sql);
      }
      
      this.epochSeedsByChainId =
          prepareStmt(SELECT_LEDGER_SALTS_BY_CHAIN_ID, closeOnFail);
      
      if (env.readOnly()) {
        this.epochsSansSeed = null;
        this.insertStmt = null;
      } else {
        this.epochsSansSeed =
            prepareStmt(SELECT_LEDGER_SALTS_SANS_SEED, closeOnFail);
        this.insertStmt = prepareStmt(INSERT_LEDGER_SALT);
      }
      
      closeOnFail.clear();
    } catch (SQLException sx) {
      throw new ChainManagementException(sx);
    }
  }

  @Override
  public void close() {
    var closer = new TaskStack();
    closer.pushClose(con, epochSeedsByChainId);
    if (env.readWrite())
      closer.pushClose(epochsSansSeed, insertStmt);
    if (deleteById != null)
      closer.pushClose(deleteById);
    closer.close();
  }

  @Override
  protected final String deleteByIdSql() {
    return DELETE_LEDGER_SALT_BY_SALT_ID;
  }
  
  
  
  /**
   * Finds table salt for the given chain, and if found returns it.
   */
  public synchronized Optional<TableSalt> findTableSalt(ChainId chainId)
      throws ChainManagementException {
    
    try (var closer = new TaskStack()) {
      
      epochSeedsByChainId.setInt(1, chainId.no());
      ResultSet rs = epochSeedsByChainId.executeQuery();
      closer.pushClose(rs);
      
      List<EpochSeed> epochSeeds = toEpochSeeds(rs, chainId);
      
      if (epochSeeds.isEmpty())
        return Optional.empty();
      
      try {
        return Optional.of(
            EpochedTableSalt.createTableSalt(epochSeeds));
      
      } catch (Exception x) {
        throw new ChainManagementException(
            "internal error on constructing TableSalt for chain %s: %s"
            .formatted(chainId, x), x);
      }
    } catch (SQLException sx) {
      throw new ChainManagementException(sx);
    }
  }
  
  
  
  
  /**
   * Returns the starting row numbers for the specified chain. This
   * operation <em>requires write-privileges</em>, since this information
   * may be a secret.
   * 
   * @param chainId the chain's ID
   * 
   * @throws UnsupportedOperationException
   *         this operation is only supported with write-privileges
   *         (even tho it's a read-operation)
   */
  public synchronized List<Long> listStartRows(ChainId chainId)
      throws UnsupportedOperationException, ChainManagementException {
    
    checkWrite();
    return listStartRowsImpl(chainId);
  }
  
  
  /**
   * Returns {@code true} iff there exists at least one epoch salt
   * for the specified chain.
   * 
   * @param chainId the chain's ID
   * 
   */
  public synchronized boolean saltConfigured(ChainId chainId)
      throws ChainManagementException {
    
    return !listStartRows(chainId).isEmpty();
  }
  
  
  
  private List<Long> listStartRowsImpl(ChainId chainId)
      throws ChainManagementException {

    
    try (var closer = new TaskStack()) {
      
      epochsSansSeed.setInt(1, chainId.no());
      ResultSet rs = epochsSansSeed.executeQuery();
      closer.pushClose(rs);
      
      
      
      var startRows =
          toEpochs(rs, chainId).stream()
          .map(Epoch::startRow)
          .sorted().toList();
      
      checkIntegrity(startRows, chainId);
    
      return startRows;
      
    } catch (SQLException sx) {
      throw new ChainManagementException(sx);
    }
  }
  
  
  
  
  
  
  
  
  /**
   * Creates a new random seed for the specified chain with the
   * given starting row no.
   * 
   * @param chainId     the chain's ID
   * @param startRow    the first row no. the new seed becomes effective at
   * 
   * @throws IllegalArgumentException
   *         if it's the first seed and {@code startRow} is not 1;
   *         if {@code startRow} is less than or equal the last existing
   *         epoch seed's start row
   * @throws ChainManagementException
   *         if an internal error occurs
   *    
   */
  public synchronized void newEpochSeed(ChainId chainId, long startRow)
      throws IllegalArgumentException, ChainManagementException {

    checkWrite();
    
    if (startRow < 1L)
      throw new IllegalArgumentException(
          "startRow must be positive: " + startRow);
    
    try (var closer = new TaskStack()) {
      
      epochsSansSeed.setInt(1, chainId.no());
      ResultSet rs = executeQuery(epochsSansSeed, closer);
      
      var seeds = toEpochs(rs, chainId);
      
      closer.pop();
      
      final long lastStartRow;
      
      if (seeds.isEmpty()) {
        if (startRow != 1L)
          throw new IllegalArgumentException(
              "startRow (%d) must be set to 1 for first seed in chain %s"
              .formatted(startRow, chainId));
        lastStartRow = 0L;
      } else {
        
        seeds = seeds.stream().sorted().toList();
        checkIntegrity(Lists.map(seeds, Epoch::startRow), chainId);
        lastStartRow = seeds.getLast().startRow();
        if (startRow <= lastStartRow)
          throw new IllegalArgumentException(
              "startRow (%d) must be greater than last (%d) for chain %s"
              .formatted(startRow, lastStartRow, chainId));
      }
      
      
      byte[] newSeed = new byte[32];    // 32, since it's Base64_32 encoded
      new SecureRandom().nextBytes(newSeed);
      
      insertStmt.setInt(1, chainId.no());
      insertStmt.setString(2, Base64_32.encode(newSeed));
      insertStmt.setLong(3, lastStartRow);
      {
        int count = insertStmt.executeUpdate();
        assert count == 1;
      }
      
      epochSeedsByChainId.setInt(1, chainId.no());
      rs = executeQuery(epochSeedsByChainId, closer);
      
      var postSeeds = toEpochSeeds(rs, chainId).stream().sorted().toList();
      if (postSeeds.size() != seeds.size() + 1) {
        // TODO: clean up effects of this txn, if possible
        // resolution will be rule-based; earliest (by seed_id) wins
        throw new ConcurrentChainModificationException(
            "concurrent addition of new salt seeds detected for chain " +
            chainId);
      }
      
      var newEpochSeed = postSeeds.getLast();
      if (newEpochSeed.startRow() != startRow)
        throw new ChainManagementException(
            """
            internal error on adding new salt seed for chain %s: \
            expected last startRow %d after insert; actual was %d"""
            .formatted(chainId, startRow, newEpochSeed.startRow()));
      
      if (!Arrays.equals(newSeed, newEpochSeed.seed()))
        throw new ChainManagementException(
            """
            internal error on adding new salt seed for chain %s: \
            expected seed was not read back after insert (saltId %s)"""
            .formatted(chainId, newEpochSeed.saltId()));
      
      
    } catch (SQLException sx) {
      throw new ChainManagementException(sx);
    }
    
  }
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  /**
   * @param startRows   sorted (!)
   */
  private void checkIntegrity(List<Long> startRows, ChainId chainId) {
    final int count = startRows.size();
    if (count == 0)
      return;
    
    long rowNo = startRows.get(0);
    if (rowNo != 1L)
      throw new ChainManagementException(
        "internal error: start-rows for chain (%s) do not begin with at 1: %s"
        .formatted(chainId, startRows));
    for (int index = 1; index < count; ++index) {
      long next = startRows.get(index);
      if (next == rowNo)
        throw new ChainManagementException(
            "internal error: chain (%s) has duplicate start rows (%d): %s"
            .formatted(chainId, rowNo, startRows));
      rowNo = next;
    }
  }
  
  
  
  private List<EpochSeed> toEpochSeeds(ResultSet rs, ChainId chainId)
      throws SQLException {
    
    if (!rs.next())
      return List.of();
    List<EpochSeed> seeds = new ArrayList<>();
    try {
      do {
        
        int saltId = rs.getInt(1);
        String seed = rs.getString(2);
        long startRow = rs.getLong(3);
        seeds.add(new EpochSeed(new SaltId(saltId, chainId), startRow, seed));
        
      } while (rs.next());
      
      return seeds;
    
    } catch (IllegalArgumentException iax) {
      throw new ChainManagementException(
          "internal error on constructing EpochSeed for saltId:chainId (%d:%s): %s"
          .formatted(rs.getInt(1), chainId, iax), iax);
    }
  }
  
  
  
  private List<Epoch> toEpochs(ResultSet rs, ChainId chainId)
      throws SQLException {
    
    if (!rs.next())
      return List.of();
    List<Epoch> seeds = new ArrayList<>();
    try {
      do {
        
        int saltId = rs.getInt(1);
        long startRow = rs.getLong(2);
        seeds.add(new Epoch(new SaltId(saltId, chainId), startRow));
        
      } while (rs.next());
      
      return seeds;
    
    } catch (IllegalArgumentException iax) {
      throw new ChainManagementException(
          "internal error on constructing EpochSeed for saltId:chainId (%d:%s): %s"
          .formatted(rs.getInt(1), chainId, iax), iax);
    }
  }
  

}






















