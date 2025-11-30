/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.mc.fx;


import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

import io.crums.sldg.mc.mgmt.DbEnv;
import io.crums.sldg.mc.mgmt.MicrochainManager;
import io.crums.util.Lists;
import io.crums.util.TaskStack;

/**
 * Application state is centralized here.
 * <p>
 * Presently, application state is comprised of 4 principal parts.
 * </p>
 * <ol>
 * <li><em>Available Workspaces.</em> This is an enumeration of database
 * connection settings, and JDBC drivers.</li>
 * <li><em>Current Workspace.</em> The current database (aka workspace).</li>
 * <li><em>Microchain Manager.</em> Manages microchains defined on the
 * database.</li>
 * <li><em>Event Listener/Dispatcher.</em> This is a super-simple
 * design (since I have little idea what events are needed a priori):
 * when a component is finished mutating an instance of this class,
 * it fires a change event ({@linkplain #fireChange()}.
 * </ol>
 * <h2>TODO</h2>
 * <p>
 * Enumerated here..
 * </p>
 * <ul>
 * <li>Clean up on close, maybe use shutdown hook. </li>
 * <li>Remove the {@code Listener} interface:
 * mystifies the logic. Used only once, thankfully.</li>
 * </ul>
 * 
 * @see #addListener(Listener)
 * @see #removeListener(Listener)
 * @see #currentWorkspace()
 * @see #microManager()
 */
class AppState {
  
  
  /** App state change listener. */
  interface Listener {
    /**
     * Fired to indicate the state has changed. (It's the listener's
     * responsiblity to monitor the {@linkplain AppState} properties
     * that matter to it.)
     * <p>
     * <b>Warning:</b> State changes ({@linkplain AppState#fireChange()}) fired
     * from this method (or downstread methods called by this method) are
     * silently dropped.
     * </p>
     * 
     * @param state     the instance on which {@linkplain AppState#fireChange()}
     *                  was invoked
     */
    void changed(AppState state);
  }
  
  
  private final List<Listener> listeners = new ArrayList<>();
  
  private List<Workspace> workspaces;
  
  
  private Workspace currentWorkspace;
  private Optional<MicrochainManager> microManager;
  private TableGraph tableGraph;
  
  
  
  
  

  /**
   * Reads the list of saved workspaces.
   */
  public AppState() {
    
    this.workspaces = Workspace.listWorkspaces();
    this.microManager = Optional.empty();
  }
  
  
  
  /**
   * Lists the available workspaces.
   */
  public List<Workspace> listWorkspaces() {
    return workspaces;
  }
  
  
  /**
   * Adds the given {@code listener} idempotently (only once).
   * 
   * @see #removeListener(Listener)
   */
  public void addListener(Listener listener) {
    Objects.requireNonNull(listener);
    if (!listeners.contains(listener))
      listeners.add(listener);
  }
  
  
  /**
   * Removes the given {@code listener}.
   */
  public void removeListener(Listener listener) {
    listeners.remove(listener);
  }
  
  
  /**
   * Returns the current workspace, if any.
   * 
   * <p>
   * API note: this is not an API; if it were, then this would
   * return a list of strings. This class expects to manage workspaces
   * itself, so callers should not mutate the returned workspace instance
   * directly.
   * </p>
   */
  public Optional<Workspace> currentWorkspace() {
    return Optional.ofNullable(currentWorkspace);
  }
  
  
  /**
   * Returns the {@linkplain MicrochainManager} (defined by tables in the
   * the current workspace/database), if any.
   */
  public Optional<MicrochainManager> microManager() {
    return microManager;
  }
  
  public List<String> listTables() {
    if (currentWorkspace == null)
      return List.of();
    
    if (tableGraph == null) try {
      
      final String sysPrefix =
          currentWorkspace.sysTablePrefix()
          .orElse(DbEnv.DEFAULT.tablePrefix());
      
      
      tableGraph = new TableGraph(
          currentWorkspace.loadDataSource(),
          Predicate.not(sysPrefix::startsWith));
      
    } catch (SQLException sx) {
      throw new AppException(sx);
    }
    
    return tableGraph.tableNames();
    
  }
  
  
  
  
  
  
  public <T> QueryBus<T> newQueryBus(Function<ResultSet, T> factory)
      throws IllegalStateException {
    Workspace ws = currentWorkspace;
    if (ws == null)
      throw new IllegalStateException("no workspace is open");
    return new QueryBus<>(ws.loadDataSource(), factory);
  }
  
   
  /**
   * Opens and returns a pooled, read-only connection to the database.
   * 
   * @return <em>close</em>, in order to return to pool
   * 
   * @throws IllegalStateException  if no workspace is open
   */
  public Connection openConnection()
      throws SQLException, IllegalStateException {
    
    Workspace ws = currentWorkspace;
    if (ws == null)
      throw new IllegalStateException("no workspace is open");
    var con = ws.loadDataSource().getConnection();
    return con;
  }
  
  
  /**
   * Opens the workspace with the given {@code name}. If another workspace
   * is currently open, then it is first closed. Invoking this method with
   * the current workspace-name causes a reload.
   * 
   * @param name        the workspace's name
   * 
   * @throws NoSuchElementException if no workspace with that name exists
   * 
   * @see #currentWorkspace()
   */
  public void openWorkspace(String name) throws NoSuchElementException {
    Workspace workspace =
        listWorkspaces().stream().filter(w -> w.name().equals(name))
        .findFirst()
        .orElseThrow();
    openWorkspace(workspace);
  }
  
  
  
  
  
  /**
   * Saves and opens the given <em>new</em> workspace.
   * 
   * @param workspace   new workspace with unique name
   */
  public void newWorkspace(Workspace workspace) {
    checkName(workspace);
    workspace.save();
    this.workspaces = Workspace.listWorkspaces();
    openWorkspace(workspace);
  }
  
  
  
  private void openWorkspace(Workspace workspace) {
    closeCurrentWorkspace();
    this.microManager = MicrochainManager.load(
        DbEnv.DEFAULT, workspace.loadDataSource(), false);
    this.currentWorkspace = workspace;
  }
  
  
  
  
  private void closeCurrentWorkspace() {
    if (currentWorkspace == null)
      return;
    
    
    try (var closer = new TaskStack()) {
      closer.push(currentWorkspace);
      if (microManager.isPresent())
        closer.push(microManager.get());
    }
    
    microManager = Optional.empty();
    tableGraph = null;
    currentWorkspace = null;
  }



  private void checkName(Workspace workspace) {
    if (Lists.map(workspaces, Workspace::name).contains(workspace.name()))
      throw new IllegalArgumentException(
          "name '%s' is already defined for another workspace"
          .formatted(workspace.name()));
    
  }




  private boolean firing;
  
  
  
  /**
   * Fires {@linkplain Listener#changed(AppState) Listener.changed(this)} on
   * all registered (added) listeners. Invoking this method while its executing
   * has no effect (if accessed serially, as required).
   * 
   * @see #addListener(Listener)
   */
  void fireChange() {
    if (firing)
      return;
    firing = true;
    try {
      // copy the listeners so that adding/removing
      // a listener during callback does not break things
      for (var l : List.copyOf(listeners))
        l.changed(this);
    } finally {
      firing = false;
    }
  }
  
  
  
  

}













