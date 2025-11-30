/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.mc.mgmt;


import static io.crums.sldg.mc.mgmt.McMgmtConstants.*;
import java.util.Objects;
import java.util.Optional;

import io.crums.sldg.mc.mgmt.ChainIdMgr.ChainId;


/**
 * A chain-entry in the system. Instances are immutable, but have
 * mutator methods that return modified instances. Instances fall in
 * one of two categories:
 * <ol>
 * <li>those created by the user.</li>
 * <li>those created and returned by the system ({@linkplain ChainIdMgr}).
 * </ol>
 * System-created instances have non-zero chain-IDs
 * ({@linkplain ChainIdMgr.ChainId}). <em>User</em>-created instances
 * don't have an ID and are used as chunky arguments to {@linkplain ChainIdMgr}
 * methods: they are referred to as "args"-instances.
 * 
 * @see ChainInfo#argsInstance(String, String, String)
 */
public final class ChainInfo {
  
  /**
   * Returns a new "args"-instance (with no set id).
   * 
   * @param name              not {@code null}, unique across the system
   * @param description       optional, {@code null} or blanks count as empty
   * @param uri               well-formed URI, if neither {@code null} nor blank
   * 
   * @return {@code new ChainInfo(name, description, uri)}
   */
  public final static ChainInfo argsInstance(
      String name, String description, String uri) {
    return new ChainInfo(name, description, uri);
  }
  
  private final ChainId id;
  private final String name;
  private final Optional<String> desc;
  private final Optional<String> uri;
  

  /**
   * Constructs a new "args"-instance (with no set id).
   * 
   * @param name              not {@code null}, unique across the system
   * @param description       optional, {@code null} or blank counts as empty
   * @param uri               well-formed URI, if neither {@code null} nor blank
   * 
   * @see ChainIdMgr.ChainId#isSet()
   */
  public ChainInfo(
      String name, Optional<String> description, Optional<String> uri) {
    this(ChainIdMgr.INIT_ID, name, description, uri);
  }
  
  
  private ChainInfo(String name, String description, String uri) {
    this(ChainIdMgr.INIT_ID, name, description, uri);
  }
  
  /**
   * Package-private, full constructor.
   * 
   * @param id                  not {@code null}
   * @param name                not {@code null}, unique across the system
   * @param description         {@code null} or blank counts as empty
   * @param uri                 well-formed URI, if present
   */
  ChainInfo(
      ChainId id,
      String name,
      Optional<String> description,
      Optional<String> uri) throws IllegalArgumentException {
    
    this.id = Objects.requireNonNull(id, "null id");
    this.name = name.trim();
    if (this.name.isEmpty())
      throw new IllegalArgumentException("empty name");;
    this.desc = normalize(description, false);
    this.uri = normalize(uri);
    checkUri(this.uri);
  }
  
  
  /**
   * Package-private, full constructor.
   * 
   * @param id                  not {@code null}
   * @param name                not {@code null}, unique across the system
   * @param description         {@code null} or blank counts as empty
   * @param uri                 well-formed URI, if not {@code null} nor blank
   */
  ChainInfo(
      ChainId id,
      String name,
      String description,
      String uri) throws IllegalArgumentException {
    
    this(id, name, Optional.ofNullable(description), Optional.ofNullable(uri));
  }
  
  
  public boolean isArgs() {
    return !id.isSet();
  }
  
  /**
   * Returns the microchain's system identifier. The other chain properties
   * (name, description, uri) may be modified throughout the chain's lifetime
   * on the system, but this identifier remains immutable.
   * 
   * @see ChainId#isSet()
   */
  public ChainId id() {
    return id;
  }
  
  /**
   * Returns the microchain's name. Unique across the system.
   */
  public String name() {
    return name;
  }
  
  
  /**
   * Returns an instance with the given new name.
   * 
   * @param newName   not blank
   * 
   * @return {@code this} instance if unchanged, an "args"-instance, otherwise
   * @see #isArgs()
   * @see #argsInstance(String, String, String)
   */
  public ChainInfo name(String newName) {
    if (newName.trim().equals(name))
      return this;
    return new ChainInfo(newName, desc, uri);
  }
  
  /**
   * Returns the optional short description (not more than a few kilobytes).
   */
  public Optional<String> description() {
    return desc;
  }
  
  /**
   * Returns an instance with the given new description.
   * 
   * @param newDescription   {@code null} or blank counts as empty
   * 
   * @return {@code this} instance if unchanged, an "args"-instance, otherwise
   * @see #isArgs()
   * @see #argsInstance(String, String, String)
   */
  public ChainInfo description(Optional<String> newDescription) {
    return
        normalize(newDescription, false).equals(desc) ?
            this :
              new ChainInfo(name, newDescription, uri);
  }
  
  /**
   * Returns the microchain's URI. Well-formed, if present.
   */
  public Optional<String> uri() {
    return uri;
  }
  
  
  /**
   * Returns an instance with the given new URI.
   * 
   * @param newUri {@code null} or blank counts as empty
   * 
   * @return {@code this} instance if unchanged, an "args"-instance, otherwise
   * @see #isArgs()
   * @see #argsInstance(String, String, String)
   */
  public ChainInfo uri(Optional<String> newUri) {
    return
        normalize(newUri).equals(uri) ?
            this :
              new ChainInfo(name, desc, newUri);
  }
  
  /**
   * Instances are equal, if they are member-wise equal.
   * 
   * @see #equalsIgnoringId(ChainInfo)
   */
  @Override
  public boolean equals(Object o) {
    return o == this ||
        o instanceof ChainInfo other &&
        other.id.equals(id) &&
        equalsIgnoringId(other);
  }
  
  /** @return {@linkplain #id()}.{@linkplain ChainId#no() no()} */
  @Override
  public int hashCode() {
    return id.no();
  }
  
  
  /**
   * Tests if this instance is member-wise equal to the {@code other},
   * ignoring {@linkplain #id() id}s.
   * 
   * @param other   not {@code null}
   */
  public boolean equalsIgnoringId(ChainInfo other) {
    return
        other.name.equals(name) &&
        other.desc.equals(desc) &&
        other.uri.equals(uri);
  }
  
  

  @Override
  public String toString() {
    return
        "[id: " + (id.isSet() ? id : "<args>") +
        ", name: " + name +
        ", uri: " + uri.orElse("") +
        ", desc: " + (desc.isPresent() ? "*]" : "]");
  }
  

}















