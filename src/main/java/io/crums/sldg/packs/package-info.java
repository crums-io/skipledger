/*
 * Copyright 2021 Babak Farhang
 */
/**
 * Implementation classes for various bags. The Xxx<em>Pack</em> moniker denotes an implementation
 * class for an Xxx<em>Bag</em> interface.
 * 
 * <h1>Immutable Types</h1>
 * 
 * <p>
 * The following types are immutable and are instantiated using a {@linkplain java.nio.ByteBuffer}
 * argument.
 * </p>
 * <ol>
 * <li>{@linkplain io.crums.sldg.packs.RowPack RowPack}</li>
 * <li>{@linkplain io.crums.sldg.packs.PathPack PathPack}</li>
 * <li>{@linkplain io.crums.sldg.packs.TrailPack TrailPack}</li>
 * <li>{@linkplain io.crums.sldg.packs.EntryPack EntryPack}</li>
 * </ol>
 * <p>
 * Finally, {@linkplain io.crums.sldg.packs.MorselPack MorselPack} is just a composition of
 * the above with added business rules.
 * </p>
 * 
 * <h1>Mutable Builder Types</h2>
 * <p>
 * Each of the above immutable types comes with its own mutable builder types. For example,
 * {@linkplain io.crums.sldg.packs.RowPackBuilder RowPackBuilder}.
 * </p>
 * 
 * <h1>Serialization</h2>
 * <p>
 * Presently it's only the mutable Xxx<em>Builder</em> types that know how to write what the
 * Xxx<em>Pack</em>s read. (The serialization interface on the <em>write</em>-path here
 * is expressed thru the super-simple {@linkplain io.crums.io.Serial Serial} abstraction.)
 * If a use case presents itself, it would be simple to add write-capability for every pack.
 * </p>
 */
package io.crums.sldg.packs;