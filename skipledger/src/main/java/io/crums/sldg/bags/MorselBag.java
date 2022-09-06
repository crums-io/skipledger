/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.bags;

import io.crums.sldg.packs.MorselPackBuilder;

/**
 * The other bags unified in one, along with "reasonable" business rules.
 * Here, <em>reasonable</em> means that the set of known full rows
 * ({@linkplain RowBag#getFullRowNumbers()} is a superset of rows with
 * information in the other bags.
 * 
 * <h2>TODO</h2>
 * <p>
 * Iterators for traversing trails and such (in lieu of the getXyzIndex methods:
 * see {@linkplain MorselPackBuilder} for use cases).
 * </p>
 */
public interface MorselBag extends RowBag, TrailBag, SourceBag, PathBag {

}
