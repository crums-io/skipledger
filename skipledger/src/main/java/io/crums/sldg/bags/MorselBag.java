/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.bags;


/**
 * The other bags unified in one, along with "reasonable" business rules.
 * Here, <em>reasonable</em> means that the set of known full rows
 * ({@linkplain RowBag#getFullRowNumbers()} is a superset of rows with
 * information in the other bags.
 * 
 */
public interface MorselBag extends RowBag, TrailBag, SourceBag, PathBag {

}
