/*
 * Copyright 2021 Babak Farhang
 */
/**
 * The original content whose {@linkplain io.crums.sldg.Row#inputHash() input hash} makes it
 * into the ledger is abstracted away here. Presently there is little structure enforced on
 * how this is done (too early in the game).
 * 
 * <h2>Basic Data Structures</h2>
 * <ul>
 * <li>{@linkplain io.crums.sldg.entry.Entry Entry}. Encapsulates the raw entry (as a byte sequence) and its
 * {@linkplain io.crums.sldg.entry.Entry#hash() hash}ing operation.</li>
 * <li>{@linkplain io.crums.sldg.entry.EntryInfo EntryInfo}. Lightweight information about the entry. This
 * may include meta-information. The motivation is to craft something similar to a zip-entry in a zip
 * file.</li>
 * </ul>
 * <p>
 * Note {@linkplain io.crums.sldg.Ledger} and its subclasses know nothing about this package.
 * </p>
 */
package io.crums.sldg.entry;