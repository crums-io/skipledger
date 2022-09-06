/*
 * Copyright 2020-2021 Babak Farhang
 */
/**
 * This package deals with memo-ization and caching.
 * <p>
 * A {@linkplain io.crums.sldg.SkipLedger} is a data structure containing
 * redundant information: given the underlying source (a table, a list of lines,
 * whatever), the <em>n</em><sup>th</sup> {@linkplain io.crums.sldg.Row row} can be constructed from
 * scratch in <b>O</b>(<em>n</em>) operations. For this reason, every skip ledger
 * implementation backs the data in some memo-ized from or another. The most basic
 * form this takes is the 2-column model: input-hash, and row-hash. This second
 * column (row-hash) can be understood as a memo-ization of the <b>O</b>(<em>n</em>)
 * steps it takes to compute the hash of the <em>n</em><sup>th</sup> row.
 * </p>
 * <h2>Offline Hashing</h2>
 * <p>
 * About creating that <em>n</em>-th row from scrach (above).. {@linkplain
 * io.crums.sldg.cache.HashFrontier} allows you to do that starting from row [1].
 * </p>
 * <h2>Opportunities for Caching</h2>
 * <p>
 * {@linkplain io.crums.sldg.SkipLedger SkipLedger}s access rows with row
 * numbers that are multiples of higher powers of 2 more often than those
 * that are multiples of lower numbers. This fact, combined with the observation
 * our rows are <em>not</em> supposed to be ever updated (under normal operation),
 * presents opportunties.
 * </p>
 */
package io.crums.sldg.cache;