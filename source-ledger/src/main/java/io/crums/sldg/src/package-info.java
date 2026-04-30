/*
 * Copyright 2021-2025 Babak Farhang
 */
/**
 * Models the representation of source-rows in a ledger. This serves
 * 2 purposes:
 * <ol>
 * <li>A uniform way to compute the hash of an ordered sequences of
 * values (cells / columns) in any ledger's rows. This serves as the
 * skipledger <em>input</em>-hash.</li>
 * <li>A compact representation of source-row values for 3rd party distribution
 * with redactable parts: a 3rd party can redact individual cell values for
 * downstream distribution without mutating the source-row's (verifiable) hash.
 * </li>
 * </ol>
 */
package io.crums.sldg.src;