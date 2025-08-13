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
 * <p>
 * T
 * </p>
 * <h2>About the hash of the HASH data type</h2>
 * <p>
 * Presently, this data type is hashed like any other byte sequence.
 * I'm on the fence whether it ought to be so: maybe its hash should
 * be itself. In that event, HASH types cannot take any salt. That's the
 * downside; the upside would be that redacted cells would be indistiguishable
 * from HASH values. Another upside, is that it introduces a data type
 * with this curious hashing property.
 * </p>
 * <h2>TODO</h2>
 */
package io.crums.sldg.src;