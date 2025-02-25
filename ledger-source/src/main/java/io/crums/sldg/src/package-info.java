/*
 * Copyright 2021-2025 Babak Farhang
 */
/**
 * Models the representation of source rows in a ledger on the
 * <em>client</em> side. We need this modeling in order to compute
 * a ledger row's hash. In skip ledger terminology, this is all about
 * how each row's <em>input-hash</em> is calculated.
 * <p>
 * The main task here concerns packaging this source information compactly,
 * but also, in a way that insulates most of the code from different
 * layout tradeoffs.
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
 */
package io.crums.sldg.src;