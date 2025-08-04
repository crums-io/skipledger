/*
 * Copyright 2025 Babak Farhang
 */
/**
 * Timechain / Crumtrail related packaging. Crumtrails are used to
 * <em>notarize</em> ledger commitment hashes (date witnessed).
 * 
 * <h2>Basic Organization</h2>
 * <p>
 * A crumtrail is composed of 2 parts. One part asserts the input-hash
 * (<em>cargo hash</em> in Timechain parlance) is derived from the hash
 * witnessed. The hash witnessed in our use case, is typically the commitment hash
 * of a ledgered row. The second part asserts <em>where</em> (at what
 * block no. and with what block-hash) that cargo hash is recorded in
 * the timechain. This 2nd proof is just a skip ledger {@linkplain io.crums.sldg.Path Path}
 * containing the row (time-block) in which the cargo hash occurs in,
 * and optionally other linked time-blocks (skip ledger rows) from the timechain.
 * </p><p>
 * Bindles package crumtrails in decomposed form. A crumtrail's first part,
 * its "cargo hash proof", is packaged alongside the ledger (that is,
 * inside the bindle's ledger-specific container). The crumtrail's second part,
 * its block proof (the skip ledger path), is treated as any other ledger.
 * </p>
 */
package io.crums.sldg.bindle.tc;