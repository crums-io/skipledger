/*
 * Copyright 2025 Babak Farhang
 */
/**
 * Morsels are portable packages of proofs about entries from one or more ledgers.
 * 
 * <h2>Basic Organization</h2>
 * <p>
 * Entries (rows) from the same ledger are packaged in a structure
 * called a {@linkplain io.crums.sldg.morsel.Nugget}: a morsel will typically
 * contain multiple nuggets.
 * </p>
 * 
 * <h3>Nuggets</h3>
 * <p>
 * Information from a single ledger is packaged as a nugget. A nugget
 * contains multiple parts:
 * </p>
 * <ol>
 * <li><em>Commitment hashes</em>: intersecting skipledger paths (see
 * {@linkplain io.crums.sldg.morsel.MultiPath MultiPath}). All other data
 * in the nugget depends on commitment hashes, thus this part is required.
 * If the ledger is in fact a timechain, then these paths are the chain's
 * blockproofs.
 * </li>
 * <li><em>Entry data</em>: ledger source rows encoded as {@linkplain
 * io.crums.sldg.src.SourceRow SourceRow}s (see {@linkplain
 * io.crums.sldg.src.SourcePack SourcePack}).
 * <li><em>Notary data</em>: entry witness dates using crumtrails. This
 * only records the so-called cargo-proof: the crumtrail's block-proof is
 * recorded in another nugget (see first item).</li>
 * <li><em>Cross-references</em>: entries in one ledger may provably reference entries
 * in another ledger (see {@linkplain io.crums.sldg.morsel.ForeignRefs}).
 * </li>
 * <li><em>Other assets</em>: some prototyped include..
 *   <ul>
 *   <li>Meta information such as column names</li>
 *   <li>PDF Report generator: DSL for generating receipts or statements.</li>
 *   <li>Other cryptographic assets such as CBFs (revocation) or signatures
 *   (tho the general aim is to minimize the use of public/private key infra).</li>
 *   </ul>
 * </li>
 * </ol>
 * 
 */
package io.crums.sldg.morsel;

























