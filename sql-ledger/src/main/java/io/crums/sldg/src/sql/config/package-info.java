/*
 * Copyright 2025 Babak Farhang
 */
/**
 * Configuration information and JSON.
 * 
 * <h2>Design Objectives</h2>
 * <p>
 * The goal is to keep things simple but flexible. "Simple" means configuration
 * info is file-based; "flexible" means it doesn't have to be; the information
 * can come from a database, for example, without much friction.
 * </p>
 * <h3>Secrets</h3>
 * <p>
 * The configuration info involves the following secrets:
 * </p>
 * <ol>
 * <li>Database access credentials (username and password)</li>
 * <li>Ledger salt seed.</li>
 * </ol>
 * <p>
 * The latter is especially sensitive, since it should <em>never</em> be exposed,
 * can never be changed, and if lost, new skipledger proofs of contents cannot
 * be generated. It may make sense to retrieve this info ultimately from a keystore
 * (not implemented).
 * </p><p>
 * The UNIX way to handle sensitive data is to restrict access simply using
 * file permissions. We take the same approach here: the sensitive info
 * can be modularized into individual files.
 * </p>
 * <h3>Modularization And Reuse</h3>
 * <p>
 * A set of ledger definitions may use the same database connection. While
 * each ledger's configuration file may be self-contained, one may modularize
 * the ledger configurations so that database connection info is read from a
 * common file.
 * </p>
 */
package io.crums.sldg.src.sql.config;