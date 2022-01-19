/*
 * Copyright 2021 Babak Farhang
 */
/**
 * SQL schema and adaptors for the storage layer of a ledger.
 * 
 * <h1>Immediate objectives</h2>
 * <p>
 * Will provide default tie-ins
 * from a source, append-only table to the ledger. Such tie-ins shall include
 * <ul>
 * <li>Default method to hash an SQL row.</li>
 * <li>Default method to package ledger <em>source</em>-rows into provable morsels.</li>
 * </ul>
 * </p>
 * <h2>Notes</h2>
 * <p>
 * Since the application now knows the source of its ledger's hashes and how it hashes them,
 * it can now emit morsels that are independently provable all the way down to the row's source.
 * </p><p>
 * Scope warning: keep it simple, so you can release early.
 * </p>
 */
package io.crums.sldg.sql;