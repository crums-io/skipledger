/*
 * Copyright 2025 Babak Farhang
 */
/**
 * Microchain configuration and hosting on a relational database.
 * 
 * <h2>Configuration</h2>
 * <p>
 * Excepting the information needed to create the database connection
 * (jdbc URL, credentials, etc.), <em>all</em> ledger definitions and
 * configuration information are managed in relational DB. Usually,
 * this will live in the same database as where the (SQL) ledgers live,
 * but it is not a requirement.
 * </p>
 * <h2>Hosting</h2>
 * <p>
 * Each microchain's (skipledger) commitment chain can optionally be
 * stored in a relational database.
 * </p>
 */
package io.crums.sldg.mc.mgmt;