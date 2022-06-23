/*
 * Copyright 2021-2022 Babak Farhang
 */
/**
 * Predicate model for defining a selection of source rows.
 * 
 * <p>
 * The motivation here is to be able compose <code>WHERE</code>-clauses
 * taking a fixed number of parameters declaratively in <em>JSON</em> (!).
 * It's functional aspects are not actually targeted to doing functional style
 * programming.
 * </p><p>
 * In order to model <code>WHERE</code>-clauses in JSON, we must first model
 * those <code>WHERE</code>-clauses in code. Thus this package.
 * </p>
 */
package io.crums.sldg.reports.pdf.model.pred;