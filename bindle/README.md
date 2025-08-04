Bindle
======

A data structure and a byte format for packaging ledger entries and their
skipledger proofs in a unified way.

### Overview

A *bindle* is a bundle of evidence about one or more ledgers. Per ledger,
evidence is packaged in a data structure called a *nugget*. Each ledger
nugget contains a set of "mutually agreeing" skipledger commitment paths
called a *multi-path". Depending on the type of ledger, a nugget may also
reveal the contents of specific ledger rows.

A nugget *may* also cross-reference other nuggets in the same bindle. There are 2
general categories of such cross-nugget references:

1. *Notarized Rows.* A notarized row is a (hash) reference to a *timechain* nugget,
establishing that the input-hash at a specific row no. in the timechain ledger
(the so-called *time block*) is derived from a row-hash in this nugget's ledger.

2. *Foregin References*. A foreign reference is a (hash) reference establishing
that a cell value in one of this nugget's rows is either equal to a cell value in
another nugget's row, or if a "beacon" reference, equal to the *row-hash* of
another nugget's row.


### Ledger IDs



### Format

Here's a preliminary sketch. The focus here
is on enumerating the *kind* of parts we need.



    HEADER        // fixed size (magic + version)
    IDS           // list of LEDGER_IDs, always parsed
    
    PARTITION     // partition: each part is a nugget; associated w/ LEDGER_ID
                  // provides random access to nuggets
    
    // per part (nugget)
    
    NUGGET := ID_NO MULTI_PATH SOURCE_PACK NOTARY_PACK REF_PACK ASSETS
    
    ASSETS        // a named-partition, with certain prefixes in the namespace
                  // reserved for other library modules
    

    


### Dependencies (Software)

Since evidence from many types of ledger is to be packaged in bindles, this module
will likely be integrated with other ledger-type-specific modules. The challenge is to
minimize this module's dependencies.

Presently, the strategy to decouple the bindle module's dependencies from other modules that
use bindle is thru the use of a *named partition* `ASSETS` section in the nugget.









