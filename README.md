# skipledger
Skip ledger base implementation

## Overview

A skip ledger is a tamper-proof, append-only list of SHA-256 hashes that are
 added by its user[s]. Since hashes are opaque, a skip ledger itself conveys
 little information. If paired with the actual object whose hash matches the
 entry (e) in a given row however, then it is evidence the object belongs in the
 ledger at the advertised row number. Ledgers also support maintaining the times
 they are modified by calling out the SHA-256 hash of their latest row to the
 crums.io hash/witness time service. (See https://crums.io/docs/rest.html )

## Respresentations

Beside thru sharing it in its entirety, the state of a skip ledger can optionally be
 advertised compactly. The most compact of all is to just advertise the hash of
 the last row in the ledger. A more detailed, but still compact representation
 is achieved by enumerating a list of rows whose SHA-256 hashpointers connect
 the last (latest) row to the first. The number of rows in this list grows by
 the log of the size of the ledger, so it's always compact. This list of rows is called
 a ledger *path* and when is serialized as a standalone file, defaults to bearing the `.spath` extension, or `.spath.json` if in JSON format.
 
 This same structure is also used to provide a compact, standalone proof that an
 item at a specific row number is indeed inside the ledger. I.e. a list of rows
 that connect the latest row to the row of interest. If the row (or any row
 after it) has been witnessed, then the crumtrail witness evidence together with
 these rows can be packaged as a standalone *nugget* file.  The file default extension
for this type of object is `.nug`, or `.nug.json` if in JSON format.
 
### Note on JSON v. Binary
 
 The JSON file formats are considerably more bloated than their binary formats. This is
 not just because of the inherent overhead of JSON: in an effort to make its model clearer,
 the JSON contains a good deal of redundant information which the binary format does away
 with.
 
 
## Crumtrails and Beacons: Row Age Evidence
 
  A row's *minimum* age is established by storing a crumtrail of the row's hash.
 (The row's hash is not exactly the user-input hash: it's the hash of that plus
 the row's hash pointers.) A crumtrail is a tamper-proof hash structure that
 leads to a unitary root hash that is published every minute or so by crums.io
 and is also maintained at multiple 3rd party sites: it is evidence of
 witnessing a hash in a small window of time. Since the hash of every row in a
 skip ledger is dependent on the hash of every row before it, witnessing a given
 row number also means effectively witnessing all its predecessors.
 
 The witnessing algorithm then only witnesses [the hashes of] monotonically
 increasing row numbers. The default behavior when there are unwitnessed rows is
 to always witness the next few subsequent rows that can't be matched with an
 already stored crumtrail as well as the last row. This is because crumtrails
 are not generated right away: they're typically generated a few minutes after
 the service first witnesses a hash. By default, about 9 unwitnessed rows are
 submitted: 8 *evenly* spaced at row numbers that are multiples of a power of 2,
 and the last unwitnessed row. The exponent of this power of 2 for witnessing is
 called *tooth-exponent*.
 
 In order to establish the *maximum* age of entries in a ledger a beacon hash
 entry may be added. This hash is just the root of the latest Merkle tree
 published at crums.io every minute or so. Since it's value cannot be predicted
 in advance--and it comes with a UTC timestamp, it can be used to establish
 "freshness". The recommended practice is to simplify the evolution of row
 numbers by either (i) add only a single beacon as the very first row, or (ii)
 add beacons at row numbers that are always a multiple of some constant that is
 a power of 2.
 
## Command Line Tools
 
 There are 2 command line tools in this alpha release. One deals with managing a
 skip ledger owned by the user; the other inspects, compares, and manipulates (coming soon)
 the tamper-proof outputs of ledgers (nugget and path objects).
 
 The tools' launch scripts are distributed under `bin/` folder (directory); the actual code is
 under a sibling `lib/` folder. The bin folder can be renamed to anything so long as its relative position to the lib folder is maintained.
 
 Both tools have a *-help* option explaining its commands. (You might wanna pipe
 it with ` | less` since the output is a bit verbose.)
 
### sldg
 
 Command line interface to a skip ledger database owned and maintained by the user.
 
#### Examples
 
 In the examples that follow, we're assuming the `bin` directory above is included in the
 user's `PATH` env variable; if not, replace with actual path to launch script.
 
 To create a new ledger `abc.sldg` in the current directory run
 
 `$ sldg dir=abc.sldg mode=c`
 
 To make the first row in the ledger a beacon (thus establishing the *maximum* age
 of the ledger) run
 
  `$ sldg dir=abc.sldg addb`
 
 To add an entry (example hex input) run
 
`$ sldg dir=abc.sldg add ff06e7158445a3507dbf2b29140843bdece789d3d9d119871c193e4479e3d82c`
 
To read the status of the ledger run

  `$ sldg dir=abc.sldg status`
  
  To add a bunch of hex entries from a file
  
  `$ sldg dir=abc.sldg ingest path/to/entries.hex`
  
 Omit the filepath above if the hex entries are to come from *stdin*.
 
 To establish the *minimum* age of the ledger's rows run
 
  `$ sldg dir=abc.sldg wit`
 
 Since crumtrails take a few minutes to generate, the above command must be repeated a few minutes hence. One doesn't have to wait for this step to complete, however. More entries may be added in the meantime, however, before the second invocation of `wit`.
 
 The following outputs a compact representation of the ledger as a *state path* file to a
  an existing `docs` directory:
 
 `$ sldg dir=abc.sldg state file=docs/`
 
 Be careful if `docs` isn't an existing directory, then the above command tries to write the object
 in a file named *docs.spath*. (The auto file extension mechanism is overrideable by passing in the
 `ext=false` option.
 
 If the `file=docs` argument is not provided then the object is printed to the console in JSON.
 
 Once a crumtrail for a given row number in a ledger has been recorded in the ledger database,
 all entries (rows) up to that row number can be packaged as individual nuggets in similar fashion
 
 `$ sldg dir=abc.sldg nug 511 file=docs/`
 
 This creates a nugget file for the entry in row 511.
 
 If you lose track of 
 
 
### nug
 
 Prototype tool for inspecting, verifying and manipulating (next release) standalone ledger objects
 (the outputs of the `sldg` tool) without accessing the actual ledger.
 
#### Examples
 
 The following *describes* what's in a ledger object:
 
 `$ nug desc path/to/nug/or/spath/object`
 
 The following *compares* 2 ledgers objects:
 
 `nug comp path/to/ledger/obj1 path/to/ledger/obj2`
 
 This can be used to establish if a nugget and a (state) path come from the same or different
 ledgers, for example.
 
 