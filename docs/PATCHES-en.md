# Nine fixes for repacking UE 4.26 IoStore containers

This fork of [retoc](https://github.com/trumank/retoc) makes it possible not
just to read an IoStore container but to rebuild one the engine will accept.

Tested against a UE 4.26.2 title: 17095 packages, 19779 chunks,
`container_header_version` = `Initial`, TOC version `DirectoryIndex`. The
rebuilt container boots and runs.

---

## 1. Package name hash for `Initial` headers

`zen_asset_conversion.rs`, `gen_hash`

The hash was skipped for the `Initial` header version, so cross-package
imports could not be resolved. The visible symptom was a crash in
`SoundNodeWavePlayer` on the first sound played. The hash is now always
computed.

## 2. Dependency check used the wrong list

`asset_conversion.rs`, `super_index`

The check walked `create_before_create` where it should have walked
`serialize_before_serialize`. The two lists differ, and using the wrong one
produces an incorrect dependency graph.

## 3. `pack-raw` did not write the Container Header

`retoc_cli/src/main.rs`

The critical one: without it the game rejects the rebuilt container outright.
`container_header_version` and `package_store_entries` were added to the raw
manifest, `unpack-raw` now preserves them, and `pack-raw` calls
`write_package_chunk`.

## 4. References to `/Engine/UnknownPackage`

`zen_asset_conversion.rs`

This placeholder appears whenever `to-legacy` cannot resolve an import.
References to it now resolve to `Null` instead of being chased further.

## 5. External arc block sorting

`zen.rs`

The cooker sorts external arc blocks by `FPackageId`. It does **not** sort
`imported_packages` in `StoreEntry` — conflating the two produces a graph the
loader disagrees with.

## 6. `export_bundles_size` was never computed

`zen.rs`

The field was left unfilled. It is `header_size` plus the sum of
`cooked_serial_size` across exports.

## 7. Bundle layout is computed, not lifted from the original container

`zen_asset_conversion.rs`, `compute_bundle_layout`

The cooker splits a package's `export_load_order` (the Create/Serialize
command sequence, which retoc already builds correctly) into bundles. The
rule: cut right after a `Serialize` command when the next command in order is
a `Create` of an export with a *smaller* index. That inversion means the
cooker deferred creating something earlier in the file until something later
in the file finished serializing first — a real cross-branch dependency, not
just traversal order.

A wider version of this rule (cutting on `Serialize`-with-smaller-index too,
not just `Create`) catches more of the real boundaries but also fires on
packages that only ever have one bundle in the original — e.g. a class
default object whose `Serialize` is deferred to the end of the package but
isn't recognized as a CDO by `class_index`. That false split only shows up
once you check *all* packages, not just the ones already known to have
several bundles, which is why the first version of this rule passed its
initial test set and still had to be narrowed. The `Create`-only rule
produces zero false splits across the whole 17095-package project it was
verified against, at the cost of some missed real boundaries.

Result: 1134 of 1243 known multi-bundle packages get an exact bundle count
and layout with no input beyond what retoc already computes; 0 single-bundle
packages are incorrectly split. The 109 remaining packages fall into two
shapes that were tried and abandoned as general rules (see `TASK.md`):
single-export native types (`USoundClass`/`USoundSubmix`-like) that still
split `Create`/`Serialize` into two bundles for reasons that look unrelated
to export order, and Blueprint packages where the real boundary is a
container export closing its bundle right after its children finish
serializing, without an index inversion to key off.

`RETOC_BUNDLE_LAYOUT` is kept as an optional override sitting on top of the
computed rule, for exactly those remaining cases:

```
RETOC_BUNDLE_LAYOUT=path\to\bundle_layout.json
```

When the variable isn't set, conversion runs entirely on the computed rule.
When it is set and the JSON has an entry for a package, that entry wins over
the computed layout; packages missing from the JSON still fall back to the
computed rule. `serial_offset` and the node-to-bundle map are recalculated to
match either source.

## 8. Arc deduplication runs after fixup, not at arc-creation time

`zen_asset_conversion.rs`, `dedup_legacy_dependency_arcs`

The dedup key is still the pair `(from_export_bundle_index, to_export_bundle_index)`
within each imported package's block — that part was already right. The bug
was *when* it ran: at arc-creation time, `from_export_bundle_index` is not yet
a real bundle index. It's a disposable placeholder handed out by a global
counter, because the actual source bundle can only be resolved once every
package has been converted and serialized (`fixup_legacy_external_arcs` runs
in a second pass afterwards, since a dependency may point at a package that
hasn't been processed yet). Deduping placeholders is deduping numbers that
are unique by construction — it doesn't collapse anything, and can even
under-collapse: two different imports of the same package that later resolve
to the identical `(from, to)` pair get deduped correctly today, but only
because the dedup now runs after resolution, not before it.

The fix moves deduplication to run once per package, after
`fixup_legacy_external_arcs` has resolved every placeholder to its real
value, operating directly on the already-serialized graph region of
`package_buffer` (patching `graph_data_size` and shrinking the buffer via
splice, rather than re-serializing the package from its struct — which is
gone by that point anyway).

Verified on the full 17095-package project (`to-zen` without
`RETOC_BUNDLE_LAYOUT`, comparing the serialized graph region against the
original byte-for-byte): arc match rate goes from 0/17095 before this fix to
92.75% (15856/17095) after. All remaining mismatches trace back to the same
109 packages from fix 7 whose bundle count is still wrong — 109 mismatch
directly (a wrong bundle count structurally can't produce the right graph),
and the other 1130 mismatch only because they import one of those 109
(confirmed exhaustively: every one of the 1130 has at least one of the 109 in
its own import list). Deduplication runs strictly after bundle layout is
decided, so it can't fix or worsen those 109 — it's an independent, later
stage acting on whatever layout it's handed.

## 9. Compression in `pack-raw`

`iostore_writer.rs`

`write_chunk` hardcoded `compression_method_index = 0`, so containers came out
at roughly twice the size of the cooked original — 17.3 GB against 9.
`compression.rs` already provided everything needed; only the call and the
header registration were missing.

Method indexing follows the reader in `lib.rs`: 0 means uncompressed, other
values index `compression_methods` offset by one. A single registered method
therefore gets index 1.

A block is stored compressed only when that is actually smaller; blocks that
do not shrink are written as-is with method 0, which is what the cooker does.
The chunk hash is still computed over uncompressed data.

Gated behind an environment variable so default behaviour is unchanged:

```
RETOC_COMPRESSION=Zlib
```

Result on the same container: 9.62 GB, and a `pack-raw` → `unpack-raw` round
trip reproduces the store entries exactly.

---

## Known gaps

**`load_order` is not computed.** Every package gets 0. The values are carried
over from the unpacked original during chunk substitution. I could not work
out the cooker's rule; if anyone knows it, I would like to hear.

**`export_bundle_count`/graph arcs are still wrong for 109 packages** out of
17095, where the computed rule in fix 7 doesn't find the real bundle boundary
(single-export native types, and Blueprint packages whose boundary isn't
keyed on an index inversion — see fix 7). `RETOC_BUNDLE_LAYOUT` covers all of
them as a fallback when the original container's layout is available.

## Verifying a rebuild

Compare the unpacked rebuild against the unpacked original:

* `load_order` — always 0 on our side, carried over from the original only
  during real `pack-raw` chunk substitution, not by direct `to-zen` (known gap);
* `export_count` — expect zero differences;
* `export_bundle_count` and graph arcs — expect zero differences when
  `RETOC_BUNDLE_LAYOUT` covers the package, otherwise expect the 109 known
  exceptions from fix 7 (plus their import-cascade effect on graph arcs);
* `export_bundles_size` will differ on exactly the packages you modified if the
  replacement strings differ in length — that is expected;
* `imported_packages` may differ in ordering only (see fix 5).
