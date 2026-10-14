# Technical debts

Known gaps between the architecture baseline and the implementation that
are accepted for now. Each entry states what is missing, what the system
does instead today, and what closing it requires.

## Offline merge window and device leases

**Missing.** The 90-day merge window enforced through device leases and
causal acknowledgement watermarks. Compaction does not consult the pending
local bases of other devices.

**Today.** Documents keep full snapshots plus their update logs, and no
history is dropped by compaction, so a device returning after any absence
can still merge. Nothing tells a long-absent device that its branch is
past a promised window, and storage grows without the bound the window
would allow.

**Closing it.** A lease per device recorded by the server on each pull;
acknowledgement watermarks per document; compaction that never advances
past the oldest live lease's base; an expired-device path that preserves
the local branch, downloads a fresh base, and offers comparison and
explicit reapplication.

## Server-side schema migrations

**Missing.** Incompatible migrations: an immutable migration plan with the
expected schema epoch, affected definitions, dry-run results and recovery
state; client-side migration code under an Owner/Admin grant; staged
results exposed at a declared commit boundary.

**Today.** Publications and pins exist, and a pin advances the schema
epoch atomically. Only compatible upgrades are possible: a definition
change that alters meaning has no guided path.

**Closing it.** A migration plan Object, a MigrationService that runs the
plan's code on a granted device, server validation of the submitted data
and the epoch change as one coordinated Change.

## Moving Objects between server Spaces

**Missing.** Move of Objects between two server-hosted Spaces as an
administrative command that keeps their ids and establishes new location
and CRDT generations.

**Today.** Moves work between local Spaces. Between server Spaces the
core refuses the move; export and import as copies with fresh ids work.

**Closing it.** A server command that checks the caller manages both
Spaces, writes the generation change and the source tombstone in one
coordinated Change, and delivers both sides to every authorized device.

## Semantic model selection

**Missing.** The evaluation the baseline requires before a model is
fixed: Russian/English retrieval tests and resource measurements on
physical Android devices.

**Today.** One multilingual embedding model is pinned by digest and used
on every platform.

**Closing it.** A retrieval test set in both languages, measurements of
memory, latency and battery on baseline devices, and a model choice per
platform recorded with the numbers.
