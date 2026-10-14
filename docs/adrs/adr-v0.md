# Product Vision

## A Universal Environment for Knowledge, Actions, and Interfaces

**One graph. Your own models. Any view.**

People build their own systems for life and work. The platform provides the data foundation, collaboration, offline access, and controlled automation.

- **Version:** 0.2 — product and architecture baseline
- **Date:** October 14, 2026
- **Basis:** Product requirements, architecture decisions, and library documentation
- **Purpose:** A shared foundation for requirements and subsequent planning

This document fixes the product vision, core technology stack, physical data model, and consistency and security rules. Section 19 defines the architecture; section 18 separates remaining choices from required implementation checks. Wire schemas and API details are defined during development within these rules.

## Contents

1. [Vision and Value](#01-vision-and-value)
2. [Product Principles and Boundaries](#02-product-principles-and-boundaries)
3. [Universal Data Model](#03-universal-data-model)
4. [Layers, Ownership, and Contexts](#04-layers-ownership-and-contexts)
5. [Spaces and Collaboration](#05-spaces-and-collaboration)
6. [Interface and Navigation](#06-interface-and-navigation)
7. [Interfaces Are Data](#07-interfaces-are-data)
8. [Documents and Living Content](#08-documents-and-living-content)
9. [Associative Memory and Search](#09-associative-memory-and-search)
10. [Agents and Automation](#10-agents-and-automation)
11. [Extensions](#11-extensions)
12. [Offline Operation and Synchronization](#12-offline-operation-and-synchronization)
13. [Conflicts and Preservation of Changes](#13-conflicts-and-preservation-of-changes)
14. [Access, Trust, and Attribution](#14-access-trust-and-attribution)
15. [History, Deletion, and Recovery](#15-history-deletion-and-recovery)
16. [Product Scope and v0 Boundaries](#16-product-scope-and-v0-boundaries)
17. [Vision Acceptance Criteria](#17-vision-acceptance-criteria)
18. [Remaining Decisions and Development Readiness](#18-remaining-decisions-and-development-readiness)
19. [Technology Stack and Physical Model](#19-technology-stack-and-physical-model)
20. [Reference Documentation](#20-reference-documentation)
21. [Appendix: Consolidated Decisions](#appendix-consolidated-decisions)

> **Status:** Architecture baseline for development. The decisions define required behavior; library integration and recovery guarantees still require the prototypes and checks in section 18.

---

## 01. Vision and Value

*A platform for a personally defined system of life and work, not a collection of isolated trackers.*

The product is a universal **local-first environment** in which people build their own systems of knowledge, actions, and interfaces. Within a connected data space, they can organize learning, work, projects, nutrition, workouts, plans, documents, and any other domains they may introduce later.

Users do not have to fit their lives into a rigid model of tasks, pages, or calendar events. They define the entities, fields, relationships, rules, and visual representations they need. AI helps build and evolve this system but has no separate authority over its data.

### The problem we solve

The product aims to close the gap between how people connect events, knowledge, and intentions in their minds and how conventional software forces them to distribute those things across separate apps. The same information should be reusable without re-entering it: a work item can appear in a personal plan, an exercise mistake can lead to a topic for review, and a note can become evidence for a decision.

A unified graph does **not** mean an undifferentiated stream of everything. Users select contexts such as *Work*, *Personal*, *Learning*, *My Week*, or their own categories. Data is connected where useful and is not mixed in the interface without an explicit rule.

### Who it is for

The primary user is someone who wants to accumulate a personal system over years, adapt its structure, and retain connections across areas of life. The next audience is partners and small teams working in shared Spaces. Enterprise-scale needs are considered as room for growth, not a reason to complicate every daily interaction.

### What creates value

The system is valuable when a person can quickly capture a thought, find it from an incomplete recollection, relate it to a current goal, visualize it in a useful way, and delegate an authorized part of the work to an agent. A new use case is added through configuration, interfaces, scripts, or extensions—not necessarily through a new release of the core.

> **Priority:** A universal platform and working environment. AI is a fully participating assistant within that environment. No single domain becomes a mandatory product model.

---

## 02. Product Principles and Boundaries

*Freedom to model anything must coexist with clear rules and a usable default experience.*

### Graph-first; the user defines the domain

There is no mandatory hierarchy, primary parent, or built-in “task” class. Trees, kanban boards, calendars, and timelines are ways to work with the same graph. Object types and capabilities are defined by users, imported from other Spaces, or provided by extensions.

### Local work does not depend on server availability

After downloading a Space, users can read and change its data without a connection. The server governs access and the accepted shared state. Synchronization must not turn every local interaction into a wait for a network response.

### The core is independent of appearance

Every interface, agent, script, and module uses shared capabilities and rules. Changing or breaking a custom interface cannot take away access to data and platform administration. A minimal built-in Internal Shell is always available.

### Universality without mandatory programming

Interfaces are data, so a visual builder, AI, a script in any language, and a native extension all shape them through the same API. Users can begin with a supplied basic interface that supports object creation and editing, search, navigation, and settings. That interface does not restrict future customization.

### Autonomy is bounded by permissions

AI and automation may operate independently only within authorized limits. Attribution remains visible; sensitive actions and broader access require consent. Publishing code into a shared Space does not authorize executing it on another participant's device.

### Work survives synchronization failures

Server rejection, conflicts, incomplete indexing, and interrupted operations must be visible states. The system preserves work and offers a recovery path instead of silently deleting it, moving it, or presenting it as published.

### What the product does not promise

It is not a digital reproduction of human memory, proof that an AI conclusion is true, or a means of forcibly controlling other people's devices. Background work is not guaranteed when the assigned device is powered off. Freedom to model anything does not imply unlimited computing resources or a lack of technical constraints.

> This document defines product meaning and behavior. The core stack and physical model are fixed in section 19; technical contracts (message formats, APIs, schemas) are defined in development.

---

## 03. Universal Data Model

*Every level of detail remains a first-class part of a single graph.*

An **Entity** is an independently identifiable object with a stable identity. It may represent a goal, a food item, a word, a workout set, a document, a rule, a custom interface, or anything else meaningful. Entities may be created without a user-defined type.

### No division into “real” and “too small” objects

A user can decompose a goal to whatever depth is useful. A step, example, error, or individual measurement retains the ability to have fields, types, layers, and external relations. Its logical capabilities do not depend on whether it is stored physically on its own or alongside other objects.

### Types are compositions of user-defined definitions

An Entity and each of its Layers can have multiple types. A type brings together field definitions, constraints, and semantic capabilities. Users can assemble and combine such sets without changing the core. Field names are human-facing labels; identical names do not make two fields identical. Fields can represent text, numbers and measurements, time, references, collections, content, and computed values.

New structures can be created from templates, a builder, or an AI proposal. If a user enters an unknown field, the application offers to create its definition or map it to an existing field. The original input remains intact even if the user declines to structure it.

### Relations are Entities, too

A relationship has its own identity, owning Space, fields, types, and Layers. It can connect multiple participants with different roles, be commented on, and participate in other relationships. Deleting a relationship does not, by itself, delete its participants.

The graph has no global prohibition against cycles, duplicate relationships, or multiple participation. User-defined relation schemas may impose constraints. A tree emerges from selected relationships and presentation rules—not from mandatory parenthood for every object.

### Updating schemas must not silently change data meaning

Published definitions are versioned. A Space uses a selected version; upgrades and migrations require consent. An unknown definition must not cause values to be lost. Capabilities such as temporal or measurable describe the meaning of fields to views rather than imposing mandatory business types.

A field definition's identity fixes its storage kind and interpretation of stored values. Renaming a field does not change its identity. Changing its CRDT kind or incompatibly changing its meaning requires a new field definition and an explicit migration. Published versions are immutable artifacts with exact dependency versions; their availability does not depend on retaining a CRDT checkout point.

An Admin or Owner applies a schema pin or migration against an expected schema epoch. Editors may create and publish definitions, but publication does not change the Space's active pins. Compatible writes from older devices remain admissible; incompatible writes are preserved for repair instead of silently reinterpreted.

> **Example:** “Pass the English exam” connects to topics, lessons, attempts, and mistakes. One mistake can relate simultaneously to an exam attempt, a grammar rule, and a future review session. It needs no single primary parent.

---

## 04. Layers, Ownership, and Contexts

*One object can be used in multiple ways without competing originals.*

Each Entity belongs to exactly one Space. Its identity does not depend on its position on screen, a tree path, or physical storage layout. An explicit copy creates a new independent Entity; transferring ownership preserves the identity of the original.

Moving an object between Spaces is an online administrative command: an Admin or Owner of the source authorizes the transfer, the destination grants creation rights, and source transfer policies must allow it. The command preserves Object ID, advances its location epoch and CRDT generation, and transfers its current logical state. Source history remains under the source Space's access rules; transferring history requires a separate explicit policy and consent. Required definitions and attachments transfer only when policy permits. Related objects and personal Layers do not move automatically. Writes against an old location or generation are preserved for repair.

### Flat, independent Layers

A Layer enriches a particular Entity with its own values, types, and interpretations. It belongs to a Space and does not inherit from another Layer. One Entity may have any number of independent Layers, including Layers in different accessible Spaces.

Fields with identical display names in different Layers do not automatically conflict. A View explicitly chooses which values to show and edit. By default there is no hidden “top Layer wins” policy and no merging by field name.

### Collaboration without forced copies

A partner with edit access can change a shared Entity directly or propose an improvement in a separate Layer. A Layer is not required for every collaborative edit; it is used when an independent interpretation needs to be preserved.

Selected Layer improvements can be accepted into canonical data or another explicitly selected Layer. Acceptance is a new action that retains the source, the proposer, and the person or agent who accepted it. The originating Layer is not automatically removed.

### Personal data layered over team data

A team Entity can appear in a personal plan without being copied. Personal values are stored in the personal Space when they must remain hidden from the team. Layers within the same shared Space are accessible to participants according to their roles; calling a Layer “personal” does not grant privacy by itself.

Dragging or editing must not silently change the write destination. If a kanban is grouped by team status, the application must not quietly substitute a personal status. Ambiguous operations ask the user to select a destination or remain unavailable until configured.

### A Context is a selection, not a new store

A Context specifies the Spaces, Layers, relations, filters, and Views relevant to the current activity. One Entity can belong to several Contexts. Visibility within a Context, availability of a local copy, and permission to access it are distinct. Contexts neither transfer ownership nor grant extra permissions.

---

## 05. Spaces and Collaboration

*One shared Space means one shared dataset, not one copy per participant.*

The current product scope assumes **one authoritative server** with multiple Spaces and users. A client can combine accessible Spaces in its local queries and views. Federation among independent servers is outside v0.

A Space owns its Entities, Layers, schemas, documents, rules, Views, and saved interface settings. Participants work with the same objects and receive changes through synchronization. Each Space has exactly **one Owner**.

| Role | Permissions in v0 |
|---|---|
| **Owner** | All rights; manage Admins; transfer ownership; change critical settings; delete the Space. |
| **Admin** | Manage Editors and Viewers, settings, and administrative migrations; cannot delete the Space, replace the Owner, or appoint Admins. |
| **Editor** | Create and change shared content: objects, Layers, schemas, Views, scripts, and agent configurations; cannot manage access. |
| **Viewer** | Read accessible content, search, and open Views; cannot write to the shared Space. |

Editors may publish new definition and interface versions. Admins and Owners apply schema pins and migrations. The Owner sets mandatory transfer restrictions and controls which policy changes an Admin may make. Server administration is separate from a role within one Space.

Memberships, object ownership and generations, schema epochs, and protected system metadata are authoritative control-plane records. Their graph representations do not grant Editors the right to alter them through generic object edits. Local-only Spaces enforce equivalent rules in the local core.

### Access boundaries

In v0, server-side permissions apply to the entire Space, not individual fields or Layers. Finer-grained policies may be developed later. A relation pointing to an object in another Space does not grant access to that object; the interface must represent inaccessible references appropriately.

### Getting started

By default, a user can start with a local Space without mandatory registration. Connecting to the server and enabling shared synchronization are separate actions. The server verifies access to server-hosted Spaces; local work does not grant additional server privileges.

> Shared interface configurations already belong to the Space. Two people do not need separate Spaces or published personal copies to use the same kanban or document together.

---

## 06. Interface and Navigation

*The Internal Shell ensures control; a Space defines the everyday experience.*

### Internal Shell

A minimal built-in shell handles application startup, connections, Space selection, permissions, and recovery from a broken custom interface. User code cannot remove this access path. The shell does not have to occupy prominent screen space during normal use.

### Space-defined interface

Inside the shell, a Space defines menus, headers, panels, navigation, editors, and work screens. **Workspace, Canvas, and Chat** are equal interaction modes, not three separate data stores or editing systems.

Users can combine tables, cards, timelines, graphs, documents, and custom visualizations. Changing a visualization does not copy its objects. Every View is subject to the same rules for data and access.

### Navigation without a primary tree

Contexts, saved Views, search, and favorites provide entry points. Relations enable navigation between objects, and navigation history allows users to return to where they began. Tabs and multiple simultaneous Views, including desktop split-screen, are supported.

### What gets synchronized

| Shared, persistent state | Local interaction state |
|---|---|
| Interface definitions, saved layouts, Views, filters, themes, and component settings. | Open tabs, active window, temporary menus, cursor, current scroll position, and navigation history. |
| Saved positions of objects on user-defined Canvases and their explicit settings. | Current camera zoom and pan unless explicitly saved as configuration. |

### Desktop and Android

Both platforms provide the same general data and View capabilities, while layout and interactions adapt to the device. Functional parity does not require pixel-identical screens or automatic mirroring.

### Supplied interface and accessibility

The supplied interface provides Space and Context selection, quick capture, search, lists and tables, a universal object inspector, definition editing, relation participants, document editing, and an explicit Layer/write-destination selector. Synchronization, grants, rejections, backup, and recovery remain available through the Internal Shell. These functions do not introduce a mandatory task or calendar model; the wider catalog in section 07 remains part of v0.

The built-in catalog and Internal Shell target WCAG 2.2 AA. Essential operations support keyboard access, visible focus, screen-reader names and roles, scalable text, and alternatives to dragging. Android touch targets are at least 48 dp. Canvas interfaces provide an accessible list or inspector alternative. User-defined layouts may introduce usability problems, but cannot remove the accessible recovery path.

> The current vision contains no separate “synchronized working session” Entity. What syncs is how a Space is configured, not everything currently open on a device.

---

## 07. Interfaces Are Data

*An interface is an object of the Space; whoever can write that object can build the interface.*

An **Interface** is an object: a tree of nodes with a type from the built-in catalog, properties, bindings, local state, and handlers. The host application ships a fixed, trusted **renderer** that draws this tree and reports user events; it never executes user code. Because an interface is data, building one means writing data through the ordinary core API. There is no separate UI SDK or UI language.

| Who shapes an interface | How |
|---|---|
| Visual builder | writes the tree |
| AI | generates the tree against its schema; the tree is validated before it is written |
| A script in any language | a client of the API: subscribes to interface events, writes the tree and its state |
| A native extension | the same API in-process |
| CLI and external tools | the same API |

### Bindings, actions, and handlers

- **Bindings:** Luau expressions over explicitly permitted graph inputs and local interface state for filtering, sorting, grouping, and formatting. They run in a read-only environment with time, memory, and host-operation limits, without external calls, secrets, writes, or arbitrary extension execution. The Luau analyzer checks them before execution; runtime limits remain authoritative.
- **Actions from the built-in catalog:** create, update, delete, navigate, set state, notify, HTTP call, AI call. Core authorization checks the acting user, interface version, and applicable grants and transfer policies. Ordinary user-initiated edits require the user's data permissions; sensitive actions use grants or confirmation in the trusted Internal Shell.
- **Handlers:** Luau functions such as `on_click` are executable code. A device grant authorizes a specific published code version and its executable dependency closure. Without a grant the tree remains viewable, but its handlers are disabled. Handlers that only use benign capabilities (section 14) are granted automatically under the device policy, so an interface that merely edits its own Space never prompts; handlers that call out, use AI, or write elsewhere ask once per version. The SDK surface does not grant unrestricted access.
- **Local state:** variable declarations belong to the interface definition; their live values remain on the device and are not synchronized (section 06).
- **Components:** reusable subtrees with parameters; objects in their own right, shared and reused across interfaces and Spaces.
- **Catalog breadth:** tables, cards, kanban, calendar, timeline, graph, forms, charts, documents, embedded Views, and a canvas scene graph (shapes, paths, text, transforms, hit testing) so that unconventional visualizations need no renderer change.

The interaction loop is local: a user event goes to the core; an authorized catalog action, handler, or subscribed script runs; the tree or state changes; the renderer updates. The renderer accepts no user JavaScript, arbitrary HTML, or executable links. Images and embeds use core-managed asset and network access so that layout cannot bypass transfer policies.

### Shared lifecycle

| Stage | Expected behavior |
|---|---|
| **Draft** | An interface object in the Space, collaboratively edited like any document. Multiple alternative drafts are allowed. |
| **Preview** | Runs the draft against a fork of the local replica with external calls disabled by default, so preview never produces real side effects. |
| **Publish** | Creates a fixed version of the object. Editor, Admin, and Owner can publish within their agreed permissions. |
| **Switch** | Which published version a device shows is the device's choice. A new version does not rebuild the screen mid-task or grant execution permission. Its declared schema and API requirements must be satisfied; an incompatible version opens read-only with recovery and version selection in the Internal Shell. |
| **Restore** | Prior versions and the built-in Internal Shell remain available. Rolling back an interface is not rolling back data. |

### Independence of data

An interface can perform authorized changes but cannot create its own permission bypass or independent data model. Its failure must not destroy saved work or block platform administration. Safe management remains in the Internal Shell, outside replaceable interfaces. The renderer is replaceable: a terminal renderer or a native one draws the same tree.

> Two participants can use different compatible published interface versions over the same data. Displaying layout and evaluating bounded read-only bindings require no separate execution consent. Luau handlers, scripts, extensions, and sensitive actions follow the grant rules in section 14.

---

## 08. Documents and Living Content

*Markdown's writing convenience meets addressable blocks and interactive Views.*

A document should feel like a comfortable place to write, not a database form. Users write and format text, and add structured blocks, code, images, references, or live Views when needed.

### Plain text and richer capabilities

The platform supports both text-oriented and block-oriented documents. Markdown provides convenient input and a readable text representation. An extended document may contain elements that require references or static substitutes when exported to ordinary Markdown.

Lossless round-tripping of every interactive document to standard Markdown and back is **not** promised. The product must distinguish the original living document, its text export, and simplified viewing.

### Addressable parts of documents

Users can reference a section, block, or selected fragment; attach comments; relate it to Entities; and ask AI to change only part of a document. Editing and rearrangement should preserve addressability where possible; a broken reference must not masquerade as a valid one.

Every addressable block is an Entity Object with its own UUID and Loro document, including text, properties, types, and fields. The containing document's movable tree stores placements and references to those blocks. Block identity differs from placement identity; removing a placement does not automatically delete its block. These documents are records in the Space database, not separate paragraph files.

Fragment references use text cursors, the captured version, and a quoted excerpt with surrounding context. Editing can preserve a position without preserving its original meaning. Deleted, detached, or ambiguous references are explicit states. Splitting and merging blocks preserve a selected original identity and record redirects where applicable; uncertain anchor recovery requires confirmation. Quoted excerpts are copies subject to transfer policies.

### Embedded Views

A document can embed a fully interactive View—for example, a study calendar, dependency graph, or form for recording results. The View references original objects rather than making hidden copies. Changes target only the Spaces and Layers that are explicitly selected and permitted.

### Comments are not exceptions to the model

A comment, reply, annotation on a relation, or discussion result can have user-defined fields, types, Layers, and relationships. Threaded discussions can be rendered from the graph without a mandatory global comment tree.

### Collaboration and visibility

Participants edit shared content with attribution. An embedded View cannot increase the viewer's access rights. Moving or publishing a document in another Space must not automatically reveal private content of linked objects or embedded results.

---
## 09. Associative Memory and Search

*Find something by meaning, time, or surrounding context, even after forgetting its exact name.*

A user should be able to ask, “I remember we discussed an idea about graphs when I was planning my studies,” and find connected original sources rather than mere keyword matches. Associative search is a required product capability, not a replacement for graph structure with model guesses.

### Different ways of remembering work together

Search combines text, custom fields, semantic similarity, graph connections, dates, Contexts, and other available signals. Results explain why an item matched, identify its source, and provide a path to the original content.

Searching local memories must not require sending all personal information to an external AI provider. If an external provider is used for an additional response, separate data-transfer permissions apply. The original data and ordinary application features remain available without a generative model.

### Transparent result completeness

Indexes are built in the background and aim to cover all downloaded Spaces. While indexing remains incomplete, the application indicates that results may be partial. It notifies users when indexing finishes, and they choose whether to repeat a query. A ready index does not guarantee that the server has not changed since the last synchronization.

Basic object projections are updated with each durable write. Expensive extraction, embeddings, and materialized results use a durable indexing queue. A result is installed only if the object's revision and the model and processing versions still match. Queries report per-Space data and index watermarks, pending work, and missing attachments. Ordinary forms and lists do not wait for semantic indexing.

Semantic retrieval has a local model path on both desktop and Android. The model and inference runtime are selected through Russian/English retrieval tests and physical-device resource measurements. Model and tokenizer versions are pinned; vectors from different versions are not mixed. Android indexes in resumable foreground batches. Sending data to an external provider requires separate authorization and does not replace the local retrieval requirement.

### Memory can evolve automatically

AI may discover and record new hypotheses and relationships in dedicated hypothesis Layers. They belong to the shared graph but are visually distinct from human-authored records and confirmed assessments. A hypothesis identifies its agent author, sources, and status.

Agents may revise hypotheses under policies applying to the agent, type, and Layer. Human approval must not erase the fact that AI originally created an item. A model's assessment must not be represented as objective proof. Rejected hypotheses are deleted according to a configured retention period.

### Field meanings are mapped explicitly

Different Spaces may represent priority, status, or duration differently. AI can suggest semantic mappings, which users must approve. A mapping helps unified search and Views, but neither merges the source fields nor grants automatic permission to write results back.

> The system helps reconstruct **recorded** context. It must not invent memories, missing sources, or confident answers based on incomplete information.

---

## 10. Agents and Automation

*Not separate hard-coded product features, but configurable scenarios built on common capabilities.*

Daily planning, vocabulary revision, workout analysis, and discovering associations are examples of user-created scenarios. None requires a mandatory domain-specific subsystem. Users express behavior through rules, formulas, scripts, and agent configurations; rules and agent configurations are objects of the Space, so other devices and participants can read and edit them.

### Configurable autonomy

Each agent has its own objectives, Contexts, authorized actions, and external services. It may analyze data, create objects, propose schemas, generate interfaces, and design automations. Sensitive actions and exceeding granted authority require approval; agents cannot grant themselves more permissions.

All changes pass through common core rules. Generating code, permitting its execution, and approving a particular sensitive action are distinct events. AI attribution does not disappear when a user accepts its proposal.

### One model for execution

Manual, event-driven, and scheduled execution are supported, including cron-like schedules. Conditions, actions, notifications, spending limits, and error handling are configurable. Notification policies can differ between agents and rules.

Each background automation has a designated executing device. Other devices can edit definitions and inspect results. Missed-run behavior is configurable per automation: skip, execute the latest, catch up on a bounded number, or ask the user.

### Durable workflows

Complex actions can resume after interruption while preserving progress. Automatic resumption is suggested, but the user sets the recovery policy for each workflow. The system must avoid uncontrolled repetition of externally consequential actions or endless chains triggered by its own events.

A run pins its code version and stores serializable state and stable step identities. Each step durably records its intent before an effect and its result afterward. Resumption reuses completed results and advances from a recorded checkpoint; it does not restore an arbitrary coroutine stack. Coroutines handle asynchronous work within the running process.

External actions use stable idempotency keys and receipts where supported. A crash after sending a request but before recording its result produces an `unknown` outcome. Automatic repetition requires safe deduplication or reconciliation; otherwise the workflow pauses for verification. Compensation is a separate action, not rollback of an already completed external effect. Event deduplication, causal-depth and frequency limits, and explicit policies for self-triggered writes bound automation loops.

### Scripting environment

A script is a **client of the core API** with its own identity and a grant of capabilities: data, AI, notifications, external integrations, interface events and state. User scripts never access internal storage directly. The same contract serves all three execution tiers of section 11; the permissions requirement is independent of the tier.

Scripts written inside the application are **Luau**: source in a Space object, run directly without a build step, with no ambient I/O and bounded VM memory, execution time, and host operations. Luau interrupts do not provide an exact instruction-fuel guarantee through mlua; host calls have independent timeout, cancellation, and resource limits. The Luau analyzer supplies diagnostics and type checking, while the core enforces authorization and runtime limits.

The SDK is a typed Luau module generated from the same protobuf contracts as the core API. Its descriptor-based generator and the separate Luau analyzer bridge are product implementation work, not assumed features of mlua. Scripts, the renderer, and the CLI use the same contract and authorization dispatcher.

Execution requires configurable limits on resources, frequency, concurrency, and costs. Third-party code runs only with consent given on the executing device.

> The server does **not** run user agents, scripts, cron jobs, or modules. If the assigned device is unavailable, the server does not automatically take over. External AI does not become available offline merely because local data is available.
>
> In v0 only a desktop device can be a background executor. On Android the core runs only while the application is open.

---

## 11. Extensions

*Extend workflows, interfaces, and operating-system integrations without bloating the domain core.*

### Sharing definitions between Spaces

Definitions, Views, interfaces, components, rules, scripts, and agent configurations are objects, so they move between Spaces by export and import of objects. Imported objects remain editable; a Layer over an imported object keeps a local customization separate from the original. Versioned packages distributed through Git are deferred beyond v0 and will be built on top of this import.

The default import creates an explicit copy: new Object IDs and fresh Loro documents built from logical state. A dependency manifest and a two-pass ID mapping rewrite internal references, field and type identities, and component dependencies. External references remain explicit and may be unavailable; export does not silently include private linked content. Provenance is retained, but execution grants are not copied. Reusing existing definition identities is an explicit import option; restoring a backup is a separate mode that preserves original identities and history.

### Three execution tiers

| Tier | Form | Written in | Isolation | Where |
|---|---|---|---|---|
| **Luau script** | source in a Space object, runs without a build | Luau | runtime sandbox with limits | every platform |
| **WASM extension** | compiled module (wasmtime) installed on the device | any language that compiles to WebAssembly | strong sandbox: memory, fuel, host functions only | every platform |
| **Native extension** | dynamically loaded library with a stable C ABI, installed on the device | any language | none; at the user's own risk | desktop only in v0 |

All three use the same core API and the same grant model (section 14); each registers the functions and events it provides as capabilities that scripts and rules can use. A WASM extension is the way to ship compiled logic safely to every platform, including Android; a native extension is for what needs the operating system or hardware directly—for example, monitoring the user's own desktop app activity.

Installation of external native extensions on Android comes later. Each extension explicitly declares supported platforms, capabilities, and lifecycle modes.

### Users control the lifecycle

An extension can run on demand, while the application is open, or in the background if it supports that mode. OS startup is requested by the extension and requires user approval. Closing the UI window does not stop authorized background work on desktop.

User rules decide what an extension's events should record in the graph and how the results should be used. OS integrations must not impose a built-in domain model for a tracker or calendar.

> An extension marketplace, reputation system, and developer trust verification are not required for v0. Users install native code at their own risk. Native code is not promised the same isolation as a restricted script; it can act outside the application's controlled API.

---

## 12. Offline Operation and Synchronization

*A full local replica of each selected Space, with the server confirming the shared state.*

### Local core and authoritative server

Each device runs a local core that the UI and other clients, including a CLI, connect to through the same protocol the server speaks. It provides data access, search, edits, and local execution. On desktop the application stays resident after its window is closed, so the core remains available. On Android the core lives only while the application is open.

The server handles users, access, storage, receiving changes, and distributing accepted changes. It checks protocol compatibility and system integrity without executing user-supplied programs, AI, or arbitrary scripts. Shared data is synchronized using CRDT principles.

The server applies Loro updates to candidate documents and validates operations and resulting state before acceptance. It runs built-in deterministic validators for system invariants and configured strict declarative constraints; it does not execute user-supplied validators. Section 19 defines atomic commit and delivery.

### Full replica, not an invisible partial cache

Each Space selected for synchronization is downloaded fully, including structured data and the state needed for reconciliation. Entities, Layers, and relations are not evicted by automatic cache cleanup. A full local copy does **not** mean loading the entire graph into RAM.

Large attachments are downloaded on demand and may be pinned for offline use. The cache is automatically managed within a configurable limit; pinned data, sole existing copies, and unsynchronized changes are never discarded as expendable cache.

### Initial download

Before initial replication completes, users may view correctly received data, but the Space remains read-only. Progress and incompleteness are visible. Interrupted downloads resume; editing is enabled after successful completion. Indexing may continue independently afterward.

Initial download uses a consistent snapshot boundary followed by accepted changes after that boundary. It includes pinned publications and the audit and control-plane data needed for permitted local operations. Historical archives may be retrieved separately; current-state completeness, archive availability, attachment availability, and index readiness are reported independently.

### Everyday operation

An edit is first durably saved locally and immediately shown with a synchronization indicator. Losing connectivity after the initial successful download does not return the Space to read-only mode. Upon reconnection, the server checks current permissions and accepts allowed changes.

Two write layers exist on a device. An **own edit** to one document (typing, a field change) is a CRDT operation appended durably to that document's update log and visible to its author immediately; it does not pass through the commit coordinator. A **Change** is the unit of submission and of cross-document atomicity: it is formed from accumulated own edits on idle, on explicit save, and always before a multi-document operation such as splitting or merging blocks or moving an object. The coordinator and its visibility gate apply to installing Changes, local multi-document ones and remote ones, never to single-document own edits.

The default supported offline merge window is **90 days**, maintained through device leases and causal acknowledgement watermarks. This is separate from deleted-object retention. Pending local bases must be considered before advancing a compaction boundary. An expired device preserves its local branch, downloads a fresh base, and offers comparison and explicit reapplication; unconditional merge after arbitrary absence is not promised. Revoked access prevents subsequent server reads and writes, while physical erasure from an offline device is not guaranteed.

The system distinguishes locally saved changes, queued submissions, waiting dependencies, server acceptance, and rejection. An unknown delivery outcome must not be shown as either success or failure.

> Data completeness, index readiness, and synchronization freshness are separate statuses. Older clients block operations that are unsafe for formats they do not understand; migrations require consent and recovery after interruption.

---

## 13. Conflicts and Preservation of Changes

*Failures must be visible and recoverable; a server rejection must not destroy work.*

### A change is accepted as a whole

One logical change may affect multiple Entities and multiple Spaces on the same server. It is either accepted in full or not accepted at all. Missing access to one part must not publish the remainder. Repeated submissions must not produce duplicates.

The local core and server use a durable commit coordinator across Space files. Readers see the state before a Change or after all its applicable parts have been installed. A receiving client installs the whole authorized envelope for its selected Spaces; it never waits for parts it cannot access and never receives hidden Space metadata. Section 19 defines this boundary and crash recovery.

Atomicity covers data in the system. It does not promise to undo an external AI call, notification, or native extension action that has already happened. Such effects follow the relevant durable workflow's policies.

### Concurrent edits and user-defined constraints

CRDT reconciles valid edits from replicas. By default, user-defined constraints are diagnostic checks on the reconciled state: a contradiction remains visible without blocking synchronization. Admins and Owners may activate strict policies from the built-in declarative validator set. Strict uniqueness is scoped to one Space and a versioned constraint, with explicit comparison and tombstone rules. The server validates candidate state and commits constraint indexes in the same coordinated acceptance. Local-only Spaces use equivalent local validation.

Offline strict-constraint checks are provisional. Two offline objects can claim the same unique value; the server accepts one Change and rejects the other in full. A user repairs the rejected work through a new Change. Arbitrary user scripts cannot become strict server validators.

Users or authorized agents repair such problems with new actions. Not every concurrent edit is a conflict, and not every conflict is a server rejection. The UI distinguishes violated constraints, denied access, and incompatible versions.

### The server rejected local work

Rejected edits are separated into a preserved local version. The main graph displays server-accepted state and other changes still awaiting acceptance. Users must be notified, and unresolved rejections must remain accessible beyond a transient toast message.

Each rejection identifies its reason, affected data, and initiator. The application offers permissible options: compare versions, repair changes, retry if access is restored, retain the local work, or explicitly discard it. Automatic transfer into a personal Space is forbidden.

### Dependent changes

If subsequent work depends on a rejected change, the dependent chain pauses and is preserved. Independent changes continue synchronizing. The application can suggest a recovery plan but applies it only after user consent.

Core records an application dependency graph in addition to Loro's per-document causal dependencies. It includes creation of referenced objects and definitions, assumptions based on unaccepted state, and explicit workflow prerequisites. A normal reference to an existing object does not require access to that object's Space or block unrelated work.

The first implementation conservatively pauses the transitive causal and semantic dependency closure. Sequential edits of different fields in one document may still be causally dependent. Rebuilding the live state excludes that closure and uses fresh editing peers where operations were excluded. Repair or retry after rejection creates new operations and a new Change linked to the original; transport retry of an unchanged submission retains its original ID and payload.

> Preserving a person's work and declaring that work published are two different obligations. The product must fulfill the first without falsely implying the second.

---

## 14. Access, Trust, and Attribution

*Permission to read, permission to transfer information, and permission to execute code are separate boundaries.*

### Three independent boundaries

Server-side roles determine access to a Space. Data-provenance policies regulate information transfers. Execution permissions constrain what a particular agent, script, UI, or device may do. A View's Context can narrow data selection but cannot broaden any of these permissions.

Every invocation identifies the acting user, device, actor and code version, execution ID, grant, and any parent invocation. Its effective permissions are the intersection of current user roles, actor/device grants, and transfer policies. A child invocation cannot receive broader permissions than its parent. Core and server derive authority from authenticated sessions and verified delegation; client-provided actor labels are not proof of authority.

### Data provenance

The application tracks which Spaces contributed to data and derived outputs. Copying, accepting a Layer, publishing, and transmitting to an external service are checked separately from normal reading. Permission to read a workplace Space must not silently turn into permission to send its contents to a partner or AI model.

For external AI, a user authorizes specific Spaces for a specific provider. Each agent also remains bound by its own permissions. Revoking permission prevents subsequent transfers but does not promise deletion of data already delivered to a recipient.

The Space Owner sets mandatory transfer rules and delegates specific policy administration to Admins. Rules cover copy, Layer acceptance, publication, export, and external sending, with allowed destinations and `allow`, `ask`, or `deny` outcomes. Ordinary confirmation cannot override a mandatory denial; an authorized exception or declassification is a separate audited action.

Core-controlled outputs retain source labels. Trusted actions label their actual inputs; sandboxed executions conservatively accumulate the sources they read and attach those labels to derived writes and external outputs. In v0 a label is a Space, not a field: an invocation that reads anything from a Space carries that Space's label for the rest of its run, and every write to another Space or external call is checked against the policies of all carried labels. A script's manifest declares the Spaces it reads and writes, so policy conflicts surface at grant time as well as at run time; a script that needs finer separation splits its work into separate invocations. Interface bindings only read and transfer nothing, so they carry no labels; catalog actions and handlers do. This enforcement applies to core-managed operations, including UI, scripts, WASM, and API export. Native code and external clients that already possess plaintext may copy it outside those operations; the product does not claim universal control over such transfers. Protected labels cannot be removed by an ordinary object patch, and client-supplied labels alone cannot establish trusted provenance.

### Executable code and sensitive actions

Third-party Luau scripts, WASM extensions, and native extensions require consent, recorded on the device as a grant for a specific version and set of capabilities. Merely storing code in a Space grants no execution permissions. Changed code or newly requested capabilities require a new grant. Assigning another person's device as an executor does not bypass local consent.

Displaying interface layout and bounded read-only bindings requires no separate execution grant. Luau handlers require a device grant just like other executable code. Sensitive catalog actions follow their grants and transfer checks.

Grants distinguish **capability classes** so that consent is asked only where a boundary changes. The *benign* class contains operations the acting user could already perform with their role in the current Space: reading it, writing to it, interface state, navigation, and notifications to themselves. Code whose capabilities are all benign receives its grant automatically under a device policy the user can switch to "always ask"; the grant is still recorded and revocable. Everything else, including external calls, AI calls, writes to other Spaces, and extension functions, requires explicit consent for that version. A new version from the same publisher with an unchanged capability set may inherit the previous grant if the user chose that option; a changed or widened set always asks again. A grant identifies the code and executable dependency hashes, capabilities, permitted Spaces and destinations, limits, expiry, and launch modes; it resides outside ordinary shared graph fields. Core checks revocation and authorization before each action. Even a Space Owner cannot automatically run code on participants' devices. Users separately accept the broader risks of native extensions.

### Mandatory attribution

Users can see who created or changed data: a human, agent, script, module, or system process. For AI, the application preserves the agent identity, available model identification, sources, and an intelligible basis for the action. Human approval is recorded separately and does not change who originally created the item.

The provenance journal guarantees product-level transparency but does not promise cryptographic proof of author identity in v0. Attribution must not be merely decorative text that every UI can freely hide or redefine.

Attribution uses a durable audit envelope tied to Change IDs and CRDT operation ranges. It is synchronized and backed up with the data; Loro's local-only `origin` is not its storage mechanism. Actor identities and editing peer IDs are distinct. Human approval is a separate attributed Change.

### Security in the first release

The sign-in and session mechanisms will be chosen during development. A network-facing release requires authentication, access checks, and a protected connection. Connection secrets are not ordinary shared graph fields. Full content encryption of Spaces and backups is not implemented in v0.

The same authorization dispatcher serves every protocol client. Network links use WSS. Local listeners bind only to loopback, authenticate sessions, verify allowed browser origins, limit messages and connections, and recheck permissions per request. Tokens are not placed in URLs or logs. WebView sessions do not carry unrestricted user authority. Protected metadata, code grants, secrets, and administrative commands cannot be edited through a generic data API.

---

## 15. History, Deletion, and Recovery

*Meaningful memory should last; storage maintenance must not silently destroy it.*

### What counts as history

The user wants a detailed timeline of activity with indefinite retention of meaningful history. Meals, workouts, lessons, and decisions may be ordinary graph Entities. Change history helps establish who changed data and how; technical logs support diagnostics.

These kinds of information serve different purposes but do not need rigidly separate domain models. CRDT history does not replace the recording of a real-world event: if an event was never recorded, the system must not pretend to know it happened.

Accepted durable Changes and recoverable states have indefinite retention until explicit purge. This guarantees saved actions, not every transient keystroke. Active CRDT documents may use shallow snapshots; immutable archive checkpoints, per-document updates, and audit retain historical reconstruction independently. Rejected and paused work remains preserved until explicit discard or an applicable explicit purge. Archive size grows with use and is visible to the user; compaction does not promise constant storage for indefinite recoverable history.

### Hide and Delete are distinct user actions

**Hide** removes an object from a selected Context or View without destroying the original. **Delete** targets the object itself in its owning Space, subject to permissions. Deleting a relation or a placement does not automatically delete related Entities.

Recovery retention is configurable per Space; **90 days** is the initial default before purge. This setting is independent of the offline merge window. Purge takes precedence over historical recovery: it removes the object's managed payload and indexes and leaves a minimal authoritative ID/generation record that prevents stale-device resurrection.

Archive payloads and checkpoints are separable per document, so erasing one object does not destroy the surviving history of a multi-object Change. Archive manifests remain verifiable after recording an erased part. Historical references to purged objects show that state. Cleanup releases the erased object's attachment references; bytes shared by surviving objects are not automatically removed. Derived quotes and copies in other objects have their own lifecycle. Purge does not promise immediate erasure from other people's offline devices, external copies, or old backups.

### Backups are separate from synchronization

Early versions create automatic **local** backups at a selected destination. Cloud backup destinations are deferred. Restorability must be verifiable and include source data, required definitions, configurations, and unsynchronized work—not merely derived indexes.

The application shows backup time, verification status, and completeness. If attachments were not downloaded and therefore were not included, the backup must not be presented as complete. A backup stored on the same physical device is not presented as protection against loss of that device.

Backup establishes a common committed boundary across Space files and the coordination database, drains committed recovery work, and includes audit, pinned publications, archive data or explicit archive omissions, outbox, and rejected work. Immutable attachments are held against garbage collection while copying. Checksums, manifest validation, and a staging restore verify readability. Restoring retains historical identities but creates fresh editing peers and installation sessions. Before publishing to an existing server, a restored client reconciles current access, generations, and the purge registry.

### Scale for years of accumulation

A long-term target is up to **one million logical Entities** in a large Space, dozens of connected Spaces, and up to **10 GB of attachments per Space**. This provides room for years of use, not a v0 performance guarantee or a requirement to load everything into memory.

> Historical recovery applies to durable accepted Changes until purge. It does not establish a promise to preserve all diagnostic logs, unsaved UI state, or every temporary operation.

---

## 16. Product Scope and v0 Boundaries

*The first release validates the coherence of the platform, not a single predefined life scenario.*

v0 is intended as the first usable version of the general platform. There is no requirement to choose one built-in product—nutrition tracking, task management, or English learning. Domains are created using the available mechanisms and can be evolved by the user.

| Included in the first release | Product purpose |
|---|---|
| **Desktop and Android** | Embedded local core (resident on desktop, foreground-only on Android), CLI, adaptive interfaces, offline operation, and one synchronization server. |
| **Model and working environment** | Graph, user-defined types, flat Layers, relations, Contexts, documents, search, and Views. |
| **Programmability** | Interfaces as data with a built-in catalog and renderer, bindings, scripts as API clients, rules, agents, within a permanent Internal Shell. |
| **Collaboration** | Shared Spaces, four roles, attribution, schema and UI versions, controlled publication. |
| **Reliability** | Atomic changes, visible rejections, recovery, local backups, and explicit offline state. |
| **Extensions** | Export and import of objects between Spaces; Luau scripts, WASM extensions on every platform, native extensions on desktop; designated client-side executors. |

### Explicitly deferred or excluded from v0

A browser client; multiple independent servers; server-side execution of user agents, cron jobs, and extensions; cloud backups; external Android native extensions; encryption of Spaces and backups; versioned packages distributed through Git and any package dependency management; and an extension reputation marketplace.

Dynamic tabs and working sessions are not synchronized. Git is not required for user data. Direct editing of exported files outside the application with automatic bidirectional re-import is also not a current requirement.

### What this document does not prescribe

Wire schemas, detailed SDK methods, editor implementation, release dates, and measured resource budgets are development work. The interpreter, physical object packing, consistency rules, and security boundaries are fixed here. Implementation choices must preserve these behaviors; changing them requires an explicit architecture revision.

> The size of the v0 scope does not mean everything must be built at once. Work can be sequenced, but reducing promised user capabilities must be an explicit revision to the vision—not an unannounced substitution of a narrow app for the intended platform.

---

## 17. Vision Acceptance Criteria

*Evaluate the outcome through user-visible behavior, independently of the implementation.*

These scenarios demonstrate that the intended platform has been built. They are not yet a technical test plan and do not mandate libraries or protocols.

| Test | Expected outcome |
|---|---|
| **New domain** | A user creates a learning or nutrition model using definitions and interfaces, without modifying the core. |
| **Arbitrary granularity** | One exercise mistake can be linked to a topic, lesson, and review plan without copying its state. |
| **Personal and shared** | Two participants edit one Space; a personal Layer for a team object can stay in a separate personal Space. |
| **Offline** | After full download, the user edits without a network, sees that work is locally saved, and synchronizes later. |
| **Server rejection** | A forbidden transaction is not partially published, is not lost, and comes with a usable resolution plan. |
| **Associative recall** | An imprecise description finds verifiable original sources; incomplete search and AI hypotheses are clearly labeled. |
| **Interface replacement** | AI writes an unconventional interface as data through the API; each participant chooses when to switch and can use built-in recovery if it breaks. |
| **Extension and automation** | Imported objects and a native extension add new scenarios; code does not run on someone else's device without consent; background work is visible. |
| **Recovery** | A clean installation can restore saved data and definitions; missing attachments are explicitly identified. |
| **Atomic visibility** | A multi-Space Change appears as one update across the receiving client's authorized selected Spaces, including after restart. Hidden parts are not disclosed. |
| **Strict constraints** | Concurrent offline claims of one unique value remain locally preserved; the server accepts at most one and explains how to repair the other. |
| **Version skew** | A schema upgrade preserves compatible old-device writes, identifies incompatible work, and offers recovery when an old interface can no longer safely edit. |
| **Execution grants** | An imported interface can display with handlers disabled; publishing or switching versions does not authorize new executable code or broader access. |
| **Durable workflow** | Restart preserves completed steps; an uncertain external result pauses without an uncontrolled repeat. |
| **Historical recovery and purge** | Compaction preserves archived durable history; explicit purge blocks stale-device resurrection while keeping unrelated objects' history recoverable. |
| **Accessible baseline** | A user can perform essential catalog operations and reach recovery with a keyboard or screen reader, without drag-only actions. |

### Unacceptable implementation outcomes

Adding a new user-defined type requires a new domain-specific core function; same-named fields merge automatically; changing the UI changes permissions; server rejection erases local work; AI is represented as a human author; search hides incomplete indexing; third-party code executes merely because it appeared in a shared Space.

> Nutrition, workouts, English learning, and projects are reference scenarios for universality. None becomes the required specialization of the product.

---

## 18. Remaining Decisions and Development Readiness

### Remaining product choices

External AI provider selection, connection methods, and initial spending-limit defaults remain open. Provider access must use explicit credentials and transfer grants; website cookies, free quotas, or subscription-included API usage are not assumed. The product name, commercial model, and release schedule are also open.

History retention, outdated-device recovery, transfer-policy authority, the supplied interface, and accessibility have defined policies in sections 06 and 12–15. They are not open alternatives to be selected during implementation.

### Implementation choices within this baseline

Authentication and session mechanisms, wire schemas, detailed Luau SDK and WASM host functions, the native C ABI, and exact SQL layouts remain development work. The semantic embedding model and inference runtime must pass Russian/English retrieval evaluation and physical Android measurements. Resource budgets must name baseline devices, datasets, cold/warm conditions, and write sizes before they become acceptance limits.

Core, server, and clients pin compatible library versions. Current upstream documentation describes candidate mechanisms, not proof that the selected releases provide all required behavior. Experimental materialized views, CDC, triggers, or index methods are not prerequisites for basic correctness.

### Required foundation prototypes

| Prototype | Required evidence |
|---|---|
| **Storage and acceptance** | Inspect and authorize Loro operation ranges, reject protected-path and forged-peer writes including invisible history changes, validate strict constraints under concurrent submissions, and preserve rejected dependent chains. |
| **Atomic commit and delivery** | Crash after every coordinator stage; repeat and reorder submissions and deliveries; recover every committed part before serving reads; verify that UI, CLI, subscriptions, and backups see one Change boundary. Test partial authorized envelopes, interrupted transfers, and access revocation during delivery. |
| **History and recovery** | Exercise shallow compaction, expired leases, fresh editing peers, old-device writes after transfer or purge, retained publications, per-document archive erasure, and restore of multi-Space pending work and attribution. Verify the exact Turso backup and index behavior. |
| **Documents of block Objects** | Open a document with a thousand block documents, type continuously, split and merge blocks through the coordinator, and measure load time, memory, and input latency against the two-layer write model of section 12. |
| **Desktop and Android runtime** | Build and run pinned Tauri, Turso, Loro, mlua/Luau, analyzer bridge, and Wasmtime together. Test sandbox interruption, host cancellation, restart recovery, desktop resident/CLI lifecycle, and physical Android resource use. Android starts with arm64 devices and x86_64 emulators; additional ABIs require separate validation. |

Wasmtime documents Android support with weaker target support tiers than its primary desktop targets. That is a validation requirement, not a decision to replace the runtime. The Rust ws-proto runtime is a separate dependency: validate its implementation and compatibility before relying on it for these links. The Luau descriptor generator and analyzer bridge are explicit implementation tasks.

### First working stage

After the foundation prototypes, build a desktop client, Android client, and one server with two users and multiple devices. Create two different user-defined domain models without core changes. The stage covers Entities, Relations, Layers, document blocks, private and shared Spaces, full initial replication, offline writes, server rejection and repair, schema version skew, attachment upload and offline pinning, partial search indicators, and a clean backup restore.

A supplied universal interface exercises these operations. Later stages complete the wider catalog, automation, and extensions already included in v0; this sequence does not reduce the release scope. Network-facing and executable-code features require verified authorization and sandbox boundaries before release.

> Development can start from this baseline. Integration and fault-injection results determine whether the implementation satisfies it; documentation alone does not establish those guarantees.

---

## 19. Technology Stack and Physical Model

### Stack

| Part | Decision |
|---|---|
| Core, server, CLI | **Rust**. A core library without shell dependencies is embedded by the desktop and Android applications and the server. |
| Shells | **Tauri 2** on desktop and Android. Desktop stays resident after window close; explicit Quit flushes durable work. Android has no background service. |
| Renderer | **TypeScript and React** in the WebView. The trusted renderer draws catalog nodes and sends events; user code executes only in authorized core runtimes. |
| Contracts and transport | **Protobuf over ws-proto** on server/core, core/renderer, core/CLI, and core/scripts/extensions links. All clients use one authorization dispatcher. The Rust ws-proto runtime is maintained separately. An in-process transport optimization must preserve protocol sessions and enforcement; removing ws-proto from a link requires an explicit revision. |
| Structured storage | **Turso**, embedded in-process: one database per Space plus a coordination database per device and an equivalent server coordinator database. Documents, base projections, and indexing tasks share each Space transaction. |
| Attachment storage | Immutable blob files plus manifests in Turso. The single structured database engine does not require storing attachment bytes in Loro or SQL. |
| CRDT | **Loro**: one document per Object and CRDT generation. |
| Scripting | **Luau through mlua**, with a separate analyzer bridge and descriptor-generated typed SDK. Rhai remains an alternative only if a validated build problem requires a revision. |
| Compiled extensions | **Wasmtime** with explicit host capabilities, bounded memory, fuel, and deadlines. Host calls have independent limits and cancellation. |
| Native extensions | Desktop dynamic libraries with a C ABI and explicit user consent; native execution is not sandboxed. |

Go is not used; iOS is outside v0. On desktop one core owns each profile's files. CLI and other tools connect to that core and never become parallel database writers. Startup may launch the resident application; operation without a desktop display must be validated on the target OS. Loopback endpoints are authenticated and subject to the checks in section 14.

### Objects and authority

Every Entity, Relation, Layer, definition, View, Context, Interface, component, rule, script, agent configuration, document block, attachment manifest, and Space configuration is an **Object** with a UUIDv7. Its live content belongs to one Space and one current Loro generation. Authoritative registry records identify the owning Space, location epoch, generation, kind, protected creation metadata, schema epochs, and purge state. Server-hosted Spaces use the server registry; local-only Spaces use the local core registry.

Ordinary CRDT edits do not change registry authority. System-controlled metadata may appear as read-only mirrors in a document, but new operations targeting protected paths must be rejected even if they lose a last-writer-wins race and do not alter visible state. Dedicated administrative commands implement ownership, schema, and purge changes.

| Container | Loro type | Content |
|---|---|---|
| `meta` | map | Protected registry mirrors, controlled deletion state, Layer target, and hypothesis metadata. Mutable semantic metadata is distinct from authoritative ownership and authorship. |
| `types` | map | Type-definition ID to presence; display names do not establish identity. |
| `fields` | map | Field-definition ID to its fixed storage kind: last-writer-wins scalar, collaborative text, map, movable list, reference, or another explicitly supported kind. |
| `participants` | map, Relations | Stable slot ID to object reference and participant-role definition. |
| `blocks` | movable tree, documents | Placement nodes with a `block_ref`; block text, properties, types, and fields live in the referenced block Object. |

References use Object ID; operation envelopes also name the target generation and location epoch. Field IDs fix CRDT kind. Concurrent initialization uses Loro mergeable containers of the same declared kind, not competing container types under one key.

A Layer has its own document, types, fields, target, and owning Space. Views explicitly choose their read and write destinations. Document placements and block identities are separate; fragment references use cursors and quote/context metadata as described in section 08. Neither Layers nor references grant access to their targets.

### Published definitions and migrations

A publication is an immutable artifact containing its definition or interface ID, version ID, logical snapshot, exact dependency versions, required API/core features, and content digest. Pins reference these artifacts, not historical Loro frontiers. Canonical digest encoding is defined by the application; ordinary protobuf serialization is not assumed canonical. Pinned artifacts remain in full local replicas and backups even after CRDT compaction.

Compatible upgrades atomically change the authorized pin. An incompatible migration pins an immutable plan, expected schema epoch, affected definitions, dry-run results, and recovery state. Client-side migration code runs under an Admin/Owner grant; the server validates its submitted data and administrative operations without executing that code. Large migrations stage their results and expose them only at the declared migration commit boundary. Older writes are accepted only when compatible with current semantics; incompatible work remains recoverable.

### Change and audit

The following is an architectural outline, not a finalized wire schema:

```text
Change {
  id, principal, actor, device, created_at,
  docs: [{object_id, space_id, generation, location_epoch, schema_epoch,
          from_version, to_version, operation_ranges, update_bytes}],
  dependencies, audit,
  status: local | queued | waiting | accepted | rejected | paused
}
```

The core builds candidate documents and exports each write's operation ranges and incremental updates. The payload of a submitted Change is immutable. Reusing its ID with different content is rejected; replaying the same submitted payload returns the recorded outcome. CRDT commit groups operations but supplies neither SQL rollback nor cross-document ACID.

The durable audit records Change ID, per-object operation ranges, actor kind and identity, authenticated principal and delegation, device/session, available model identification, sources and reason, local timestamp, and server acceptance timestamp. Authorized delivery, initial replication, archives, and backups include the corresponding audit data. Private metadata is filtered with the data it describes.

Loro `origin` is a local event label and is not persisted. A persisted commit message may carry an additional Change link; neither replaces audit. Actor ID and editing peer ID are distinct. Editing peers cannot be shared by concurrent writers or reused with excluded counters after rebuild. Restore and new editing sessions use fresh peers while retaining historical authorship.

### Commit coordinator and visibility

One core owns and coordinates writes to its Space files. The initial server uses the same single-writer approach; more concurrency requires preserving validation and commit ordering. A local-only Space passes through the local validator and coordinator without server submission. Own single-document edits bypass the coordinator (section 12); opening a document prefetches its blocks' snapshots in one query from the placement tree and keeps hot documents in a bounded in-memory cache.

For an ordinary Change, the coordinator:

1. Builds and validates all candidate documents and projection changes without modifying live state; read views remain at the previous committed boundary.
2. Durably writes a full redo record to the coordination database: Change payload, all part payloads, audit and control-plane effects, ordering, and a `COMMIT` decision.
3. Applies each part idempotently in its Space transaction, including document data, base projections, outbox or acceptance state, indexing tasks, and applicable cursor advances. Each part records its applied Change ID.
4. Marks the journal entry `APPLIED` after every part is durable, switches live caches, releases the visibility gate, and publishes one batch notification. Only then does it report local durability or server acceptance.

Before the durable decision, no live part changes. After it, restart recovery completes every committed part before serving requests. An inability to finish, including disk-full, produces an explicit recovery-required state rather than partial visibility. All API reads, subscriptions, CLI operations, and backup use the gate; existing immutable read snapshots may finish against the previous boundary.

The coordinator journal is source data until its committed parts are installed and checkpointed. It is included in recovery and backup. This is an application protocol over per-file transactions, not an assumed Turso cross-file transaction. Native code that bypasses the core is outside these guarantees.

### Server acceptance and strict constraints

The server authenticates the principal and delegation, checks every affected Space's current permissions, and validates protocol versions, limits, registry state, schema epochs, and idempotency. It verifies admissible new operation ranges, protected paths, and peer claims; arbitrary client snapshots do not bypass ingestion checks.

Validation is two-staged. Every update is first **decoded without being applied**: Loro's JSON operation export exposes peer, counter, container, and key, which is enough for protected paths, forged peers, operation ranges, and size limits. A **full import** into a candidate document happens only for documents whose touched fields are under a strict constraint or whose operations affect registry-mirrored state. Hot documents stay in a bounded server cache; cold ones load from the snapshot store. Each Change is bounded by configured limits on bytes, number of documents, and per-document size; a multi-Space Change takes per-Space writer locks in Space-ID order.

Dependencies must already be accepted. Missing dependencies produce `waiting`, not publication. The server imports updates into candidate documents; unresolved Loro pending operations prevent acceptance of the whole Change. Shared deterministic Rust validators check system invariants and strict declarative policies. User scripts, bindings, or migration code never execute here.

Strict uniqueness uses a versioned constraint ID and a Space-local canonical key with defined type, null, normalization, and tombstone rules. Validation and constraint-index changes share the coordinated commit, preventing concurrent claims from both succeeding. Offline validation is provisional. Diagnostic constraints record violations without rejecting the Change.

After validation the coordinator installs document state, logs, audit, and assigned sequences. Acceptance is acknowledged only after all parts are durable and visible. The server merges CRDT state using Loro; it does not execute user code or invent an alternative CRDT conflict policy.

### Dependencies and rejection

The core maintains a Change DAG containing document causal prerequisites and semantic prerequisites: creation of referenced objects or definitions, reads of unaccepted state used by an action, and explicit workflow dependencies. Loro pending imports describe dependencies within a document and do not replace this graph.

Rejection preserves the original bytes, visible local branch, reason, and dependent work. The live state is rebuilt from accepted checkpoints and accepted updates plus pending local Changes outside the rejection's transitive causal and semantic closure. The closure becomes `paused`, with its data retained. New editing peers prevent counter collisions after excluding operations.

The user compares, repairs, retries, retains, or discards the preserved work. Repair creates a new Change and fresh operations linked to the original. An unknown transport outcome is reconciled by querying or repeating the same immutable submission; a rejected branch is not silently resubmitted on a new base. Changes are never automatically moved into a personal Space.

### Synchronization and authorized delivery

Per-Space monotonic sequences discover accepted Changes. Clients retain committed cursors; Loro absorbs duplicate operations. A cursor below the retained discovery history triggers a new snapshot boundary and tail download, not an assumption that all old log entries remain available. A live stream announces new sequences. Set reconciliation for federation is a later alternative to discovery, not part of v0.

The server projects each accepted Change into a complete delivery envelope for the user's currently authorized and selected Spaces. Hidden Space IDs, hidden part counts, private audit/provenance details, and private dependency metadata are omitted. The original Change is accepted in full; the client applies all parts represented in its envelope atomically. It never waits for inaccessible or unselected parts.

Large envelopes are staged through chunks and installed only after the whole authorized manifest has arrived. Subscription and ACL changes invalidate or rebuild the delivery scope. Cursor advances happen with installation through the coordinator, followed by one UI notification. Initial replication establishes a consistent snapshot boundary and then imports the tail.

The default 90-day merge lease is enforced through causal frontiers and acknowledgements from non-expired devices, including pending local bases. Compaction must not advance past a promised active lease's required base. After lease expiry, local branches are preserved and compared against a fresh replica; explicit reapplication creates new operations. History archives do not imply automatic reconciliation of arbitrarily old branches.

### Historical archive and purge

Hot document storage contains snapshots and updates needed for current state and the supported merge window. Cold history contains immutable checkpoints, per-document update segments, and audit for indefinite recovery of accepted durable Changes. A historical viewer reconstructs read-only state independently of live admission. Publication artifacts remain independent of archive frontiers.

Purge removes managed payload for the erased object from live state, archives, and indexes, and releases its attachment references. Independent per-document payloads and rewritten verifiable archive manifests preserve other objects' history. Surviving references may point to erased objects; reconstruction does not resurrect their prerequisites. Derived copies have their own lifecycle.

The authoritative graveyard stores the minimum purged Object ID/generation information needed to reject stale writes and creation with a purged identity. Transfer commands preserve Object ID but establish new location and CRDT generations; default transfer copies current logical state and keeps source history under source ACL. An explicit copy creates a new identity and is subject to transfer policies. Backups and uncontrolled external copies are not remotely erased.

### Attachments

An attachment Object identifies immutable bytes by SHA-256, size, media type, and display metadata. Access is scoped to the owning Space; a content hash does not confer permission or reveal another Space's possession of a blob. Cross-Space deduplication must preserve those boundaries.

Upload and download use resumable ws-proto chunks with transfer IDs, offsets or parts, and checksums. Finalization verifies the complete blob. A shared Change introducing a blob reference waits until the server has finalized the bytes; the offline form is explicitly local and pending. No accepted reference depends on an uncommitted upload.

Garbage collection respects live references, retained history, rejected/outbox work, and active backup holds, subject to explicit purge. Abandoned uploads expire. Local cache eviction never removes pinned blobs, sole existing copies, or unsynchronized bytes. Availability and backup omissions remain visible.

### Storage layout and projections

| Data | Location | Recovery role |
|---|---|---|
| Live documents and replication state | Space database: snapshots, updates, outbox, rejection branches, accepted IDs, sequences, and cursors | Source data; never reconstructed from indexes alone. |
| Publications and history | Space-managed immutable publication records, archive checkpoints, per-document segments, and audit | Required for pinned versions and retained historical recovery; archive availability is reported. |
| Coordinator and local control | Device database: redo, authority records for local Spaces, editing sessions, grants, workflow checkpoints, and configuration; equivalent server coordinator database for server authority | Source data; journals and authority must be restored consistently with Space files. Secrets use protected credential storage and are not exposed in ordinary exports. |
| Base projections | Space database: object metadata, types, typed field values, participants, placements, and adjacency tables | Rebuilt from source documents and authoritative records; updated in the same per-Space transaction. |
| Search and expensive derived results | Durable dirty tasks, full-text and vector indexes, and derived tables with revision/model/processing versions | Rebuildable; completeness watermarks expose lag. |
| Attachment bytes | Immutable blob files referenced by Space manifests | Source data with explicit missing-item reporting and checksums. |

Rust code updates base projection tables and queues indexing tasks with the source write. Embedding workers compute outside the write transaction and install results only after a revision and processing-version check. Query results report data and lexical/semantic index watermarks, pending work, and missing bytes. Index completeness is separate from server freshness.

Ordinary derived tables and Rust traversal over adjacency projections provide the baseline; correctness does not depend on recursive SQL, triggers, experimental materialized views, custom index methods, or CDC. The exact release's FTS crash behavior and vector support must be tested; SQLite compatibility does not imply FTS5 compatibility.

### Backup and restore

Backup takes a coordinator barrier, completes committed redo, and temporarily stops writes, compaction, and purge while capturing a common boundary for all selected Space files and the coordination database. A verified engine export mechanism creates each consistent file; current Turso compatibility documents support `VACUUM INTO`, while SQLite backup C API support must not be assumed. The pinned release must pass a staging restore test.

The manifest identifies commit boundaries, formats, Spaces, cursors, authority, publications, audit, archive coverage, workflow checkpoints, pending/rejected work, attachment hashes, and omissions. Immutable blobs remain pinned while copying after the database barrier is released. A backup becomes complete only after manifest, checksum, and readability checks; missing archive or attachment data is named explicitly.

Restore first opens a staging dataset, recovers its coordinator, and verifies it before activation. Historical IDs and attribution are retained; fresh installation sessions and editing peers prevent cloned writers. Reconnecting to an existing server first reconciles current memberships, epochs, generations, and purge records. Credentials and execution grants are revalidated on the restored installation rather than automatically authorizing code from the backup.

> **Stack formula:** Rust core in Tauri; protobuf over ws-proto; Loro documents per Object generation; Turso per Space with synchronous base projections and durable indexing tasks; a coordinator for atomic multi-Space visibility; immutable attachments and retained history; per-Space discovery with authorized Change envelopes.

---

## 20. Reference Documentation

These sources establish library behavior and constraints. Application policies and coordinator algorithms above are product decisions, not guarantees supplied by these libraries. Version-dependent behavior must be checked against pinned releases during implementation.

| Topic | Primary references |
|---|---|
| Loro transactions, origin, imports, and peers | [Rust LoroDoc API](https://docs.rs/loro/latest/loro/struct.LoroDoc.html), [import status](https://loro.dev/docs/concepts/import_status), [peer management](https://loro.dev/docs/concepts/peerid_management) |
| Loro packing and recovery | [mergeable containers](https://loro.dev/docs/concepts/container), [trees](https://loro.dev/docs/tutorial/tree), [stable cursors](https://loro.dev/docs/concepts/cursor_stable_positions), [encoding](https://loro.dev/docs/tutorial/encoding), [persistence](https://loro.dev/docs/tutorial/persistence) |
| Coordination and storage | [SQLite WAL](https://www.sqlite.org/wal.html), [ATTACH](https://www.sqlite.org/lang_attach.html), [VACUUM INTO](https://www.sqlite.org/lang_vacuum.html), [Turso compatibility](https://github.com/tursodatabase/turso/blob/main/COMPAT.md), [coordination avoidance research](https://www.bailis.org/papers/ca-vldb2015.pdf) |
| Execution boundaries | [Luau sandbox](https://luau.org/sandbox/), [mlua API](https://docs.rs/mlua/latest/mlua/struct.Lua.html), [Tauri security](https://v2.tauri.app/security/), [Wasmtime support tiers](https://docs.wasmtime.dev/stability-tiers.html) |
| Authorization and network access | [OWASP authorization](https://cheatsheetseries.owasp.org/cheatsheets/Authorization_Cheat_Sheet.html), [WebSocket security](https://cheatsheetseries.owasp.org/cheatsheets/WebSocket_Security_Cheat_Sheet.html) |
| Workflow and attachment practices | [Temporal workflow history](https://docs.temporal.io/workflows), [multipart lifecycle](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html), [upload integrity](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html) |
| Versioning and usable interfaces | [Protobuf best practices](https://protobuf.dev/best-practices/dos-donts/), [W3C fragment selectors](https://www.w3.org/TR/selectors-states/), [WCAG 2.2](https://www.w3.org/TR/WCAG22/), [Android accessibility](https://developer.android.com/guide/topics/ui/accessibility/views/apps-views) |

---

## Appendix: Consolidated Decisions

*The latest agreed decisions replace earlier alternatives rather than coexisting with them.*

This document consolidates an architecture interview. It is a working product vision, not confirmation of every intermediate technical suggestion in the conversation. Where decisions changed, the latest explicit user choice takes precedence.

| Earlier direction | Current vision |
|---|---|
| Git as the foundation of data and authorization | One authoritative server; Git is not used for user data in v0. |
| Multiple servers and repositories as the foundation | Multiple Spaces on one server; independent servers are outside v0. |
| Canonical tree and primary parent | Graph-first; hierarchies arise from selected relationships and Views. |
| Physical packing independent of Object identity | One Loro document per Object generation; document trees reference first-class block Objects stored in the Space database. |
| Partial cache of source graph data | Full replica of each selected Space; lazy loading for attachments. |
| Server executes user workflows | Agents, cron, scripts, and modules execute on client devices. |
| Several equal Space Owners | One Owner; Admin, Editor, and Viewer roles. |
| Synchronization of working sessions | Saved interfaces and settings sync, not open tabs. |
| Fully replaceable application | Permanent Internal Shell; Space-defined interfaces inside it. |
| Programmable UI through a TypeScript SDK with its own runtime | Interfaces are data rendered by a fixed renderer; built through the core API by builders, AI, scripts, and extensions. |
| Git packages in v0 | Export and import of objects; packages deferred. |
| Native modules behind a separate extension API | Three tiers on one API: Luau scripts, WASM extensions, native libraries with a C ABI. |
| Scripting language and expression language left open | Luau for scripts, handlers, and bindings; no separate expression language. |
| External file editing as a required feature | Not required in v0; export does not imply automatic bidirectional import. |
| Encryption and cloud backups in the first release | Both deferred; v0 uses local backups without custom content encryption. |
| Local core as a separate daemon process | The core is a library inside the Tauri application; the desktop application stays resident; Android has no background service. |
| Cursor-based sync engine | CRDT merge by Loro; discovery by a semantics-free per-Space sequence, replaceable by set reconciliation for federation. |
| SQL transaction assumed atomic across Space files | Turso per Space with synchronous base projections; durable redo coordinator provides multi-Space visibility and recovery. |
| Every derived index changes with source state | Durable index tasks share the source transaction; expensive results are installed asynchronously with revision checks and completeness watermarks. |
| CRDT dependencies alone pause rejected work | Core maintains a causal and semantic Change DAG; rejected closures are preserved and repaired through new Changes. |
| Attribution stored in Loro origin | Durable synchronized audit links actors to operation ranges; origin is a local event label. |
| Interface data implies handlers need no consent | Layout and bounded read-only bindings display without a separate grant; executable handlers require version-specific device grants. |
| Compaction defines the lifetime of user history | Hot CRDT state may compact; accepted durable history remains archived until purge. |
| Coroutine state provides workflow durability | Stable steps, durable intent/result records, checkpoints, and explicit unknown outcomes provide resumption. |
| Every write passes the commit coordinator | Own single-document edits are durable and visible immediately; the coordinator governs Changes, the unit of submission and multi-document atomicity. |
| Server applies every update to a full document | Decode-only inspection for all updates; full candidate import only for strict constraints and registry-mirrored state. |
| One grant prompt per code version | Capability classes: benign code is granted by device policy; only boundary-changing capabilities prompt. |
| Provenance labels per value | Space-level labels per invocation, declared in the script manifest and enforced by the dispatcher. |

### Status of this baseline

Version 0.2 defines the product and architecture baseline, including consistency, execution, and recovery policies. Section 18 names remaining choices and required implementation evidence. The product name, commercial model, and release schedule remain open; integration tests must establish the specified guarantees before release.

> **Core formula:** One graph and a shared capability core; user-defined models, interfaces, and agents; autonomous devices; one authoritative server for shared state and access.
