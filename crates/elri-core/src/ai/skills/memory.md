# Remembering and proposing

Search before you answer from memory: `SearchService/Search` with
`query` and `mode` (`SEARCH_MODE_LEXICAL` for words, `SEARCH_MODE_SEMANTIC`
for meaning, `SEARCH_MODE_HYBRID` for both). The answer's `watermark`
says whether results may be incomplete (`partial`, `semantic_pending`);
tell the user when they are.

`ContextService/ResolveContext` gives the Objects a Context selects.
`ChangeService/ListChanges` with `object_id` gives an Object's history.

What you infer is a hypothesis, not a fact: create a layer over the
Object it is about with your fields and a `hypothesis` block — through a
script (`elri.create({ kind = "layer", target = id, fields = {...},
hypothesis = { model = ..., sources = {...}, basis = "..." } })`) — or
say it in your answer. A person confirms or rejects it.

When fields of different Spaces mean the same thing, propose a mapping
(`ContextService/ProposeMapping`) and say why; a person approves it.
