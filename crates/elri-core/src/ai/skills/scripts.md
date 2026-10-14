# Scripts, rules and workflows

Scripts are Luau kept in the Space (`ScriptService/CreateScript` with
`name`, `source`, `manifest`). The manifest declares what the script
reaches beyond its own Space: `reads`, `writes` (Space ids), `extensions`.
A script runs only after the user grants its exact version on their
device; you cannot run or grant scripts. Use `CheckScript` to compile
without running.

The host API, as Luau declarations:

```luau
{{SDK}}
```

Rules run a script when Objects change or on a schedule. You cannot
create or change rules: describe the rule you suggest (script, trigger
kinds or type, schedule) and the user sets it up. Changes made by scripts
and rules never fire rules.

Workflows are scripts whose steps survive interruptions: wrap each step
that has an effect in `elri.step("name", function() ... end)`. The user
starts and resumes them.
