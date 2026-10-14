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

Rules (`RuleService/CreateRule`) run a script when Objects change
(`trigger.kinds`, `trigger.type_id`) or on a schedule
(`trigger.every_seconds`, `trigger.missed`: skip, latest, catch-up, ask).
Changes made by scripts and rules never fire rules.

Workflows are scripts whose steps survive interruptions: wrap each step
that has an effect in `elri.step("name", function() ... end)`. The user
starts and resumes them.
