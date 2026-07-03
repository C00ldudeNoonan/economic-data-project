---
title: dg scaffold component
triggers:
  - "creating a custom reusable component type"
---

Scaffold a new custom Dagster component type class. Must be run inside a Dagster project directory. The scaffold is placed in `<project_name>.lib.<name>`.

Use `dg scaffold component` when the component will be used multiple times. For one-off components, use `dg scaffold defs inline-component` instead, which places the definition directly under `defs/`.

```bash
dg scaffold component <class-name>
```

`--model / --no-model` — whether the generated class inherits from `dagster.components.Model` (default: `--model`).
