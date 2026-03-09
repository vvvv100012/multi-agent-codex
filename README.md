## Goal-Driven Research Crypto

This project is a goal-driven workflow that turns a plain-language research objective into:
`research_brief -> source_registry -> evidence_cards -> gap_log -> final_answer -> workbench`

### Research mode

Example:

```bash
python3 scripts/run_pipeline.py \
  internal \
  --goal "Do a quick Hyperliquid tradfi/rwa market study. Focus on implementation, product richness, volume growth, and user demand. The purpose is to judge how far traditional-asset perpetuals have progressed." \
  --live-search
```

Useful flags:

```bash
python3 scripts/run_pipeline.py \
  internal \
  --goal "Compare the current state of tokenized treasuries demand across major crypto venues." \
  --ticker ONDO \
  --mode auto \
  --audience internal_strategy_team \
  --rounds 3 \
  --model gpt-5.4 \
  --live-search
```

Outputs are written to `output/<RUN_ID>/`:

- `run_manifest.json`
- `data_registry.json`
- `research_brief.json`
- `source_registry.json`
- `evidence_cards.json`
- `gap_log.json`
- `final_answer.json`
- `final_answer.md`
- `workbench.html`

Resume the latest run:

```bash
python3 scripts/run_pipeline.py internal --resume
```

Resume a specific run:

```bash
python3 scripts/run_pipeline.py internal --resume --run-id 20260309T000000Z_hyperliquid
```

Inspect the local data registry without running the full pipeline:

```bash
python3 scripts/build_data_registry.py --output output/data_registry_preview.json
```

### Design notes

- CLI now exposes two workflow entrypoints: `internal` and `external`.
- Only `internal` is implemented today. `external` is reserved for a future public-facing research workflow with different prompts and report structure.
- The default workflow is goal-driven, not ticker-driven.
- The final deliverable is a research answer, not a forced LONG/SHORT/PASS memo.
- The multi-agent team is now `planner / scope_guard / research_scout / data_extractor / skeptic / synthesizer / editor`.
- The workflow assumes you precompute datasets into `data/` ahead of time.
- `data_registry.json` is generated from `data/` and tells agents which local files exist, what structure they have, and how they should be used.
- Local datasets are indexed into `source_registry.json` as `Data` sources so agents can cite them directly and use web search mainly for context or validation.
- `workbench.html` is a static local page that visualizes the run, sources, evidence, gaps, and final answer.
