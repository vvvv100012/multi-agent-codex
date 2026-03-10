## Goal-Driven Internal Crypto Research Reports

This project is a goal-driven workflow that turns a plain-language research objective into:

`research_brief -> source_registry -> evidence_cards -> gap_log -> final_answer -> workbench`

The system is still **internal-first**. It now supports two internal report shapes while keeping the same research pipeline and final JSON/Markdown artifacts:

- `management_brief`: decision-first, compressed, implication-heavy
- `internal_share`: insight-first, mechanism-aware, discussion-friendly

### Research mode

Management brief example:

```bash
python3 scripts/run_pipeline.py   internal   --goal "Do a quick Hyperliquid tradfi/rwa market study. Focus on implementation, product richness, volume growth, and user demand. The purpose is to judge how far traditional-asset perpetuals have progressed."   --audience management   --report-mode management_brief   --live-search
```

Internal share example:

```bash
python3 scripts/run_pipeline.py   internal   --goal "Prepare an internal sharing note on how tokenized treasury demand is splitting across venues, and what this might imply for future exchange product design."   --audience internal_research_team   --report-mode internal_share   --live-search
```

Useful flags:

```bash
python3 scripts/run_pipeline.py   internal   --goal "Compare the current state of tokenized treasuries demand across major crypto venues."   --ticker ONDO   --mode auto   --audience internal_strategy_team   --report-mode auto   --rounds 3   --live-search
```

Hard timeout controls (all codex-driven stages):

```bash
python3 scripts/run_pipeline.py internal --goal "..." --hard-timeout-profile balanced
python3 scripts/run_pipeline.py internal --goal "..." --hard-timeout-sec 1800
```

- `--hard-timeout-profile`: `strict | balanced | relaxed` (default `balanced`)
- `--hard-timeout-sec`: override all stage timeouts with one value
- `balanced` profile: planning 1200s, source_scout 1500s, evidence 2100s, skeptic 1800s, synthesis 2100s, polish 900s
- Hard timeout triggers one automatic retry for that stage. If it still times out, the run fails and can be resumed with `--resume`.

Outputs are written to `output/<RUN_ID>/`:

- `run_manifest.json`
- `feedbacks.txt`
- `run_events.jsonl`
- `schema_metrics.json`
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

Feedback-driven round-by-round iteration:

1. Run to a target round (for example `--rounds 1`).
2. Add feedback to `output/<RUN_ID>/feedbacks.txt`.
3. Resume with a higher round target (for example `--rounds 2`, then `--rounds 3`).

```bash
python3 scripts/run_pipeline.py internal --resume --run-id <RUN_ID> --rounds 2
python3 scripts/run_pipeline.py internal --resume --run-id <RUN_ID> --rounds 3
```

If feedback should re-shape the research brief itself, add `--replan-on-feedback` on resume:

```bash
python3 scripts/run_pipeline.py internal --resume --run-id <RUN_ID> --rounds 2 --replan-on-feedback
```

Inspect the local data registry without running the full pipeline:

```bash
python3 scripts/build_data_registry.py --output output/data_registry_preview.json
```

### Design notes

- CLI exposes two workflow entrypoints: `internal` and `external`.
- Only `internal` is implemented today. `external` remains reserved for a future public-facing workflow.
- The default workflow is goal-driven, not ticker-driven.
- The final deliverable stays `research_answer`, but it is now shaped as either a `management_brief` or an `internal_share`.
- The multi-agent team is `planner / scope_guard / research_scout / data_extractor / skeptic / synthesizer / editor`.
- The workflow assumes you precompute datasets into `data/` ahead of time.
- `data_registry.json` is generated from `data/` and tells agents which local files exist, what structure they have, and how they should be used.
- Local datasets are indexed into `source_registry.json` as `Data` sources so agents can cite them directly and use web search mainly for context or validation.
- `workbench.html` is a static local page that visualizes the run, sources, evidence, gaps, and final answer.
