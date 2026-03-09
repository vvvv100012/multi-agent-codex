#!/usr/bin/env python3
import argparse
import html
import json
import os
import re
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from data_registry import build_data_registry, data_sources_from_registry

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_ROOT = ROOT / "output"
LAST_RUN_ID_PATH = OUTPUT_ROOT / ".last_run_id"
WORKFLOW_CHOICES = ("internal", "external")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def compact_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def log(message: str) -> None:
    print(message, flush=True)


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def slugify(text: str, max_len: int = 32) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9]+", "-", (text or "").strip())
    cleaned = re.sub(r"-{2,}", "-", cleaned).strip("-").lower()
    return cleaned[:max_len]


def first_non_empty_line(text: str, max_len: int = 120) -> str:
    for line in (text or "").splitlines():
        stripped = line.strip()
        if stripped:
            return stripped[:max_len]
    return ""


def format_duration(seconds: float) -> str:
    return f"{seconds:.1f}s"


def to_relative(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def unique_strings(values: list[str], *, limit: int | None = None) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        stripped = normalize_space(value)
        if not stripped:
            continue
        key = stripped.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(stripped)
        if limit is not None and len(out) >= limit:
            break
    return out


def try_parse_json(raw: str) -> dict | None:
    raw = (raw or "").strip()
    if not raw:
        return None
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return obj if isinstance(obj, dict) else None


def extract_json_object_from_text(text: str) -> dict | None:
    text = (text or "").strip()
    if not text:
        return None

    obj = try_parse_json(text)
    if obj is not None:
        return obj

    decoder = json.JSONDecoder()
    last_obj = None
    for index, char in enumerate(text):
        if char != "{":
            continue
        try:
            parsed, _ = decoder.raw_decode(text, idx=index)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            last_obj = parsed
    return last_obj


def render_template(path: Path, mapping: dict[str, str]) -> str:
    text = path.read_text(encoding="utf-8")
    for key, value in mapping.items():
        text = text.replace(key, value)
    return text


def run_codex_exec(
    prompt: str,
    schema_path: Path,
    out_json_path: Path,
    *,
    sandbox: str = "read-only",
    model: str | None = None,
    live_search: bool = False,
    stage_label: str | None = None,
    heartbeat_seconds: int = 20,
) -> dict:
    cmd = ["codex", "--ask-for-approval", "never", "exec", "--skip-git-repo-check"]
    if live_search:
        cmd.append("--search")
    cmd += ["--sandbox", sandbox, "--output-schema", str(schema_path), "--output-last-message", str(out_json_path)]
    if model:
        cmd += ["--model", model]
    cmd += ["-"]

    started = time.perf_counter()
    if stage_label:
        log(
            f"[RUN] {stage_label} | start "
            f"(schema={schema_path.name}, out={out_json_path.name}, search={live_search})"
        )

    with tempfile.TemporaryFile(mode="w+", encoding="utf-8") as stdout_f, tempfile.TemporaryFile(
        mode="w+", encoding="utf-8"
    ) as stderr_f:
        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=stdout_f,
            stderr=stderr_f,
            text=True,
            cwd=str(ROOT),
        )

        if proc.stdin is not None:
            try:
                proc.stdin.write(prompt)
                proc.stdin.close()
            except BrokenPipeError:
                pass

        while proc.poll() is None:
            time.sleep(max(1, heartbeat_seconds))
            if proc.poll() is None and stage_label:
                elapsed = time.perf_counter() - started
                log(f"[RUN] {stage_label} | still running ({format_duration(elapsed)})")

        stdout_f.seek(0)
        stdout = stdout_f.read()
        stderr_f.seek(0)
        stderr = stderr_f.read()

    file_raw = out_json_path.read_text(encoding="utf-8") if out_json_path.exists() else ""
    obj = try_parse_json(file_raw)

    if obj is None:
        obj = extract_json_object_from_text(stdout)
        if obj is not None:
            out_json_path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
            if stage_label:
                log(f"[RUN] {stage_label} | recovered JSON from stdout fallback")

    if obj is not None:
        if proc.returncode != 0 and stage_label:
            log(
                f"[RUN] {stage_label} | warning: codex exit code={proc.returncode}, "
                "but JSON result was recovered"
            )
        if stage_label:
            log(f"[RUN] {stage_label} | done in {format_duration(time.perf_counter() - started)}")
        return obj

    if proc.returncode != 0:
        raise RuntimeError(
            f"codex exec failed (code {proc.returncode}) and no valid JSON output was found.\n"
            f"STDERR:\n{stderr}\nSTDOUT:\n{stdout}"
        )

    raise RuntimeError(
        f"codex exec succeeded (code 0) but output was not valid JSON.\n"
        f"Path: {out_json_path}\nSTDERR:\n{stderr}\nSTDOUT:\n{stdout}"
    )


def save_json(path: Path, obj: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def load_json(path: Path, default: dict[str, Any] | None = None) -> dict[str, Any]:
    if not path.exists():
        return {} if default is None else default
    raw = path.read_text(encoding="utf-8").strip()
    if not raw:
        return {} if default is None else default
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        return {} if default is None else default
    return obj if isinstance(obj, dict) else ({} if default is None else default)


def ensure_json_file(path: Path, default_obj: dict[str, Any]) -> None:
    if not path.exists():
        save_json(path, default_obj)


def ensure_text_file(path: Path, default_text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text(default_text, encoding="utf-8")


def relative_from(base: Path, target: Path) -> str:
    return os.path.relpath(target, start=base)


def load_last_run_id() -> str | None:
    if not LAST_RUN_ID_PATH.exists():
        return None
    run_id = normalize_space(LAST_RUN_ID_PATH.read_text(encoding="utf-8"))
    return run_id or None


def remember_last_run_id(run_id: str) -> None:
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    LAST_RUN_ID_PATH.write_text(f"{run_id}\n", encoding="utf-8")


def latest_run_id() -> str | None:
    manifests = sorted(
        OUTPUT_ROOT.glob("*/run_manifest.json"),
        key=lambda item: item.stat().st_mtime,
        reverse=True,
    )
    return manifests[0].parent.name if manifests else None


def resolve_goal(args: argparse.Namespace) -> str:
    if args.goal and normalize_space(args.goal):
        return normalize_space(args.goal)
    if args.ticker:
        return f"Research {args.ticker.upper()} and explain the most decision-relevant findings."
    raise SystemExit("Provide --goal, or pass --ticker as a research hint.")


def resolve_mode(requested: str, goal: str, ticker: str | None) -> str:
    if requested != "auto":
        return requested
    if not ticker:
        return "objective"

    lowered_goal = goal.lower()
    broader_markers = (
        "compare",
        "landscape",
        "sector",
        "ecosystem",
        "market",
        "demand",
        "adoption",
        "growth",
        "tradfi",
        "rwa",
        "需求",
        "调研",
        "增长",
        "赛道",
        "竞争",
        "阶段",
    )
    if any(marker in lowered_goal for marker in broader_markers):
        return "objective"

    ticker_goal = lowered_goal.strip()
    if ticker_goal in {ticker.lower(), f"research {ticker.lower()}", f"analyze {ticker.lower()}"}:
        return "ticker"
    if len(ticker_goal.split()) <= 4 and re.fullmatch(r"[a-z0-9 ._/-]+", ticker_goal):
        return "ticker"
    return "objective"


def resolve_workflow_config(workflow: str) -> dict[str, Any]:
    if workflow == "internal":
        return {
            "name": "internal",
            "label": "Internal Research Workflow",
            "stage_prompts_dir": ROOT / "prompts" / "stages",
            "output_style": "internal_research_answer",
        }
    if workflow == "external":
        raise SystemExit("External workflow is scaffolded in CLI but not implemented yet. Use `internal` for now.")
    raise SystemExit(f"Unsupported workflow: {workflow}")


def resolve_research_run_id(args: argparse.Namespace, goal: str, ticker: str | None) -> str:
    if args.resume:
        run_id = args.run_id or load_last_run_id() or latest_run_id()
        if not run_id:
            raise SystemExit("Could not find a previous run to resume. Pass --run-id explicitly.")
        return run_id

    if args.run_id:
        return slugify(args.run_id, 48)

    seed = ticker.upper() if ticker else goal
    return f"{compact_timestamp()}_{slugify(seed, 24) or 'research'}"


def max_prefixed_id(items: list[dict[str, Any]], prefix: str) -> int:
    max_id = 0
    for item in items:
        value = item.get("id", "")
        match = re.fullmatch(rf"{prefix}(\d+)", str(value))
        if match:
            max_id = max(max_id, int(match.group(1)))
    return max_id


def normalize_url(url: str) -> str:
    return normalize_space(url)


def merge_sources(
    registry: dict[str, Any],
    added_sources: list[dict[str, Any]],
    *,
    round_num: int,
) -> tuple[dict[str, Any], int]:
    registry.setdefault("sources", [])
    by_url = {
        normalize_url(source.get("url", "")): source
        for source in registry["sources"]
        if normalize_url(source.get("url", ""))
    }
    next_id = max_prefixed_id(registry["sources"], "S")
    inserted = 0

    for item in added_sources:
        url_key = normalize_url(item.get("url", ""))
        if not url_key:
            continue

        payload = {
            "title": normalize_space(item.get("title", "")),
            "url": item.get("url", "").strip(),
            "workbench_url": item.get("workbench_url", "").strip(),
            "type": item.get("type", "Fact"),
            "updated_at": normalize_space(item.get("updated_at", "")),
            "relevance": normalize_space(item.get("relevance", "")),
            "takeaways": unique_strings(item.get("takeaways", []), limit=8),
            "first_seen_round": round_num,
            "last_seen_round": round_num,
        }

        if url_key in by_url:
            existing = by_url[url_key]
            for field in ("title", "workbench_url", "type", "updated_at", "relevance"):
                if payload[field]:
                    existing[field] = payload[field]
            existing["takeaways"] = unique_strings(
                list(existing.get("takeaways", [])) + payload["takeaways"],
                limit=8,
            )
            existing["last_seen_round"] = round_num
            continue

        next_id += 1
        payload["id"] = f"S{next_id:03d}"
        registry["sources"].append(payload)
        by_url[url_key] = payload
        inserted += 1

    registry["sources"].sort(key=lambda source: source.get("id", ""))
    return registry, inserted


def merge_evidence_cards(
    existing: dict[str, Any],
    added_cards: list[dict[str, Any]],
    registry: dict[str, Any],
    *,
    round_num: int,
) -> tuple[dict[str, Any], int]:
    existing.setdefault("evidence_cards", [])
    source_ids_by_url = {
        normalize_url(source.get("url", "")): source.get("id", "")
        for source in registry.get("sources", [])
        if normalize_url(source.get("url", ""))
    }
    by_claim = {
        normalize_space(card.get("claim", "")).lower(): card
        for card in existing["evidence_cards"]
        if normalize_space(card.get("claim", ""))
    }
    next_id = max_prefixed_id(existing["evidence_cards"], "E")
    inserted = 0

    for card in added_cards:
        claim = normalize_space(card.get("claim", ""))
        if not claim:
            continue
        claim_key = claim.lower()
        source_urls = unique_strings(card.get("source_urls", []), limit=8)
        source_ids = unique_strings(
            [source_ids_by_url.get(normalize_url(url), "") for url in source_urls],
            limit=8,
        )
        payload = {
            "claim": claim,
            "verdict": card.get("verdict", "insufficient"),
            "confidence": card.get("confidence", "medium"),
            "answer_relevance": card.get("answer_relevance", "supporting"),
            "as_of": normalize_space(card.get("as_of", "")),
            "metric_definition": normalize_space(card.get("metric_definition", "")),
            "notes": normalize_space(card.get("notes", "")),
            "question_ids": unique_strings(card.get("question_ids", []), limit=6),
            "source_ids": source_ids,
            "source_urls": source_urls,
            "first_seen_round": round_num,
            "last_seen_round": round_num,
        }

        if claim_key in by_claim:
            existing_card = by_claim[claim_key]
            for field in (
                "verdict",
                "confidence",
                "answer_relevance",
                "as_of",
                "metric_definition",
                "notes",
            ):
                if payload[field]:
                    existing_card[field] = payload[field]
            existing_card["question_ids"] = unique_strings(
                list(existing_card.get("question_ids", [])) + payload["question_ids"],
                limit=6,
            )
            existing_card["source_ids"] = unique_strings(
                list(existing_card.get("source_ids", [])) + payload["source_ids"],
                limit=8,
            )
            existing_card["source_urls"] = unique_strings(
                list(existing_card.get("source_urls", [])) + payload["source_urls"],
                limit=8,
            )
            existing_card["last_seen_round"] = round_num
            continue

        next_id += 1
        payload["id"] = f"E{next_id:03d}"
        existing["evidence_cards"].append(payload)
        by_claim[claim_key] = payload
        inserted += 1

    existing["evidence_cards"].sort(key=lambda item: item.get("id", ""))
    return existing, inserted


def merge_gap_entries(
    gap_log: dict[str, Any],
    added_gaps: list[dict[str, Any]],
    *,
    round_num: int,
) -> tuple[dict[str, Any], int]:
    gap_log.setdefault("gaps", [])
    by_question = {
        normalize_space(item.get("question", "")).lower(): item
        for item in gap_log["gaps"]
        if normalize_space(item.get("question", ""))
    }
    next_id = max_prefixed_id(gap_log["gaps"], "G")
    inserted = 0

    for item in added_gaps:
        question = normalize_space(item.get("question", ""))
        if not question:
            continue
        question_key = question.lower()
        payload = {
            "question": question,
            "why_it_matters": normalize_space(item.get("why_it_matters", "")),
            "why_unresolved": normalize_space(item.get("why_unresolved", "")),
            "suggested_next_step": normalize_space(item.get("suggested_next_step", "")),
            "severity": item.get("severity", "medium"),
            "status": "open",
            "first_seen_round": round_num,
            "last_seen_round": round_num,
        }

        if question_key in by_question:
            existing = by_question[question_key]
            for field in ("why_it_matters", "why_unresolved", "suggested_next_step", "severity"):
                if payload[field]:
                    existing[field] = payload[field]
            existing["status"] = "open"
            existing["last_seen_round"] = round_num
            continue

        next_id += 1
        payload["id"] = f"G{next_id:03d}"
        gap_log["gaps"].append(payload)
        by_question[question_key] = payload
        inserted += 1

    gap_log["gaps"].sort(key=lambda item: item.get("id", ""))
    return gap_log, inserted


def merge_contradictions(
    gap_log: dict[str, Any],
    contradictions: list[dict[str, Any]],
    *,
    round_num: int,
) -> tuple[dict[str, Any], int]:
    gap_log.setdefault("contradictions", [])
    by_issue = {
        normalize_space(item.get("issue", "")).lower(): item
        for item in gap_log["contradictions"]
        if normalize_space(item.get("issue", ""))
    }
    next_id = max_prefixed_id(gap_log["contradictions"], "C")
    inserted = 0

    for item in contradictions:
        issue = normalize_space(item.get("issue", ""))
        if not issue:
            continue
        issue_key = issue.lower()
        payload = {
            "issue": issue,
            "severity": item.get("severity", "medium"),
            "affected_question_ids": unique_strings(item.get("affected_question_ids", []), limit=6),
            "evidence_urls": unique_strings(item.get("evidence_urls", []), limit=8),
            "fix": normalize_space(item.get("fix", "")),
            "first_seen_round": round_num,
            "last_seen_round": round_num,
        }

        if issue_key in by_issue:
            existing = by_issue[issue_key]
            for field in ("severity", "fix"):
                if payload[field]:
                    existing[field] = payload[field]
            existing["affected_question_ids"] = unique_strings(
                list(existing.get("affected_question_ids", [])) + payload["affected_question_ids"],
                limit=6,
            )
            existing["evidence_urls"] = unique_strings(
                list(existing.get("evidence_urls", [])) + payload["evidence_urls"],
                limit=8,
            )
            existing["last_seen_round"] = round_num
            continue

        next_id += 1
        payload["id"] = f"C{next_id:03d}"
        gap_log["contradictions"].append(payload)
        by_issue[issue_key] = payload
        inserted += 1

    gap_log["contradictions"].sort(key=lambda item: item.get("id", ""))
    return gap_log, inserted


def merge_next_queries(gap_log: dict[str, Any], next_queries: list[str]) -> dict[str, Any]:
    gap_log["next_queries"] = unique_strings(list(gap_log.get("next_queries", [])) + list(next_queries), limit=24)
    return gap_log


def ensure_research_manifest(
    run_dir: Path,
    args: argparse.Namespace,
    goal: str,
    mode_resolved: str,
    workflow: dict[str, Any],
) -> tuple[Path, dict[str, Any]]:
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "interim").mkdir(parents=True, exist_ok=True)

    manifest_path = run_dir / "run_manifest.json"
    if args.resume and not manifest_path.exists():
        raise SystemExit(f"Resume requested but no run manifest exists at {manifest_path}")

    notes_seed = ""
    notes_seed_path = ROOT / "data" / "preliminary_thinking.txt"
    if notes_seed_path.exists():
        notes_seed = notes_seed_path.read_text(encoding="utf-8")

    artifacts = {
        "goal": to_relative(run_dir / "goal.txt"),
        "notes": to_relative(run_dir / "notes.txt"),
        "data_registry": to_relative(run_dir / "data_registry.json"),
        "research_brief": to_relative(run_dir / "research_brief.json"),
        "source_registry": to_relative(run_dir / "source_registry.json"),
        "evidence_cards": to_relative(run_dir / "evidence_cards.json"),
        "gap_log": to_relative(run_dir / "gap_log.json"),
        "final_answer_json": to_relative(run_dir / "final_answer.json"),
        "final_answer_markdown": to_relative(run_dir / "final_answer.md"),
        "workbench": to_relative(run_dir / "workbench.html"),
    }

    if args.resume and manifest_path.exists():
        manifest = load_json(manifest_path, {})
        existing_workflow = normalize_space(str(manifest.get("workflow_mode", "")))
        if existing_workflow and existing_workflow != workflow["name"]:
            raise SystemExit(
                f"Run {run_dir.name} was created for workflow `{existing_workflow}`, not `{workflow['name']}`."
            )
        manifest.setdefault("artifacts", {}).update(artifacts)
        manifest.setdefault("stages", {})
        manifest.setdefault("goal", goal)
        manifest.setdefault("audience", args.audience)
        manifest.setdefault("rounds_requested", args.rounds)
        manifest.setdefault("workflow_mode", workflow["name"])
        manifest.setdefault("workflow_label", workflow["label"])
        manifest.setdefault("output_style", workflow["output_style"])
    else:
        manifest = {
            "run_id": run_dir.name,
            "created_at": utc_now_iso(),
            "updated_at": utc_now_iso(),
            "status": "running",
            "goal": goal,
            "ticker_hint": (args.ticker or "").upper(),
            "audience": args.audience,
            "workflow_mode": workflow["name"],
            "workflow_label": workflow["label"],
            "mode_requested": args.mode,
            "mode_resolved": mode_resolved,
            "output_style": workflow["output_style"],
            "rounds_requested": args.rounds,
            "current_round": 0,
            "stop_reason": "",
            "primary_entity": (args.ticker or "").upper(),
            "artifacts": artifacts,
            "stages": {},
        }
        save_json(manifest_path, manifest)

    ensure_text_file(run_dir / "goal.txt", f"{goal}\n")
    ensure_text_file(run_dir / "notes.txt", notes_seed)
    ensure_json_file(run_dir / "data_registry.json", {"datasets": []})
    ensure_json_file(run_dir / "source_registry.json", {"sources": []})
    ensure_json_file(run_dir / "evidence_cards.json", {"evidence_cards": []})
    ensure_json_file(run_dir / "gap_log.json", {"gaps": [], "contradictions": [], "next_queries": []})

    save_json(manifest_path, manifest)
    remember_last_run_id(run_dir.name)
    return manifest_path, manifest


def update_manifest_stage(
    manifest_path: Path,
    manifest: dict[str, Any],
    stage_name: str,
    *,
    status: str,
    path: Path | None = None,
    summary: str = "",
    meta: dict[str, Any] | None = None,
) -> None:
    stage = manifest.setdefault("stages", {}).setdefault(stage_name, {})
    stage["status"] = status
    stage["updated_at"] = utc_now_iso()
    if path is not None:
        stage["path"] = to_relative(path)
    if summary:
        stage["summary"] = summary
    if meta:
        stage["meta"] = meta
    manifest["updated_at"] = utc_now_iso()
    save_json(manifest_path, manifest)


def set_manifest_status(
    manifest_path: Path,
    manifest: dict[str, Any],
    *,
    status: str,
    stop_reason: str = "",
) -> None:
    manifest["status"] = status
    manifest["updated_at"] = utc_now_iso()
    manifest["stop_reason"] = stop_reason
    save_json(manifest_path, manifest)


def render_markdownish(text: str) -> str:
    lines = (text or "").splitlines()
    parts: list[str] = []
    in_list = False

    def close_list() -> None:
        nonlocal in_list
        if in_list:
            parts.append("</ul>")
            in_list = False

    for raw_line in lines:
        line = raw_line.rstrip()
        stripped = line.strip()
        if not stripped:
            close_list()
            continue
        escaped = html.escape(stripped)
        if stripped.startswith("- "):
            if not in_list:
                parts.append("<ul>")
                in_list = True
            parts.append(f"<li>{escaped[2:]}</li>")
            continue
        close_list()
        parts.append(f"<p>{escaped}</p>")

    close_list()
    return "\n".join(parts)


def render_final_answer_markdown(answer: dict[str, Any]) -> str:
    lines = [f"# {answer.get('title', 'Research Answer')}", ""]
    summary = normalize_space(answer.get("summary", ""))
    if summary:
        lines.extend([summary, ""])

    for section in answer.get("sections", []):
        heading = normalize_space(section.get("heading", "Section"))
        if heading:
            lines.append(f"## {heading}")
        evidence_ids = unique_strings(section.get("evidence_ids", []), limit=12)
        if evidence_ids:
            lines.append(f"_Evidence: {', '.join(evidence_ids)}_")
            lines.append("")
        body = (section.get("body", "") or "").strip()
        if body:
            lines.append(body)
            lines.append("")

    open_gaps = unique_strings(answer.get("open_gaps", []), limit=12)
    if open_gaps:
        lines.append("## Open Gaps")
        for item in open_gaps:
            lines.append(f"- {item}")
        lines.append("")

    next_actions = unique_strings(answer.get("next_actions", []), limit=12)
    if next_actions:
        lines.append("## Next Actions")
        for item in next_actions:
            lines.append(f"- {item}")
        lines.append("")

    return "\n".join(lines).strip() + "\n"


def generate_workbench(
    run_dir: Path,
    manifest: dict[str, Any],
    brief: dict[str, Any],
    data_registry: dict[str, Any],
    source_registry: dict[str, Any],
    evidence_cards: dict[str, Any],
    gap_log: dict[str, Any],
    final_answer: dict[str, Any] | None,
) -> None:
    question_items = "".join(
        (
            "<li>"
            f"<strong>{html.escape(item.get('id', 'Q'))}</strong> "
            f"{html.escape(item.get('question', ''))}"
            f"<div class=\"meta\">{html.escape(item.get('why_it_matters', ''))}</div>"
            "</li>"
        )
        for item in brief.get("key_questions", [])
    ) or "<li>No research brief yet.</li>"

    metric_items = "".join(
        (
            "<li>"
            f"<strong>{html.escape(item.get('name', 'Metric'))}</strong>"
            f"<div class=\"meta\">{html.escape(item.get('definition', ''))}</div>"
            "</li>"
        )
        for item in brief.get("metrics", [])
    ) or "<li>No metrics defined yet.</li>"

    stage_cards = "".join(
        (
            "<div class=\"stage-card\">"
            f"<div class=\"stage-name\">{html.escape(stage_name)}</div>"
            f"<div class=\"stage-status {html.escape(stage.get('status', 'pending'))}\">{html.escape(stage.get('status', 'pending'))}</div>"
            f"<div class=\"meta\">{html.escape(stage.get('summary', ''))}</div>"
            "</div>"
        )
        for stage_name, stage in manifest.get("stages", {}).items()
    ) or "<div class=\"empty\">No stages completed yet.</div>"

    data_cards = "".join(
        (
            "<article class=\"card\">"
            f"<div class=\"eyebrow\">{html.escape(item.get('id', 'dataset'))} · {html.escape(item.get('kind', 'file'))} · {html.escape(item.get('analysis_priority', 'low'))}</div>"
            f"<h3>{html.escape(item.get('title', item.get('name', 'Dataset')))}</h3>"
            f"<p>{html.escape(item.get('description', ''))}</p>"
            + (
                f"<p class=\"meta\"><a href=\"{html.escape(item.get('workbench_path', '#'))}\" target=\"_blank\" rel=\"noreferrer\">Open local file</a></p>"
                if item.get("workbench_path")
                else ""
            )
            + (
                "<ul>"
                + "".join(
                    f"<li>{html.escape(line)}</li>"
                    for line in (
                        item.get("headline_takeaways")
                        or item.get("usage_hints")
                        or ([f"Columns: {', '.join(item.get('columns', [])[:6])}"] if item.get("columns") else [])
                    )
                )
                + "</ul>"
                if item.get("headline_takeaways") or item.get("usage_hints") or item.get("columns")
                else ""
            )
            + (
                f"<p class=\"meta\">Rows: {html.escape(str(item.get('row_count')))}</p>"
                if item.get("row_count") is not None
                else ""
            )
            + "</article>"
        )
        for item in data_registry.get("datasets", [])
    ) or "<div class=\"empty\">No local data indexed from data/ yet.</div>"

    source_cards = "".join(
        (
            "<article class=\"card source-card\" "
            f"id=\"source-{html.escape(source.get('id', ''))}\">"
            f"<div class=\"eyebrow\">{html.escape(source.get('id', ''))} · {html.escape(source.get('type', 'Fact'))}</div>"
            f"<h3>{html.escape(source.get('title', 'Untitled source'))}</h3>"
            f"<p class=\"meta\">{html.escape(source.get('updated_at', ''))}</p>"
            f"<p>{html.escape(source.get('relevance', ''))}</p>"
            "<ul>"
            + "".join(f"<li>{html.escape(item)}</li>" for item in source.get("takeaways", []))
            + "</ul>"
            f"<a href=\"{html.escape(source.get('workbench_url') or source.get('url', '#'))}\" target=\"_blank\" rel=\"noreferrer\">Open source</a>"
            "</article>"
        )
        for source in source_registry.get("sources", [])
    ) or "<div class=\"empty\">No sources yet.</div>"

    evidence_lookup = {
        card.get("id", ""): card for card in evidence_cards.get("evidence_cards", []) if card.get("id")
    }
    evidence_cards_html = "".join(
        (
            "<article class=\"card evidence-card\" "
            f"id=\"evidence-{html.escape(card.get('id', ''))}\">"
            f"<div class=\"eyebrow\">{html.escape(card.get('id', ''))} · {html.escape(card.get('verdict', 'insufficient'))} · {html.escape(card.get('confidence', 'medium'))}</div>"
            f"<h3>{html.escape(card.get('claim', 'Untitled evidence'))}</h3>"
            f"<p class=\"meta\">Relevance: {html.escape(card.get('answer_relevance', 'supporting'))}</p>"
            f"<p>{html.escape(card.get('notes', ''))}</p>"
            "<div class=\"chips\">"
            + "".join(
                f"<a class=\"chip\" href=\"#source-{html.escape(source_id)}\">{html.escape(source_id)}</a>"
                for source_id in card.get("source_ids", [])
            )
            + "</div>"
            "</article>"
        )
        for card in evidence_cards.get("evidence_cards", [])
    ) or "<div class=\"empty\">No evidence cards yet.</div>"

    contradiction_html = "".join(
        (
            "<article class=\"card gap-card\">"
            f"<div class=\"eyebrow\">{html.escape(item.get('id', ''))} · contradiction · {html.escape(item.get('severity', 'medium'))}</div>"
            f"<h3>{html.escape(item.get('issue', ''))}</h3>"
            f"<p>{html.escape(item.get('fix', ''))}</p>"
            "</article>"
        )
        for item in gap_log.get("contradictions", [])
    )
    gap_cards_html = "".join(
        (
            "<article class=\"card gap-card\">"
            f"<div class=\"eyebrow\">{html.escape(item.get('id', ''))} · gap · {html.escape(item.get('severity', 'medium'))}</div>"
            f"<h3>{html.escape(item.get('question', ''))}</h3>"
            f"<p>{html.escape(item.get('why_unresolved', ''))}</p>"
            f"<p class=\"meta\">Next: {html.escape(item.get('suggested_next_step', ''))}</p>"
            "</article>"
        )
        for item in gap_log.get("gaps", [])
    )
    gap_section_html = contradiction_html + gap_cards_html or "<div class=\"empty\">No open gaps yet.</div>"

    if final_answer:
        answer_sections_html = "".join(
            (
                "<article class=\"card answer-card\">"
                f"<div class=\"eyebrow\">{html.escape(section.get('heading', 'Section'))}</div>"
                f"{render_markdownish(section.get('body', ''))}"
                "<div class=\"chips\">"
                + "".join(
                    f"<a class=\"chip\" href=\"#evidence-{html.escape(evidence_id)}\">{html.escape(evidence_id)}</a>"
                    for evidence_id in section.get("evidence_ids", [])
                    if evidence_id in evidence_lookup
                )
                + "</div>"
                "</article>"
            )
            for section in final_answer.get("sections", [])
        )
        final_answer_html = (
            "<article class=\"card answer-hero\">"
            f"<div class=\"eyebrow\">Final Answer · {html.escape(manifest.get('status', 'running'))}</div>"
            f"<h2>{html.escape(final_answer.get('title', 'Research Answer'))}</h2>"
            f"{render_markdownish(final_answer.get('summary', ''))}"
            "</article>"
            + answer_sections_html
        )
    else:
        final_answer_html = "<div class=\"empty\">Final answer has not been generated yet.</div>"

    page_title = html.escape(final_answer.get("title", "Research Workbench") if final_answer else "Research Workbench")
    goal_text = html.escape(manifest.get("goal", ""))
    stop_reason = html.escape(manifest.get("stop_reason", "")) or "Still running"
    next_queries_html = "".join(f"<li>{html.escape(item)}</li>" for item in gap_log.get("next_queries", [])) or "<li>None</li>"

    html_doc = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{page_title}</title>
  <style>
    :root {{
      --paper: #f5f1e8;
      --ink: #1c2a28;
      --accent: #ad5e2f;
      --accent-soft: #e8c7a8;
      --line: rgba(28, 42, 40, 0.14);
      --card: rgba(255, 255, 255, 0.76);
      --muted: #5f6b68;
      --ok: #2f6d57;
      --warn: #9d5b1c;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Avenir Next", "Helvetica Neue", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(173, 94, 47, 0.18), transparent 34%),
        linear-gradient(180deg, #f7f3eb 0%, var(--paper) 100%);
    }}
    a {{ color: inherit; }}
    .shell {{
      max-width: 1360px;
      margin: 0 auto;
      padding: 32px 24px 56px;
    }}
    .hero {{
      display: grid;
      grid-template-columns: 1.2fr 0.8fr;
      gap: 18px;
      margin-bottom: 24px;
    }}
    .panel {{
      border: 1px solid var(--line);
      border-radius: 24px;
      padding: 24px;
      background: var(--card);
      backdrop-filter: blur(12px);
      box-shadow: 0 18px 50px rgba(28, 42, 40, 0.08);
    }}
    .eyebrow {{
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.14em;
      color: var(--muted);
      margin-bottom: 10px;
    }}
    h1, h2, h3 {{
      margin: 0 0 10px;
      font-family: "Iowan Old Style", "Palatino Linotype", serif;
      font-weight: 700;
    }}
    h1 {{ font-size: clamp(30px, 4vw, 48px); line-height: 1.02; }}
    h2 {{ font-size: 28px; }}
    h3 {{ font-size: 20px; }}
    p {{
      margin: 0 0 12px;
      line-height: 1.6;
    }}
    ul {{
      margin: 0;
      padding-left: 18px;
      line-height: 1.6;
    }}
    .meta {{
      color: var(--muted);
      font-size: 14px;
    }}
    .status-grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
    }}
    .status-chip {{
      padding: 12px 14px;
      border-radius: 16px;
      border: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.62);
    }}
    .stage-strip {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
      gap: 12px;
      margin-bottom: 24px;
    }}
    .stage-card {{
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 16px;
      background: rgba(255, 255, 255, 0.55);
    }}
    .stage-name {{
      font-weight: 700;
      margin-bottom: 8px;
    }}
    .stage-status {{
      display: inline-block;
      padding: 4px 10px;
      border-radius: 999px;
      background: var(--accent-soft);
      color: var(--ink);
      font-size: 12px;
      margin-bottom: 8px;
    }}
    .stage-status.completed {{ background: rgba(47, 109, 87, 0.16); color: var(--ok); }}
    .stage-status.running {{ background: rgba(173, 94, 47, 0.16); color: var(--warn); }}
    .grid {{
      display: grid;
      grid-template-columns: 0.9fr 1.1fr;
      gap: 18px;
      align-items: start;
    }}
    .stack {{
      display: grid;
      gap: 18px;
    }}
    .card-grid {{
      display: grid;
      gap: 14px;
    }}
    .card {{
      border: 1px solid var(--line);
      border-radius: 20px;
      padding: 18px;
      background: rgba(255, 255, 255, 0.6);
    }}
    .chips {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 12px;
    }}
    .chip {{
      display: inline-flex;
      align-items: center;
      padding: 6px 10px;
      border-radius: 999px;
      text-decoration: none;
      border: 1px solid rgba(173, 94, 47, 0.2);
      background: rgba(173, 94, 47, 0.1);
      font-size: 13px;
    }}
    .section-title {{
      margin-bottom: 12px;
    }}
    .empty {{
      border: 1px dashed var(--line);
      border-radius: 18px;
      padding: 20px;
      color: var(--muted);
      background: rgba(255, 255, 255, 0.4);
    }}
    @media (max-width: 960px) {{
      .hero, .grid {{
        grid-template-columns: 1fr;
      }}
      .shell {{
        padding: 20px 16px 44px;
      }}
    }}
  </style>
</head>
<body>
  <div class="shell">
    <section class="hero">
      <div class="panel">
        <div class="eyebrow">Goal-Driven Research Workbench</div>
        <h1>{goal_text}</h1>
        <p class="meta">Run ID: {html.escape(manifest.get('run_id', ''))}</p>
        <p class="meta">Audience: {html.escape(manifest.get('audience', ''))}</p>
        <p class="meta">Workflow: {html.escape(manifest.get('workflow_mode', 'internal'))} · Mode: {html.escape(manifest.get('mode_resolved', 'objective'))} · Output: {html.escape(manifest.get('output_style', 'research_answer'))}</p>
      </div>
      <div class="panel">
        <div class="eyebrow">Run Status</div>
        <div class="status-grid">
          <div class="status-chip"><strong>Status</strong><br>{html.escape(manifest.get('status', 'running'))}</div>
          <div class="status-chip"><strong>Current Round</strong><br>{html.escape(str(manifest.get('current_round', 0)))}</div>
          <div class="status-chip"><strong>Primary Entity</strong><br>{html.escape(manifest.get('primary_entity', '') or 'Unresolved')}</div>
          <div class="status-chip"><strong>Stop Reason</strong><br>{stop_reason}</div>
        </div>
      </div>
    </section>

    <section class="stage-strip">
      {stage_cards}
    </section>

    <section class="grid">
      <div class="stack">
        <div class="panel">
          <div class="eyebrow">Plan</div>
          <h2 class="section-title">{html.escape(brief.get('research_angle', 'Research Brief'))}</h2>
          <p>{html.escape(brief.get('objective_statement', 'Research brief not generated yet.'))}</p>
          <div class="card-grid">
            <div class="card">
              <div class="eyebrow">Key Questions</div>
              <ul>{question_items}</ul>
            </div>
            <div class="card">
              <div class="eyebrow">Metrics</div>
              <ul>{metric_items}</ul>
            </div>
            <div class="card">
              <div class="eyebrow">Next Queries</div>
              <ul>{next_queries_html}</ul>
            </div>
          </div>
        </div>

        <div class="panel">
          <div class="eyebrow">Local Data</div>
          <h2 class="section-title">{len(data_registry.get('datasets', []))} indexed files</h2>
          <div class="card-grid">{data_cards}</div>
        </div>

        <div class="panel">
          <div class="eyebrow">Sources</div>
          <h2 class="section-title">{len(source_registry.get('sources', []))} sources indexed</h2>
          <div class="card-grid">{source_cards}</div>
        </div>
      </div>

      <div class="stack">
        <div class="panel">
          <div class="eyebrow">Final Answer</div>
          <div class="card-grid">{final_answer_html}</div>
        </div>

        <div class="panel">
          <div class="eyebrow">Evidence</div>
          <h2 class="section-title">{len(evidence_cards.get('evidence_cards', []))} evidence cards</h2>
          <div class="card-grid">{evidence_cards_html}</div>
        </div>

        <div class="panel">
          <div class="eyebrow">Gaps and Challenges</div>
          <h2 class="section-title">{len(gap_log.get('gaps', []))} gaps · {len(gap_log.get('contradictions', []))} contradictions</h2>
          <div class="card-grid">{gap_section_html}</div>
        </div>
      </div>
    </section>
  </div>
</body>
</html>
"""
    (run_dir / "workbench.html").write_text(html_doc, encoding="utf-8")


def run_research_pipeline(args: argparse.Namespace) -> None:
    workflow = resolve_workflow_config(args.workflow)
    goal = resolve_goal(args)
    mode_resolved = resolve_mode(args.mode, goal, args.ticker)
    run_id = resolve_research_run_id(args, goal, args.ticker)
    run_dir = OUTPUT_ROOT / run_id
    manifest_path, manifest = ensure_research_manifest(run_dir, args, goal, mode_resolved, workflow)
    interim_dir = run_dir / "interim"

    data_registry_path = run_dir / "data_registry.json"
    brief_path = run_dir / "research_brief.json"
    source_registry_path = run_dir / "source_registry.json"
    evidence_cards_path = run_dir / "evidence_cards.json"
    gap_log_path = run_dir / "gap_log.json"
    final_answer_json_path = run_dir / "final_answer.json"
    final_answer_md_path = run_dir / "final_answer.md"
    draft_answer_path = run_dir / "final_answer_draft.json"

    prompts_dir = workflow["stage_prompts_dir"]
    schema_dir = ROOT / "schema"

    set_manifest_status(manifest_path, manifest, status="running", stop_reason="")

    data_registry = load_json(data_registry_path, {"datasets": []})
    brief = load_json(brief_path, {})
    source_registry = load_json(source_registry_path, {"sources": []})
    evidence_cards = load_json(evidence_cards_path, {"evidence_cards": []})
    gap_log = load_json(gap_log_path, {"gaps": [], "contradictions": [], "next_queries": []})
    final_answer = load_json(final_answer_json_path, {}) or None
    generate_workbench(run_dir, manifest, brief, data_registry, source_registry, evidence_cards, gap_log, final_answer)

    if args.resume and data_registry_path.exists() and load_json(data_registry_path, {}):
        data_registry = load_json(data_registry_path, {"datasets": []})
        log(f"[DATA] resume from {to_relative(data_registry_path)}")
    else:
        update_manifest_stage(
            manifest_path,
            manifest,
            "local_data",
            status="running",
            path=data_registry_path,
            summary="Indexing precomputed local data from data/.",
        )
        data_registry = build_data_registry(run_dir=run_dir, data_dir=ROOT / "data")
        save_json(data_registry_path, data_registry)
        update_manifest_stage(
            manifest_path,
            manifest,
            "local_data",
            status="completed",
            path=data_registry_path,
            summary="Local data registry ready.",
            meta=data_registry.get("summary", {}),
        )

    local_data_sources = data_sources_from_registry(data_registry)
    source_registry, inserted_local_sources = merge_sources(
        source_registry,
        local_data_sources,
        round_num=0,
    )
    save_json(source_registry_path, source_registry)

    if data_registry.get("summary", {}).get("structured_files", 0) == 0:
        gap_log, _ = merge_gap_entries(
            gap_log,
            [
                {
                    "question": "No structured local datasets were found under data/.",
                    "why_it_matters": "This workflow is designed to use precomputed local data to ground quantitative claims.",
                    "why_unresolved": "Only notes or unsupported file types were indexed.",
                    "suggested_next_step": "Place CSV or JSON datasets in data/ before rerunning the workflow.",
                    "severity": "medium",
                }
            ],
            round_num=0,
        )
        save_json(gap_log_path, gap_log)

    planning_mapping = {
        "<RUN_ID>": run_id,
        "<RUN_DIR>": to_relative(run_dir),
        "<GOAL_PATH>": to_relative(run_dir / "goal.txt"),
        "<NOTES_PATH>": to_relative(run_dir / "notes.txt"),
        "<DATA_REGISTRY_PATH>": to_relative(data_registry_path),
        "<MANIFEST_PATH>": to_relative(manifest_path),
    }

    if args.resume and brief_path.exists() and load_json(brief_path, {}):
        brief = load_json(brief_path, {})
        log(f"[PLAN] resume from {to_relative(brief_path)}")
    else:
        update_manifest_stage(
            manifest_path,
            manifest,
            "planning",
            status="running",
            path=brief_path,
            summary="Building research brief.",
        )
        planning_prompt = render_template(prompts_dir / "research_planning.md", planning_mapping)
        brief = run_codex_exec(
            planning_prompt,
            schema_dir / "research_brief.schema.json",
            brief_path,
            sandbox="read-only",
            model=args.model,
            live_search=False,
            stage_label="planning",
        )
        save_json(brief_path, brief)
        manifest["primary_entity"] = brief.get("primary_entity", manifest.get("primary_entity", ""))
        update_manifest_stage(
            manifest_path,
            manifest,
            "planning",
            status="completed",
            path=brief_path,
            summary=first_non_empty_line(brief.get("objective_statement", "")) or "Research brief ready.",
            meta={"questions": len(brief.get("key_questions", [])), "metrics": len(brief.get("metrics", []))},
        )
        save_json(manifest_path, manifest)

    update_manifest_stage(
        manifest_path,
        manifest,
        "local_data",
        status="completed",
        path=data_registry_path,
        summary="Local data registry ready.",
        meta={
            **data_registry.get("summary", {}),
            "inserted_sources": inserted_local_sources,
        },
    )

    generate_workbench(run_dir, manifest, brief, data_registry, source_registry, evidence_cards, gap_log, final_answer)

    stop_reason = ""
    last_skeptic_path: Path | None = None

    for round_num in range(1, args.rounds + 1):
        manifest["current_round"] = round_num
        save_json(manifest_path, manifest)
        round_mapping = {
            "<RUN_ID>": run_id,
            "<RUN_DIR>": to_relative(run_dir),
            "<ROUND>": str(round_num),
            "<BRIEF_PATH>": to_relative(brief_path),
            "<DATA_REGISTRY_PATH>": to_relative(data_registry_path),
            "<SOURCE_REGISTRY_PATH>": to_relative(source_registry_path),
            "<EVIDENCE_PATH>": to_relative(evidence_cards_path),
            "<GAP_LOG_PATH>": to_relative(gap_log_path),
            "<PREVIOUS_SKEPTIC_PATH>": to_relative(last_skeptic_path) if last_skeptic_path else "(none yet)",
        }

        scout_path = interim_dir / f"source_scout_v{round_num}.json"
        if args.resume and scout_path.exists():
            scout_obj = load_json(scout_path, {})
            log(f"[ROUND {round_num}] source scout resume from {to_relative(scout_path)}")
        else:
            update_manifest_stage(
                manifest_path,
                manifest,
                f"source_scout_v{round_num}",
                status="running",
                path=scout_path,
                summary=f"Round {round_num}: gathering sources.",
            )
            scout_prompt = render_template(prompts_dir / "research_source_scout.md", round_mapping)
            scout_obj = run_codex_exec(
                scout_prompt,
                schema_dir / "sources.schema.json",
                scout_path,
                sandbox="read-only",
                model=args.model,
                live_search=args.live_search,
                stage_label=f"round {round_num} source_scout",
            )

        source_registry, inserted_sources = merge_sources(
            source_registry,
            scout_obj.get("sources_added", []),
            round_num=round_num,
        )
        gap_log, inserted_source_gaps = merge_gap_entries(
            gap_log,
            scout_obj.get("unresolved_gaps", []),
            round_num=round_num,
        )
        save_json(source_registry_path, source_registry)
        save_json(gap_log_path, gap_log)
        update_manifest_stage(
            manifest_path,
            manifest,
            f"source_scout_v{round_num}",
            status="completed",
            path=scout_path,
            summary=f"Added {inserted_sources} new sources.",
            meta={
                "new_sources": inserted_sources,
                "total_sources": len(source_registry.get("sources", [])),
                "new_gaps": inserted_source_gaps,
            },
        )
        generate_workbench(run_dir, manifest, brief, data_registry, source_registry, evidence_cards, gap_log, final_answer)

        evidence_path = interim_dir / f"evidence_v{round_num}.json"
        if args.resume and evidence_path.exists():
            evidence_obj = load_json(evidence_path, {})
            log(f"[ROUND {round_num}] evidence resume from {to_relative(evidence_path)}")
        else:
            update_manifest_stage(
                manifest_path,
                manifest,
                f"evidence_v{round_num}",
                status="running",
                path=evidence_path,
                summary=f"Round {round_num}: extracting evidence cards.",
            )
            evidence_prompt = render_template(prompts_dir / "research_evidence.md", round_mapping)
            evidence_obj = run_codex_exec(
                evidence_prompt,
                schema_dir / "evidence_cards.schema.json",
                evidence_path,
                sandbox="read-only",
                model=args.model,
                live_search=False,
                stage_label=f"round {round_num} evidence",
            )

        evidence_cards, inserted_evidence = merge_evidence_cards(
            evidence_cards,
            evidence_obj.get("evidence_cards", []),
            source_registry,
            round_num=round_num,
        )
        gap_log, inserted_evidence_gaps = merge_gap_entries(
            gap_log,
            evidence_obj.get("unresolved_gaps", []),
            round_num=round_num,
        )
        save_json(evidence_cards_path, evidence_cards)
        save_json(gap_log_path, gap_log)
        update_manifest_stage(
            manifest_path,
            manifest,
            f"evidence_v{round_num}",
            status="completed",
            path=evidence_path,
            summary=f"Added {inserted_evidence} evidence cards.",
            meta={
                "new_evidence": inserted_evidence,
                "total_evidence": len(evidence_cards.get("evidence_cards", [])),
                "new_gaps": inserted_evidence_gaps,
            },
        )
        generate_workbench(run_dir, manifest, brief, data_registry, source_registry, evidence_cards, gap_log, final_answer)

        skeptic_path = interim_dir / f"skeptic_v{round_num}.json"
        if args.resume and skeptic_path.exists():
            skeptic_obj = load_json(skeptic_path, {})
            log(f"[ROUND {round_num}] skeptic resume from {to_relative(skeptic_path)}")
        else:
            update_manifest_stage(
                manifest_path,
                manifest,
                f"skeptic_v{round_num}",
                status="running",
                path=skeptic_path,
                summary=f"Round {round_num}: challenging the evidence set.",
            )
            skeptic_prompt = render_template(prompts_dir / "research_skeptic.md", round_mapping)
            skeptic_obj = run_codex_exec(
                skeptic_prompt,
                schema_dir / "gaps.schema.json",
                skeptic_path,
                sandbox="read-only",
                model=args.model,
                live_search=False,
                stage_label=f"round {round_num} skeptic",
            )

        gap_log, inserted_contradictions = merge_contradictions(
            gap_log,
            skeptic_obj.get("contradictions", []),
            round_num=round_num,
        )
        gap_log, inserted_missing_proof = merge_gap_entries(
            gap_log,
            skeptic_obj.get("missing_proof", []),
            round_num=round_num,
        )
        gap_log = merge_next_queries(gap_log, skeptic_obj.get("next_queries", []))
        save_json(gap_log_path, gap_log)
        continue_research = bool(skeptic_obj.get("continue_research", True))
        stop_reason = normalize_space(skeptic_obj.get("stop_reason", "")) or stop_reason
        update_manifest_stage(
            manifest_path,
            manifest,
            f"skeptic_v{round_num}",
            status="completed",
            path=skeptic_path,
            summary="Continue research." if continue_research else "Skeptic sign-off reached.",
            meta={
                "new_contradictions": inserted_contradictions,
                "new_missing_proof": inserted_missing_proof,
                "continue_research": continue_research,
            },
        )
        last_skeptic_path = skeptic_path
        generate_workbench(run_dir, manifest, brief, data_registry, source_registry, evidence_cards, gap_log, final_answer)

        if not continue_research:
            break

    if not stop_reason:
        stop_reason = (
            "Reached configured round limit."
            if manifest.get("current_round", 0) >= args.rounds
            else "Research stopped after skeptic sign-off."
        )

    synthesis_mapping = {
        "<RUN_ID>": run_id,
        "<RUN_DIR>": to_relative(run_dir),
        "<BRIEF_PATH>": to_relative(brief_path),
        "<DATA_REGISTRY_PATH>": to_relative(data_registry_path),
        "<SOURCE_REGISTRY_PATH>": to_relative(source_registry_path),
        "<EVIDENCE_PATH>": to_relative(evidence_cards_path),
        "<GAP_LOG_PATH>": to_relative(gap_log_path),
        "<MANIFEST_PATH>": to_relative(manifest_path),
        "<DRAFT_ANSWER_PATH>": to_relative(draft_answer_path),
    }

    if args.resume and draft_answer_path.exists() and load_json(draft_answer_path, {}):
        draft_answer = load_json(draft_answer_path, {})
        log(f"[FINAL] synthesis resume from {to_relative(draft_answer_path)}")
    else:
        update_manifest_stage(
            manifest_path,
            manifest,
            "synthesis",
            status="running",
            path=draft_answer_path,
            summary="Building direct research answer.",
        )
        synthesis_prompt = render_template(prompts_dir / "research_synthesis.md", synthesis_mapping)
        draft_answer = run_codex_exec(
            synthesis_prompt,
            schema_dir / "final_answer.schema.json",
            draft_answer_path,
            sandbox="read-only",
            model=args.model,
            live_search=False,
            stage_label="final synthesis",
        )
        update_manifest_stage(
            manifest_path,
            manifest,
            "synthesis",
            status="completed",
            path=draft_answer_path,
            summary=first_non_empty_line(draft_answer.get("summary", "")) or "Draft answer ready.",
            meta={"sections": len(draft_answer.get("sections", []))},
        )

    if args.resume and final_answer_json_path.exists() and load_json(final_answer_json_path, {}):
        final_answer = load_json(final_answer_json_path, {})
        log(f"[FINAL] polish resume from {to_relative(final_answer_json_path)}")
    else:
        update_manifest_stage(
            manifest_path,
            manifest,
            "polish",
            status="running",
            path=final_answer_json_path,
            summary="Polishing final research answer.",
        )
        polish_prompt = render_template(prompts_dir / "research_polish.md", synthesis_mapping)
        final_answer = run_codex_exec(
            polish_prompt,
            schema_dir / "final_answer.schema.json",
            final_answer_json_path,
            sandbox="read-only",
            model=args.model,
            live_search=False,
            stage_label="final polish",
        )
        save_json(final_answer_json_path, final_answer)
        final_answer_md_path.write_text(render_final_answer_markdown(final_answer), encoding="utf-8")
        update_manifest_stage(
            manifest_path,
            manifest,
            "polish",
            status="completed",
            path=final_answer_json_path,
            summary=first_non_empty_line(final_answer.get("summary", "")) or "Final answer ready.",
            meta={"sections": len(final_answer.get("sections", []))},
        )

    final_answer_md_path.write_text(render_final_answer_markdown(final_answer), encoding="utf-8")
    set_manifest_status(manifest_path, manifest, status="completed", stop_reason=stop_reason)
    generate_workbench(run_dir, manifest, brief, data_registry, source_registry, evidence_cards, gap_log, final_answer)
    print(f"[OK] Finished. See: {run_dir}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "workflow",
        nargs="?",
        choices=WORKFLOW_CHOICES,
        default="internal",
        help="workflow/report mode: internal or external",
    )
    parser.add_argument("--goal", help="research objective in plain language")
    parser.add_argument("--ticker", help="optional primary asset hint, e.g. ETH or HYPE")
    parser.add_argument("--mode", choices=["auto", "objective", "ticker"], default="auto")
    parser.add_argument("--audience", default="internal_strategy_team")
    parser.add_argument("--model", default="gpt-5.4", help="override parent orchestration model")
    parser.add_argument("--live-search", action="store_true", help="enable live web search where supported")
    parser.add_argument("--resume", action="store_true", help="resume from the last or specified run")
    parser.add_argument("--run-id", help="resume or force a specific run id for research mode")
    parser.add_argument("--rounds", type=int, default=3, help="max research rounds")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if args.rounds < 1:
        raise SystemExit("--rounds must be >= 1")

    try:
        run_research_pipeline(args)
    except Exception as exc:
        run_id = args.run_id or load_last_run_id()
        if run_id:
            manifest_path = OUTPUT_ROOT / run_id / "run_manifest.json"
            manifest = load_json(manifest_path, {})
            if manifest:
                set_manifest_status(manifest_path, manifest, status="failed", stop_reason=str(exc))
        raise


if __name__ == "__main__":
    main()
