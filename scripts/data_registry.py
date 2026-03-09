#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATA_DIR = ROOT / "data"
SKIP_FILENAMES = {"data_registry.json"}
STRUCTURED_SOURCE_SUFFIXES = {".csv", ".tsv", ".json"}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def to_relative(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def relative_from(base: Path, target: Path) -> str:
    return os.path.relpath(target, start=base)


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


def titleize_filename(path: Path) -> str:
    return re.sub(r"[_-]+", " ", path.stem).strip().title() or path.name


def infer_role(path: Path) -> str:
    name = path.stem.lower()
    if name.endswith("_summary"):
        return "summary"
    if "detail" in name:
        return "detail"
    if "aggregated" in name:
        return "aggregate"
    if "pivot" in name:
        return "timeseries"
    if "sector" in name or "category" in name:
        return "segmentation"
    if path.suffix.lower() in {".txt", ".md"}:
        return "notes"
    return "dataset"


def classify_analysis_priority(path: Path, role: str) -> str:
    if role == "summary":
        return "high"
    if path.suffix.lower() in STRUCTURED_SOURCE_SUFFIXES:
        return "medium"
    return "low"


def infer_usage_hints(path: Path, role: str, columns: list[str], top_keys: list[str]) -> list[str]:
    hints: list[str] = []
    lowered_name = path.stem.lower()
    lowered_columns = {item.lower() for item in columns}
    lowered_keys = {item.lower() for item in top_keys}

    if role == "summary":
        hints.append("Read this first to understand date coverage, headline takeaways, and which companion files matter.")
    if role == "detail":
        hints.append("Use this for asset-by-date drilldowns and custom aggregation.")
    if role == "aggregate":
        hints.append("Use this for ranked comparisons across assets, categories, or time windows.")
    if role == "timeseries":
        hints.append("Use this for trend direction, acceleration, and cross-asset time-series comparisons.")
    if role == "segmentation":
        hints.append("Use this for category concentration, breadth, and sector mix analysis.")
    if role == "notes":
        hints.append("Use this only as analyst context, not as external evidence.")

    if {"date", "asset", "category"} & lowered_columns:
        hints.append("This file can support growth, richness, and concentration analysis.")
    if {"headline_takeaways", "date_range", "top_assets_latest_1d"} & lowered_keys:
        hints.append("This file can quickly anchor quantitative claims before deeper file inspection.")
    if "volume" in lowered_name or any("volume" in item for item in lowered_columns):
        hints.append("This file is relevant for demand proxies such as trading activity and growth.")

    return unique_strings(hints, limit=4)


def compact_row(row: dict[str, Any], *, limit: int = 6) -> dict[str, str]:
    out: dict[str, str] = {}
    for key, value in row.items():
        if len(out) >= limit:
            break
        key_clean = normalize_space(str(key))
        value_clean = normalize_space(str(value))
        if not key_clean or not value_clean:
            continue
        out[key_clean] = value_clean[:120]
    return out


def looks_like_date(value: str) -> bool:
    value = normalize_space(value)
    if not value:
        return False
    return bool(
        re.fullmatch(r"\d{4}-\d{2}-\d{2}", value)
        or re.fullmatch(r"\d{4}-\d{2}-\d{2}T[^ ]+", value)
    )


def summarize_csv(path: Path, delimiter: str) -> dict[str, Any]:
    columns: list[str] = []
    row_count = 0
    sample_rows: list[dict[str, str]] = []
    date_values: list[str] = []

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle, delimiter=delimiter)
        columns = [normalize_space(item) for item in (reader.fieldnames or []) if normalize_space(item)]
        for row in reader:
            row_count += 1
            if len(sample_rows) < 2:
                sample_rows.append(compact_row(row))
            for key, value in row.items():
                if key and value and normalize_space(key).lower() in {"date", "day", "timestamp"}:
                    normalized = normalize_space(str(value))
                    if looks_like_date(normalized):
                        date_values.append(normalized[:10])

    date_range = None
    if date_values:
        date_range = {
            "start": min(date_values),
            "end": max(date_values),
        }

    return {
        "columns": columns[:32],
        "row_count": row_count,
        "sample_rows": sample_rows,
        "date_range": date_range,
    }


def summarize_json(path: Path, data_dir: Path) -> dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    summary: dict[str, Any] = {
        "top_level_type": type(obj).__name__,
        "top_keys": [],
        "headline_takeaways": [],
        "sample_items": [],
        "date_range": None,
        "related_paths": [],
    }

    if isinstance(obj, dict):
        summary["top_keys"] = [str(key) for key in list(obj.keys())[:24]]
        summary["headline_takeaways"] = unique_strings(obj.get("headline_takeaways", []), limit=6)
        if isinstance(obj.get("date_range"), dict):
            start = normalize_space(str(obj["date_range"].get("start", "")))
            end = normalize_space(str(obj["date_range"].get("end", "")))
            days = obj["date_range"].get("days")
            if start or end:
                summary["date_range"] = {"start": start, "end": end, "days": days}

        related_paths: list[str] = []
        for item in obj.get("artifacts", []):
            raw_path = normalize_space(str(item.get("path", "")))
            if not raw_path:
                continue
            candidate = Path(raw_path)
            if not candidate.exists():
                candidate = data_dir / Path(raw_path).name
            if candidate.exists():
                related_paths.append(to_relative(candidate))
        summary["related_paths"] = unique_strings(related_paths, limit=12)

        sample_items: list[dict[str, str]] = []
        for value in obj.values():
            if isinstance(value, list) and value and isinstance(value[0], dict):
                for item in value[:2]:
                    sample_items.append(compact_row(item))
                break
        summary["sample_items"] = sample_items[:2]
    elif isinstance(obj, list):
        summary["sample_items"] = [
            compact_row(item) if isinstance(item, dict) else {"value": normalize_space(str(item))[:120]}
            for item in obj[:2]
        ]

    return summary


def summarize_text(path: Path) -> dict[str, Any]:
    lines = path.read_text(encoding="utf-8").splitlines()
    sample_lines = [normalize_space(line)[:160] for line in lines if normalize_space(line)]
    return {"sample_lines": sample_lines[:3]}


def build_dataset_entry(path: Path, run_dir: Path | None = None, data_dir: Path | None = None) -> dict[str, Any]:
    data_dir = data_dir or DEFAULT_DATA_DIR
    role = infer_role(path)
    suffix = path.suffix.lower()
    modified_at = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat().replace("+00:00", "Z")
    columns: list[str] = []
    top_keys: list[str] = []
    row_count: int | None = None
    sample_rows: list[dict[str, str]] = []
    sample_items: list[dict[str, str]] = []
    sample_lines: list[str] = []
    date_range: dict[str, Any] | None = None
    headline_takeaways: list[str] = []
    related_paths: list[str] = []

    if suffix == ".csv":
        csv_summary = summarize_csv(path, ",")
        columns = csv_summary["columns"]
        row_count = csv_summary["row_count"]
        sample_rows = csv_summary["sample_rows"]
        date_range = csv_summary["date_range"]
    elif suffix == ".tsv":
        csv_summary = summarize_csv(path, "\t")
        columns = csv_summary["columns"]
        row_count = csv_summary["row_count"]
        sample_rows = csv_summary["sample_rows"]
        date_range = csv_summary["date_range"]
    elif suffix == ".json":
        json_summary = summarize_json(path, data_dir)
        top_keys = json_summary["top_keys"]
        sample_items = json_summary["sample_items"]
        headline_takeaways = json_summary["headline_takeaways"]
        date_range = json_summary["date_range"]
        related_paths = json_summary["related_paths"]
    else:
        text_summary = summarize_text(path)
        sample_lines = text_summary["sample_lines"]

    usage_hints = infer_usage_hints(path, role, columns, top_keys)
    description_parts: list[str] = []
    if role == "summary":
        description_parts.append("High-level summary dataset.")
    elif role == "detail":
        description_parts.append("Granular dataset for drilldowns.")
    elif role == "aggregate":
        description_parts.append("Pre-aggregated comparison dataset.")
    elif role == "timeseries":
        description_parts.append("Time-series comparison dataset.")
    elif role == "segmentation":
        description_parts.append("Category segmentation dataset.")
    elif role == "notes":
        description_parts.append("Local analyst notes.")
    else:
        description_parts.append("Local dataset.")

    if row_count is not None:
        description_parts.append(f"{row_count} rows.")
    if columns:
        description_parts.append(f"Columns: {', '.join(columns[:6])}.")
    if top_keys:
        description_parts.append(f"Top keys: {', '.join(top_keys[:6])}.")

    relative_path = to_relative(path)
    entry = {
        "name": path.name,
        "title": titleize_filename(path),
        "path": relative_path,
        "workbench_path": relative_from(run_dir, path) if run_dir else relative_path,
        "kind": suffix.lstrip(".") or "file",
        "role": role,
        "analysis_priority": classify_analysis_priority(path, role),
        "modified_at": modified_at,
        "size_bytes": path.stat().st_size,
        "description": " ".join(description_parts).strip(),
        "usage_hints": usage_hints,
        "columns": columns[:32],
        "row_count": row_count,
        "top_keys": top_keys[:24],
        "date_range": date_range,
        "headline_takeaways": headline_takeaways,
        "sample_rows": sample_rows,
        "sample_items": sample_items,
        "sample_lines": sample_lines,
        "related_paths": related_paths,
        "index_as_source": suffix in STRUCTURED_SOURCE_SUFFIXES and role != "notes",
        "citation_hint": usage_hints[0] if usage_hints else "Use this file when it directly supports a quantitative or structural claim.",
    }
    return entry


def build_data_registry(run_dir: Path | None = None, data_dir: Path | None = None) -> dict[str, Any]:
    data_dir = (data_dir or DEFAULT_DATA_DIR).resolve()
    datasets: list[dict[str, Any]] = []

    if data_dir.exists():
        for path in sorted(item for item in data_dir.rglob("*") if item.is_file()):
            if path.name.startswith(".") or path.name in SKIP_FILENAMES or path.name.startswith("data_registry"):
                continue
            datasets.append(build_dataset_entry(path, run_dir=run_dir, data_dir=data_dir))

    for index, item in enumerate(datasets, start=1):
        item["id"] = f"D{index:03d}"

    summary = {
        "total_files": len(datasets),
        "structured_files": sum(1 for item in datasets if item.get("index_as_source")),
        "csv_files": sum(1 for item in datasets if item.get("kind") == "csv"),
        "json_files": sum(1 for item in datasets if item.get("kind") == "json"),
        "text_files": sum(1 for item in datasets if item.get("kind") in {"txt", "md"}),
    }

    return {
        "generated_at": utc_now_iso(),
        "data_root": to_relative(data_dir),
        "summary": summary,
        "datasets": datasets,
    }


def data_sources_from_registry(registry: dict[str, Any]) -> list[dict[str, Any]]:
    sources: list[dict[str, Any]] = []
    for dataset in registry.get("datasets", []):
        if not dataset.get("index_as_source"):
            continue
        takeaways = unique_strings(
            list(dataset.get("headline_takeaways", []))
            + list(dataset.get("usage_hints", []))
            + (
                [f"Rows: {dataset['row_count']}"]
                if isinstance(dataset.get("row_count"), int)
                else []
            )
            + (
                [f"Columns: {', '.join(dataset.get('columns', [])[:6])}"]
                if dataset.get("columns")
                else []
            ),
            limit=8,
        )
        sources.append(
            {
                "title": f"Local data: {dataset.get('title', dataset.get('name', 'Dataset'))}",
                "url": dataset.get("path", ""),
                "workbench_url": dataset.get("workbench_path", ""),
                "type": "Data",
                "updated_at": dataset.get("modified_at", ""),
                "relevance": dataset.get("description", ""),
                "takeaways": takeaways,
            }
        )
    return sources
