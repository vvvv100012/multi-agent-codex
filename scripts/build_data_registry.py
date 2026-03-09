#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from data_registry import build_data_registry


ROOT = Path(__file__).resolve().parents[1]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default=str(ROOT / "data"), help="directory containing precomputed local data")
    parser.add_argument("--output", help="optional path to write the resulting data registry JSON")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    registry = build_data_registry(data_dir=Path(args.data_dir))
    rendered = json.dumps(registry, ensure_ascii=False, indent=2)
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered, encoding="utf-8")
    else:
        sys.stdout.write(rendered + "\n")


if __name__ == "__main__":
    main()
