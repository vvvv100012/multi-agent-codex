#!/usr/bin/env python3
import argparse
import csv
import json
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path

import requests

API_URL = "https://api.hyperliquid.xyz/info"
DEFAULT_START_DATE = "2025-01-01"
DEXES_TO_QUERY = ["xyz", "flx", "km", "cash", "vntl"]

CATEGORIES = {
    "Commodities": {
        "SILVER": "白银 (Silver)",
        "GOLD": "黄金 (Gold)",
        "COPPER": "铜 (Copper)",
        "PLATINUM": "铂金 (Platinum)",
        "CL": "原油 (Crude Oil)",
        "NATGAS": "天然气 (Natural Gas)",
    },
    "Stocks": {
        "NVDA": "英伟达 (NVIDIA)",
        "TSLA": "特斯拉 (Tesla)",
        "MSFT": "微软 (Microsoft)",
        "AMZN": "亚马逊 (Amazon)",
        "GOOGL": "谷歌 (Alphabet)",
        "META": "脸书 (Meta)",
        "AAPL": "苹果 (Apple)",
        "INTC": "英特尔 (Intel)",
        "AMD": "超威半导体 (AMD)",
        "MU": "美光科技 (Micron)",
        "ORCL": "甲骨文 (Oracle)",
        "NFLX": "网飞 (Netflix)",
        "PLTR": "Palantir",
        "HOOD": "罗宾汉 (Robinhood)",
        "COIN": "Coinbase",
        "MSTR": "MicroStrategy",
        "RIVN": "Rivian",
        "SNDK": "闪迪 (SanDisk)",
        "URNM": "铀矿ETF (URNM)",
    },
    "Indices": {
        "USA500": "标普500 (S&P 500)",
        "USTECH": "纳斯达克科技指数 (Nasdaq Tech)",
        "MAG7": "七巨头指数 (Magnificent 7)",
    },
    "Forex": {
        "EUR": "欧元/美元 (EUR/USD)",
        "JPY": "美元/日元 (USD/JPY)",
    },
}

SYMBOL_ALIASES = {
    "US500": "USA500",
    "USDJPY": "JPY",
}

ALL_TARGET_SYMBOLS = set()
for assets in CATEGORIES.values():
    ALL_TARGET_SYMBOLS.update(assets.keys())

SYMBOL_INFO = {}
for category, assets in CATEGORIES.items():
    for symbol, display_name in assets.items():
        SYMBOL_INFO[symbol] = (category, display_name)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def resolve_symbol(symbol: str) -> str:
    return SYMBOL_ALIASES.get(symbol, symbol)


def discover_dex_target_symbols(dex_name: str) -> list[tuple[str, str]]:
    payload = {"type": "metaAndAssetCtxs", "dex": dex_name}
    response = requests.post(API_URL, json=payload, timeout=15)
    response.raise_for_status()
    data = response.json()

    universe = data[0]["universe"]
    found = []
    for asset in universe:
        raw_name = asset["name"]
        base = raw_name.split(":")[-1] if ":" in raw_name else raw_name
        canonical = resolve_symbol(base)
        if canonical in ALL_TARGET_SYMBOLS:
            found.append((raw_name, canonical))
    return found


def fetch_candles(coin: str, start_ms: int, end_ms: int) -> list[dict]:
    payload = {
        "type": "candleSnapshot",
        "req": {
            "coin": coin,
            "interval": "1d",
            "startTime": start_ms,
            "endTime": end_ms,
        },
    }
    response = requests.post(API_URL, json=payload, timeout=15)
    response.raise_for_status()
    return response.json()


def write_csv(path: Path, headers: list[str], rows: list[list]) -> None:
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.writer(handle)
        writer.writerow(headers)
        writer.writerows(rows)


def write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def apply_ma7(headers: list[str], grid: list[list]) -> list[list]:
    num_cols = len(headers)
    windows = [deque(maxlen=7) for _ in range(num_cols)]
    result = []
    for row in grid:
        ma_row = [row[0]]
        for index in range(1, num_cols):
            value = row[index] if index < len(row) else ""
            if value != "":
                windows[index].append(float(value))
            if windows[index]:
                ma_row.append(round(sum(windows[index]) / len(windows[index]), 2))
            else:
                ma_row.append("")
        result.append(ma_row)
    return result


def parse_date_to_ms(date_str: str) -> int:
    parsed = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


def rounded_top_items(rows: list[dict], *, limit: int = 8) -> list[dict]:
    return [
        {
            key: (round(value, 2) if isinstance(value, float) else value)
            for key, value in item.items()
        }
        for item in rows[:limit]
    ]


def build_summary(
    output_dir: Path,
    dex_symbols: dict[str, list[tuple[str, str]]],
    all_dates: list[str],
    daily_data: dict,
) -> dict:
    date_asset_totals: dict[tuple[str, str], float] = {}
    date_category_totals: dict[tuple[str, str], float] = defaultdict(float)
    asset_total_volumes: dict[str, float] = defaultdict(float)

    for (date_str, canonical), dex_volumes in daily_data.items():
        total_volume = sum(dex_volumes.values())
        date_asset_totals[(date_str, canonical)] = total_volume
        asset_total_volumes[canonical] += total_volume
        category, _display_name = SYMBOL_INFO[canonical]
        date_category_totals[(date_str, category)] += total_volume

    latest_date = all_dates[-1] if all_dates else ""
    trailing_dates_7 = set(all_dates[-7:])
    trailing_dates_30 = set(all_dates[-30:])

    asset_rows = []
    for canonical in sorted(ALL_TARGET_SYMBOLS):
        category, display_name = SYMBOL_INFO[canonical]
        latest_1d = date_asset_totals.get((latest_date, canonical), 0.0) if latest_date else 0.0
        last_7d = sum(
            volume
            for (date_str, symbol), volume in date_asset_totals.items()
            if symbol == canonical and date_str in trailing_dates_7
        )
        last_30d = sum(
            volume
            for (date_str, symbol), volume in date_asset_totals.items()
            if symbol == canonical and date_str in trailing_dates_30
        )
        active_days = sum(1 for (date_str, symbol) in date_asset_totals if symbol == canonical)
        asset_rows.append(
            {
                "asset": canonical,
                "category": category,
                "display_name": display_name,
                "latest_1d_volume_usd": latest_1d,
                "last_7d_volume_usd": last_7d,
                "last_30d_volume_usd": last_30d,
                "since_start_volume_usd": asset_total_volumes.get(canonical, 0.0),
                "active_days": active_days,
            }
        )

    asset_rows.sort(key=lambda item: item["since_start_volume_usd"], reverse=True)

    category_rows = []
    for category in CATEGORIES:
        latest_1d = date_category_totals.get((latest_date, category), 0.0) if latest_date else 0.0
        last_30d = sum(
            volume
            for (date_str, group), volume in date_category_totals.items()
            if group == category and date_str in trailing_dates_30
        )
        since_start = sum(
            volume for (_date_str, group), volume in date_category_totals.items() if group == category
        )
        category_rows.append(
            {
                "category": category,
                "latest_1d_volume_usd": latest_1d,
                "last_30d_volume_usd": last_30d,
                "since_start_volume_usd": since_start,
            }
        )
    category_rows.sort(key=lambda item: item["since_start_volume_usd"], reverse=True)

    headline_takeaways = []
    if latest_date:
        top_assets = rounded_top_items(
            sorted(asset_rows, key=lambda item: item["latest_1d_volume_usd"], reverse=True),
            limit=3,
        )
        if top_assets:
            top_names = ", ".join(
                f"{item['asset']} (${item['latest_1d_volume_usd']:.0f})" for item in top_assets if item["latest_1d_volume_usd"] > 0
            )
            if top_names:
                headline_takeaways.append(f"On {latest_date}, the largest single-day volumes were in {top_names}.")

        top_categories = rounded_top_items(
            sorted(category_rows, key=lambda item: item["last_30d_volume_usd"], reverse=True),
            limit=3,
        )
        if top_categories:
            category_text = ", ".join(
                f"{item['category']} (${item['last_30d_volume_usd']:.0f})"
                for item in top_categories
                if item["last_30d_volume_usd"] > 0
            )
            if category_text:
                headline_takeaways.append(f"Over the last 30 days, volume was concentrated in {category_text}.")

    summary = {
        "collector_id": "rwa_volume",
        "collector_name": "Hyperliquid RWA Volume",
        "generated_at": utc_now_iso(),
        "api_url": API_URL,
        "date_range": {
            "start": all_dates[0] if all_dates else "",
            "end": latest_date,
            "days": len(all_dates),
        },
        "dexes_queried": DEXES_TO_QUERY,
        "discovered_pairs": [
            {"dex": dex, "count": len(symbols), "symbols": [full_name for full_name, _canonical in symbols]}
            for dex, symbols in dex_symbols.items()
        ],
        "headline_takeaways": headline_takeaways,
        "top_assets_latest_1d": rounded_top_items(
            sorted(asset_rows, key=lambda item: item["latest_1d_volume_usd"], reverse=True),
            limit=10,
        ),
        "top_assets_last_30d": rounded_top_items(
            sorted(asset_rows, key=lambda item: item["last_30d_volume_usd"], reverse=True),
            limit=10,
        ),
        "top_assets_since_start": rounded_top_items(asset_rows, limit=10),
        "category_latest_1d": rounded_top_items(
            sorted(category_rows, key=lambda item: item["latest_1d_volume_usd"], reverse=True),
            limit=10,
        ),
        "category_last_30d": rounded_top_items(
            sorted(category_rows, key=lambda item: item["last_30d_volume_usd"], reverse=True),
            limit=10,
        ),
        "artifacts": [
            {"label": "detail_csv", "path": str(output_dir / "data/rwa_volume_detail.csv")},
            {"label": "aggregated_csv", "path": str(output_dir / "data/rwa_volume_aggregated.csv")},
            {"label": "pivot_csv", "path": str(output_dir / "data/rwa_volume_pivot.csv")},
            {"label": "pivot_ma7_csv", "path": str(output_dir / "data/rwa_volume_pivot_ma7.csv")},
            {"label": "sector_csv", "path": str(output_dir / "data/rwa_volume_sector.csv")},
            {"label": "sector_ma7_csv", "path": str(output_dir / "data/rwa_volume_sector_ma7.csv")},
            {"label": "summary_json", "path": str(output_dir / "data/rwa_volume_summary.json")},
        ],
    }
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output-dir",
        default=".",
        help="directory for CSV and JSON outputs",
    )
    parser.add_argument(
        "--start-date",
        default=DEFAULT_START_DATE,
        help="UTC start date in YYYY-MM-DD format",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    now_ms = int(time.time() * 1000)
    start_ms = parse_date_to_ms(args.start_date)

    print("=" * 60)
    print("  Hyperliquid RWA 历史每日交易量采集")
    print("=" * 60)
    print(f"  输出目录: {output_dir}")

    print("\n[1/3] 扫描各 Dex 上的目标资产...")
    dex_symbols = {}
    for dex in DEXES_TO_QUERY:
        try:
            symbols = discover_dex_target_symbols(dex)
            dex_symbols[dex] = symbols
            print(f"  {dex}: {len(symbols)} 个目标资产")
        except Exception as exc:
            print(f"  {dex}: 获取失败 - {exc}")

    total_pairs = sum(len(values) for values in dex_symbols.values())
    print(f"  共计 {total_pairs} 个交易对需要获取")

    daily_data = defaultdict(lambda: defaultdict(float))

    print("\n[2/3] 获取历史 K 线数据...")
    fetched = 0
    for dex, symbols in dex_symbols.items():
        for full_name, canonical in symbols:
            fetched += 1
            print(f"  [{fetched}/{total_pairs}] {full_name}...", end=" ")
            try:
                candles = fetch_candles(full_name, start_ms, now_ms)
                count = 0
                for candle in candles:
                    date_str = datetime.fromtimestamp(
                        candle["t"] / 1000, tz=timezone.utc
                    ).strftime("%Y-%m-%d")
                    base_volume = float(candle["v"])
                    close_price = float(candle["c"])
                    notional_volume = base_volume * close_price
                    daily_data[(date_str, canonical)][full_name] += notional_volume
                    count += 1
                print(f"{count} 天")
            except Exception as exc:
                print(f"失败 - {exc}")
            time.sleep(0.15)

    print("\n[3/3] 生成输出文件...")
    all_dates = sorted({key[0] for key in daily_data})
    all_canonical = sorted(ALL_TARGET_SYMBOLS)

    detail_rows = []
    for (date_str, canonical), dex_volumes in sorted(daily_data.items()):
        category, display_name = SYMBOL_INFO[canonical]
        for dex_coin, volume in sorted(dex_volumes.items(), key=lambda item: -item[1]):
            detail_rows.append(
                {
                    "date": date_str,
                    "category": category,
                    "asset": canonical,
                    "display_name": display_name,
                    "dex_symbol": dex_coin,
                    "volume_usd": round(volume, 2),
                }
            )

    detail_file = output_dir / "rwa_volume_detail.csv"
    detail_fields = ["date", "category", "asset", "display_name", "dex_symbol", "volume_usd"]
    with detail_file.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=detail_fields)
        writer.writeheader()
        writer.writerows(detail_rows)
    print(f"  明细数据: {detail_file} ({len(detail_rows)} 行)")

    aggregated_rows = []
    for date_str in all_dates:
        for canonical in all_canonical:
            dex_volumes = daily_data.get((date_str, canonical))
            if not dex_volumes:
                continue
            category, display_name = SYMBOL_INFO[canonical]
            total_volume = sum(dex_volumes.values())
            dex_breakdown = ", ".join(
                f"{coin}={volume:.0f}"
                for coin, volume in sorted(dex_volumes.items(), key=lambda item: -item[1])
            )
            aggregated_rows.append(
                {
                    "date": date_str,
                    "category": category,
                    "asset": canonical,
                    "display_name": display_name,
                    "total_volume_usd": round(total_volume, 2),
                    "dex_breakdown": dex_breakdown,
                }
            )

    aggregated_file = output_dir / "rwa_volume_aggregated.csv"
    aggregated_fields = ["date", "category", "asset", "display_name", "total_volume_usd", "dex_breakdown"]
    with aggregated_file.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=aggregated_fields)
        writer.writeheader()
        writer.writerows(aggregated_rows)
    print(f"  汇总数据: {aggregated_file} ({len(aggregated_rows)} 行)")

    pivot_file = output_dir / "rwa_volume_pivot.csv"
    pivot_headers = ["date"] + list(all_canonical)
    pivot_grid = []
    for date_str in all_dates:
        row = [date_str]
        for symbol in all_canonical:
            dex_volumes = daily_data.get((date_str, symbol))
            if dex_volumes:
                row.append(round(sum(dex_volumes.values()), 2))
            else:
                row.append("")
        pivot_grid.append(row)
    write_csv(pivot_file, pivot_headers, pivot_grid)
    print(f"  透视表:       {pivot_file} ({len(all_dates)} 天 x {len(all_canonical)} 资产)")

    pivot_ma7_file = output_dir / "rwa_volume_pivot_ma7.csv"
    write_csv(pivot_ma7_file, pivot_headers, apply_ma7(pivot_headers, pivot_grid))
    print(f"  透视表 MA7:   {pivot_ma7_file}")

    sector_map = {
        "Commodities": "Commodities",
        "Stocks": "Stocks & Indices",
        "Indices": "Stocks & Indices",
        "Forex": "Forex",
    }
    sector_names = ["Commodities", "Stocks & Indices", "Forex"]
    sector_headers = ["date"] + sector_names + ["Total"]
    sector_grid = []
    for date_str in all_dates:
        sector_totals = defaultdict(float)
        for canonical in all_canonical:
            dex_volumes = daily_data.get((date_str, canonical))
            if not dex_volumes:
                continue
            category, _display_name = SYMBOL_INFO[canonical]
            sector = sector_map[category]
            sector_totals[sector] += sum(dex_volumes.values())
        row = [date_str]
        grand_total = 0.0
        for sector in sector_names:
            value = round(sector_totals.get(sector, 0.0), 2)
            row.append(value if value > 0 else "")
            grand_total += value
        row.append(round(grand_total, 2) if grand_total > 0 else "")
        sector_grid.append(row)
    sector_file = output_dir / "rwa_volume_sector.csv"
    write_csv(sector_file, sector_headers, sector_grid)
    print(f"  板块汇总:     {sector_file}")

    sector_ma7_file = output_dir / "rwa_volume_sector_ma7.csv"
    write_csv(sector_ma7_file, sector_headers, apply_ma7(sector_headers, sector_grid))
    print(f"  板块汇总 MA7: {sector_ma7_file}")

    summary = build_summary(output_dir, dex_symbols, all_dates, daily_data)
    summary_path = output_dir / "rwa_volume_summary.json"
    write_json(summary_path, summary)
    print(f"  摘要 JSON:    {summary_path}")

    if all_dates:
        print(f"\n{'=' * 60}")
        print(f"  完成! 数据范围: {all_dates[0]} ~ {all_dates[-1]}")
        print(f"  共 {len(all_dates)} 天, {len(ALL_TARGET_SYMBOLS)} 个资产")
        print(f"{'=' * 60}")
    else:
        print(f"\n{'=' * 60}")
        print("  完成，但没有返回任何符合条件的历史数据。")
        print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
