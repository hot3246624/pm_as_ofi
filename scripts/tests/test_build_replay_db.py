import importlib.util
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path


def load_build_replay_module():
    module_path = Path(__file__).resolve().parents[1] / "build_replay_db.py"
    spec = importlib.util.spec_from_file_location("build_replay_db", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


build_replay_db = load_build_replay_module()


def write_jsonl(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def envelope(
    stream: str,
    payload: dict,
    *,
    slug: str = "btc-updown-5m-test",
    capture_seq: int = 1,
    recv_unix_ms: int | None = None,
):
    return {
        "capture_seq": capture_seq,
        "recv_unix_ms": recv_unix_ms if recv_unix_ms is not None else 1_746_000_000_000 + capture_seq,
        "recv_monotonic_ns": capture_seq * 10,
        "stream": stream,
        "slug": slug,
        "condition_id": "0xcond",
        "market_id": "0xmarket",
        "strategy": "pair_arb",
        "dry_run": True,
        "payload": payload,
    }


class BuildReplayDbTests(unittest.TestCase):
    def test_build_for_slug_prefers_structured_market_md(self):
        with tempfile.TemporaryDirectory(prefix="pm_as_ofi_replay_") as tmp:
            root = Path(tmp)
            day = "2026-04-26"
            slug_dir = root / day / "btc-updown-5m-structured"
            write_jsonl(
                slug_dir / "meta.jsonl",
                [
                    envelope(
                        "meta",
                        {
                            "event": "session_start",
                            "round_start_ts": 1,
                            "round_end_ts": 2,
                            "yes_asset_id": "yes-asset",
                            "no_asset_id": "no-asset",
                        },
                    )
                ],
            )
            write_jsonl(
                slug_dir / "market_md.jsonl",
                [
                    envelope(
                        "market_md",
                        {
                            "kind": "book_l1",
                            "yes_bid": 0.48,
                            "yes_ask": 0.52,
                            "no_bid": 0.47,
                            "no_ask": 0.53,
                        },
                        capture_seq=2,
                    ),
                    envelope(
                        "market_md",
                        {
                            "kind": "trade",
                            "asset_id": "yes-asset",
                            "market_side": "YES",
                            "taker_side": "BUY",
                            "price": 0.51,
                            "size": 7.0,
                            "trade_id": "",
                            "trade_ts_ms": 1_746_000_000_003,
                        },
                        capture_seq=3,
                        recv_unix_ms=1_746_000_000_003,
                    ),
                    envelope(
                        "market_md",
                        {
                            "kind": "trade",
                            "asset_id": "yes-asset",
                            "market_side": "YES",
                            "taker_side": "BUY",
                            "price": 0.51,
                            "size": 7.0,
                            "trade_id": "",
                            "trade_ts_ms": 1_746_000_000_003,
                        },
                        capture_seq=4,
                        recv_unix_ms=1_746_000_000_003,
                    ),
                ],
            )
            write_jsonl(
                slug_dir / "market_ws.jsonl",
                [
                    envelope(
                        "market_ws",
                        {"raw_text": json.dumps({"event_type": "trade", "price": 0.99, "size": 1, "side": "SELL"})},
                        capture_seq=5,
                    )
                ],
            )

            conn = sqlite3.connect(":memory:")
            try:
                build_replay_db.init_db(conn)
                build_replay_db.build_for_slug(conn, day, slug_dir)
                conn.commit()

                book_rows = conn.execute(
                    "SELECT side, bid, ask, bid_sz, ask_sz, source FROM md_book_l1 ORDER BY side"
                ).fetchall()
                trade_rows = conn.execute(
                    "SELECT asset_id, side, taker_side, price, size, trade_id, trade_ts_ms FROM md_trades"
                ).fetchall()
            finally:
                conn.close()

            self.assertEqual(
                book_rows,
                [
                    ("NO", 0.47, 0.53, None, None, "structured_book_l1"),
                    ("YES", 0.48, 0.52, None, None, "structured_book_l1"),
                ],
            )
            self.assertEqual(
                trade_rows,
                [("yes-asset", "BUY", "BUY", 0.51, 7.0, "", 1_746_000_000_003)],
            )

    def test_build_for_slug_falls_back_to_raw_market_ws(self):
        with tempfile.TemporaryDirectory(prefix="pm_as_ofi_replay_") as tmp:
            root = Path(tmp)
            day = "2026-04-26"
            slug_dir = root / day / "btc-updown-5m-raw"
            write_jsonl(
                slug_dir / "market_ws.jsonl",
                [
                    envelope(
                        "market_ws",
                        {
                            "raw_text": json.dumps(
                                [
                                    {"event_type": "book", "market_side": "YES", "best_bid": 0.48, "best_ask": 0.52},
                                    {
                                        "event_type": "trade",
                                        "asset_id": "yes-asset",
                                        "side": "BUY",
                                        "price": 0.51,
                                        "size": 4.0,
                                        "trade_id": "tid-1",
                                        "timestamp": 1_746_000_000,
                                    },
                                ]
                            )
                        },
                        capture_seq=1,
                    )
                ],
            )

            conn = sqlite3.connect(":memory:")
            try:
                build_replay_db.init_db(conn)
                build_replay_db.build_for_slug(conn, day, slug_dir)
                conn.commit()

                book_count = conn.execute("SELECT COUNT(*) FROM md_book_l1").fetchone()[0]
                trade_rows = conn.execute(
                    "SELECT asset_id, side, taker_side, price, size, trade_id, trade_ts_ms FROM md_trades"
                ).fetchall()
            finally:
                conn.close()

            self.assertEqual(book_count, 1)
            self.assertEqual(
                trade_rows,
                [("yes-asset", "BUY", "BUY", 0.51, 4.0, "tid-1", 1_746_000_000_000)],
            )


if __name__ == "__main__":
    unittest.main()
