"""Close leftover token accounts in the bot wallet and get their rent back.

A Jupiter route can open a token account for a coin it only passes through. That account keeps
about 0.0015 SOL of rent locked, often with zero coins or a tiny dust remainder.

Preview (default, sends nothing):
    venv/bin/python scripts/close_route_accounts.py

Close the empty ones:
    venv/bin/python scripts/close_route_accounts.py --send

Also burn the dust in named coins and close those (the dust is destroyed):
    venv/bin/python scripts/close_route_accounts.py --send --burn DGuudmnJ...

Never touches: a coin with a real balance (unless named in --burn), a coin the bot has open,
or wrapped SOL.
"""
from __future__ import annotations

import argparse
import base64
import json
import sys
from pathlib import Path

import httpx
import yaml

TOKEN_PROGRAMS = (
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",
)
WSOL = "So11111111111111111111111111111111111111112"
ROOT = Path(__file__).resolve().parents[1]


def rpc_urls() -> list[str]:
    cfg = yaml.safe_load((ROOT / "config" / "sniper.yaml").read_text())
    rpc = cfg.get("rpc") or {}
    return list(rpc.get("paid") or []) + list(rpc.get("urls") or [])


def call(method: str, params: list):
    last = None
    for url in rpc_urls():
        try:
            body = httpx.post(url, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params}, timeout=15).json()
        except Exception as exc:  # next RPC
            last = type(exc).__name__
            continue
        if "result" in body:
            return body["result"]
        last = str(body.get("error"))[:200]
    raise RuntimeError(f"{method} failed on every RPC: {last}")


def bot_open_mints() -> set[str]:
    """Coins the bot holds right now (sqlite open positions)."""
    import sqlite3
    out: set[str] = set()
    db = ROOT / "sqlitebase" / "sniper.db"
    try:
        con = sqlite3.connect(db)
        for (row,) in con.execute("select row from position"):
            try:
                pos = json.loads(row)
            except Exception:
                continue
            if str(pos.get("status") or "OPEN").upper() == "OPEN" and pos.get("mint"):
                out.add(pos["mint"])
    except Exception as exc:
        print(f"! could not read open positions ({exc}); stopping to be safe")
        sys.exit(2)
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--send", action="store_true", help="really close (default is a preview)")
    ap.add_argument("--burn", nargs="*", default=[], help="mint prefixes whose dust may be burned, then closed")
    args = ap.parse_args()

    sys.path.insert(0, str(ROOT))
    from src.config.file_config import apply_config_file
    from src.execution.signer import load_keypair_from_env
    apply_config_file(str(ROOT / "config" / "sniper.yaml"))   # sets the wallet key the same way the bot does
    kp, owner = load_keypair_from_env()
    owner = str(owner or kp.pubkey())

    held = bot_open_mints()
    plan = []
    for program in TOKEN_PROGRAMS:
        res = call("getTokenAccountsByOwner", [owner, {"programId": program}, {"encoding": "jsonParsed"}])
        for acc in res["value"]:
            info = acc["account"]["data"]["parsed"]["info"]
            mint = info["mint"]
            amt = info["tokenAmount"]
            raw = int(amt["amount"])
            lamports = int(acc["account"]["lamports"])
            burn_ok = any(mint.startswith(p) for p in args.burn if p)
            if mint == WSOL:
                why = "skip: wrapped SOL"
            elif mint in held:
                why = "skip: the bot has this coin open"
            elif raw > 0 and not burn_ok:
                why = f"skip: holds {amt['uiAmountString']} (name it in --burn to burn and close)"
            elif info.get("state") == "frozen":
                why = "skip: frozen account"
            else:
                why = "CLOSE" + (f" (burn {amt['uiAmountString']})" if raw > 0 else "")
            plan.append({"pubkey": acc["pubkey"], "mint": mint, "raw": raw, "program": program,
                         "lamports": lamports, "why": why})

    total = 0
    for row in plan:
        print(f"{row['pubkey'][:8]}  mint {row['mint'][:8]}  rent {row['lamports']/1e9:.6f}  {row['why']}")
        if row["why"].startswith("CLOSE"):
            total += row["lamports"]
    todo = [r for r in plan if r["why"].startswith("CLOSE")]
    print(f"\n{len(todo)} account(s) to close, ≈{total/1e9:.6f} SOL back (minus 0.000005 fee each)")
    if not args.send:
        print("Preview only. Run again with --send to close them.")
        return

    from solders.hash import Hash
    from solders.instruction import AccountMeta, Instruction
    from solders.message import Message
    from solders.pubkey import Pubkey
    from solders.transaction import Transaction

    owner_key = Pubkey.from_string(owner)
    for row in todo:
        program = Pubkey.from_string(row["program"])
        account = Pubkey.from_string(row["pubkey"])
        ixs = []
        if row["raw"] > 0:
            ixs.append(Instruction(program, bytes([8]) + row["raw"].to_bytes(8, "little"), [
                AccountMeta(account, is_signer=False, is_writable=True),
                AccountMeta(Pubkey.from_string(row["mint"]), is_signer=False, is_writable=True),
                AccountMeta(owner_key, is_signer=True, is_writable=False),
            ]))
        ixs.append(Instruction(program, bytes([9]), [
            AccountMeta(account, is_signer=False, is_writable=True),
            AccountMeta(owner_key, is_signer=False, is_writable=True),
            AccountMeta(owner_key, is_signer=True, is_writable=False),
        ]))
        blockhash = Hash.from_string(call("getLatestBlockhash", [{"commitment": "confirmed"}])["value"]["blockhash"])
        tx = Transaction.new_unsigned(Message.new_with_blockhash(ixs, owner_key, blockhash))
        tx.sign([kp], blockhash)
        b64 = base64.b64encode(bytes(tx)).decode("ascii")
        try:
            sig = call("sendTransaction", [b64, {"encoding": "base64", "preflightCommitment": "confirmed"}])
            print(f"closed {row['pubkey'][:8]} ({row['mint'][:8]})  sig {sig}")
        except RuntimeError as exc:
            print(f"not closed {row['pubkey'][:8]} — {exc} (simulation rejected, no fee)")


if __name__ == "__main__":
    main()
