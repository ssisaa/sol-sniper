"""
Transaction signing for SOL-SNIPER.

Loads a keypair from:
  - SOL_SNIPER_KEYPAIR path (JSON byte array as Solana CLI format)
  - or SOL_SNIPER_PRIVATE_KEY base58 secret

Uses `solders` when installed. If not available, signing fails with a clear error
(install: pip install solders).
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Optional, Tuple

log = logging.getLogger("sol-sniper.signer")


class SignerError(Exception):
    pass


def _load_solders():
    try:
        from solders.keypair import Keypair
        from solders.transaction import VersionedTransaction
        return Keypair, VersionedTransaction
    except ImportError as e:
        raise SignerError(
            "solders not installed — cannot sign. Run: pip install solders"
        ) from e


def _project_keypair_ref(path: Path, root: Path) -> str:
    """Store the key as /sol-sniper/<file>, never /mnt/f/... or C:\\..."""
    try:
        rel = path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        rel = path.name
    return f"/sol-sniper/{rel}"


def apply_uploaded_wallet_env() -> None:
    """If a keypair was uploaded from the page, that file wins over the shell export."""
    from src.config.file_config import resolve_keypair_path
    marker = Path(__file__).resolve().parents[2] / "logs" / "wallet_active.path"
    if not marker.is_file():
        return
    raw = marker.read_text(encoding="utf-8").strip()
    path = resolve_keypair_path(raw)
    if path and Path(path).is_file():
        os.environ["SOL_SNIPER_KEYPAIR"] = path
        os.environ.pop("SOL_SNIPER_PRIVATE_KEY", None)
        log.info("Using uploaded wallet file")


def _secret_bytes(body: bytes) -> bytes:
    """Solana CLI array, Phantom base58, or a JSON object with a secret key."""
    text = body.decode("utf-8-sig").strip()
    if not text:
        raise SignerError("The keypair file is empty")
    data = text
    try:
        data = json.loads(text)
    except Exception:
        data = text
    secret = data
    if isinstance(data, dict):
        secret = (
            data.get("secretKey")
            or data.get("secret_key")
            or data.get("privateKey")
            or data.get("private_key")
            or data.get("keypair")
        )
    if isinstance(secret, str):
        token = secret.strip()
        try:
            import base58
            raw = base58.b58decode(token)
        except Exception as e:
            raise SignerError("That text is not a base58 Solana secret key") from e
    elif isinstance(secret, list):
        try:
            raw = bytes(int(x) & 0xFF for x in secret)
        except Exception as e:
            raise SignerError("Keypair file must be a JSON array of numbers") from e
    else:
        raise SignerError("Upload a Solana keypair JSON array, or a base58 secret key")
    if len(raw) not in (32, 64):
        raise SignerError(f"Keypair is {len(raw)} bytes. Solana needs 32 or 64.")
    return raw


def install_uploaded_keypair(body: bytes) -> str:
    """
    Save a Solana keypair and point the bot at it.
    Accepts the CLI JSON array (bot-wallet.json) or a base58 secret.
    Returns the public key. The secret is never logged.
    """
    raw = _secret_bytes(body)
    Keypair, _ = _load_solders()
    try:
        if len(raw) == 64:
            kp = Keypair.from_bytes(raw)
        else:
            kp = Keypair.from_seed(raw)
            raw = bytes(kp)
    except Exception as e:
        raise SignerError("That file is not a Solana keypair") from e
    pub = str(kp.pubkey())
    root = Path(__file__).resolve().parents[2]
    folder = root / "logs"
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / "active-wallet.json"
    path.write_text(json.dumps(list(raw)), encoding="utf-8")
    try:
        path.chmod(0o600)
    except Exception:
        pass
    (folder / "wallet_active.path").write_text(_project_keypair_ref(path, root), encoding="utf-8")
    os.environ["SOL_SNIPER_KEYPAIR"] = str(path.resolve())
    os.environ.pop("SOL_SNIPER_PRIVATE_KEY", None)
    if os.environ.get("SOL_SNIPER_LIVE", "").strip() not in ("1", "true", "TRUE", "yes", "YES"):
        os.environ["SOL_SNIPER_LIVE"] = "1"
    yaml_path = root / "config" / "sniper.yaml"
    if yaml_path.is_file():
        text = yaml_path.read_text(encoding="utf-8")
        stored = _project_keypair_ref(path, root)
        lines = []
        for line in text.splitlines():
            if line.strip().startswith("keypair_path:"):
                lines.append(f'  keypair_path: "{stored}"')
            elif line.strip().startswith("private_key:"):
                lines.append("  private_key: null")
            elif line.strip().startswith("live:"):
                lines.append("  live: true")
            else:
                lines.append(line)
        yaml_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    log.info("Wallet changed to %s…", pub[:8])
    return pub


def load_keypair_from_env() -> Tuple[object, str]:
    """
    Returns (Keypair, pubkey_str).
    """
    Keypair, _ = _load_solders()
    from src.config.file_config import resolve_keypair_path
    path = resolve_keypair_path(os.environ.get("SOL_SNIPER_KEYPAIR", ""))
    b58 = os.environ.get("SOL_SNIPER_PRIVATE_KEY", "").strip()

    if path:
        p = Path(path)
        if not p.exists():
            raise SignerError(f"Keypair file not found: {path}")
        data = json.loads(p.read_text())
        if not isinstance(data, list):
            raise SignerError("Keypair file must be a JSON array of bytes (Solana CLI format)")
        kp = Keypair.from_bytes(bytes(data))
        return kp, str(kp.pubkey())

    if b58:
        try:
            import base58
            secret = base58.b58decode(b58)
        except Exception as e:
            raise SignerError(f"Invalid SOL_SNIPER_PRIVATE_KEY base58: {e}") from e
        kp = Keypair.from_bytes(secret)
        return kp, str(kp.pubkey())

    raise SignerError(
        "No keypair configured. Set SOL_SNIPER_KEYPAIR=/path/to/id.json "
        "or SOL_SNIPER_PRIVATE_KEY=<base58>"
    )


def sign_versioned_tx_base64(unsigned_b64: str) -> str:
    """
    Sign a Jupiter (or other) versioned transaction (base64) and return signed base64.
    """
    import base64

    Keypair, VersionedTransaction = _load_solders()
    kp, pubkey = load_keypair_from_env()
    raw = base64.b64decode(unsigned_b64)
    tx = VersionedTransaction.from_bytes(raw)
    # solders: VersionedTransaction populates signatures when constructed with keypairs
    # Prefer populate pattern:
    try:
        from solders.transaction import VersionedTransaction as VT
        # Re-sign message
        msg = tx.message
        signed = VT(msg, [kp])
        out = base64.b64encode(bytes(signed)).decode("ascii")
        log.info("Signed versioned tx with pubkey=%s…", pubkey[:8])
        return out
    except Exception as e:
        raise SignerError(f"Failed to sign transaction: {e}") from e


def pubkey_from_env() -> Optional[str]:
    try:
        _, pk = load_keypair_from_env()
        return pk
    except SignerError:
        return None


def live_armed() -> bool:
    """True only when explicit env arm + key material present."""
    if os.environ.get("SOL_SNIPER_LIVE", "").strip() not in ("1", "true", "TRUE", "yes", "YES"):
        return False
    try:
        load_keypair_from_env()
        return True
    except SignerError:
        return False
