#!/usr/bin/env python3
"""
One-time script: derive Polymarket API credentials from your private key.
Writes the credentials to .env (appends/updates existing file).
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import settings


def main():
    if not settings.POLY_PRIVATE_KEY:
        print("ERROR: Set POLY_PRIVATE_KEY in .env first")
        sys.exit(1)

    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import ApiCreds

        # Level 1: just private key (no creds) to derive API keys
        client = ClobClient(
            host=settings.CLOB_BASE_URL,
            chain_id=settings.CHAIN_ID,
            key=settings.POLY_PRIVATE_KEY,
        )

        print("Deriving API credentials from private key…")
        api_creds = client.create_api_key()

        print("\n✓ API Credentials generated:")
        print(f"  POLY_API_KEY={api_creds.api_key}")
        print(f"  POLY_API_SECRET={api_creds.api_secret}")
        print(f"  POLY_API_PASSPHRASE={api_creds.api_passphrase}")

        # Update .env file
        env_path = Path(__file__).parent.parent / ".env"
        if env_path.exists():
            content = env_path.read_text()
        else:
            env_path.write_text("")
            content = ""

        def _set_env(content: str, key: str, value: str) -> str:
            import re
            pattern = rf"^{key}=.*$"
            replacement = f"{key}={value}"
            if re.search(pattern, content, re.MULTILINE):
                return re.sub(pattern, replacement, content, flags=re.MULTILINE)
            return content + f"\n{replacement}"

        content = _set_env(content, "POLY_API_KEY", api_creds.api_key)
        content = _set_env(content, "POLY_API_SECRET", api_creds.api_secret)
        content = _set_env(content, "POLY_API_PASSPHRASE", api_creds.api_passphrase)
        env_path.write_text(content)

        print(f"\n✓ Credentials saved to {env_path}")

    except ImportError:
        print("ERROR: py-clob-client not installed. Run: pip install py-clob-client")
        sys.exit(1)
    except Exception as exc:
        print(f"ERROR: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
