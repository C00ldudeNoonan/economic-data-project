"""One-time OAuth bootstrap for the Gmail alert notifier.

Run this once on a machine with a browser to mint a long-lived refresh
token for sending mail as your personal Gmail account. The sensor then
uses the token at runtime — no further interaction required.

Usage:
    uv run --project macro_agents python scripts/gmail_oauth_bootstrap.py \\
        --client-secrets ~/.config/gmail/client_secret.json \\
        --token-output ~/.config/gmail/token.json

Prerequisites (GCP setup, one time):
    1. Console > APIs & Services > Library > enable "Gmail API".
    2. Console > APIs & Services > OAuth consent screen:
         - User type: External
         - Add yourself under "Test users".
         - Add scope `.../auth/gmail.send`.
    3. Console > APIs & Services > Credentials > Create OAuth client ID:
         - Application type: Desktop app
         - Download the JSON; that's the --client-secrets file.

Behavior:
    Opens a browser, you click Allow, the script writes a JSON file
    containing refresh_token + client_id + client_secret. Point the
    `GMAIL_OAUTH_TOKEN_PATH` env var at that file.
"""

import argparse
import json
import subprocess
from pathlib import Path

from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/gmail.send"]


def _repo_root() -> Path | None:
    """Return the git working tree root, or None if not inside a repo."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            check=True,
            capture_output=True,
            text=True,
        )
        return Path(result.stdout.strip()).resolve()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


def _reject_in_repo(path: Path) -> None:
    """Refuse to write the token inside the git working tree.

    Why: the token file contains a long-lived refresh_token plus the
    OAuth client_id/client_secret. Even though .gitignore covers it,
    an accidental `git add -f` or a misconfigured tool could publish
    credentials to a public repo. Force the user to pick a path
    outside the working tree.
    """
    root = _repo_root()
    if root is None:
        return
    resolved = path.resolve()
    try:
        resolved.relative_to(root)
    except ValueError:
        return
    raise SystemExit(
        f"Refusing to write token inside the repo: {resolved}\n"
        f"Pick a path outside {root} (e.g. ~/.config/gmail/token.json)."
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--client-secrets",
        required=True,
        type=Path,
        help="Path to the OAuth client secrets JSON downloaded from GCP.",
    )
    parser.add_argument(
        "--token-output",
        required=True,
        type=Path,
        help="Where to write the resulting refresh-token JSON.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=0,
        help="Local server port for the OAuth redirect (0 = auto).",
    )
    args = parser.parse_args()

    if not args.client_secrets.exists():
        raise SystemExit(f"client secrets not found: {args.client_secrets}")

    _reject_in_repo(args.client_secrets)
    _reject_in_repo(args.token_output)

    flow = InstalledAppFlow.from_client_secrets_file(
        str(args.client_secrets), scopes=SCOPES
    )
    credentials = flow.run_local_server(port=args.port, prompt="consent")

    if not credentials.refresh_token:
        raise SystemExit(
            "No refresh_token returned. Re-run with prompt=consent and "
            "ensure the OAuth consent screen is configured."
        )

    payload = {
        "token": credentials.token,
        "refresh_token": credentials.refresh_token,
        "token_uri": credentials.token_uri,
        "client_id": credentials.client_id,
        "client_secret": credentials.client_secret,
        "scopes": list(credentials.scopes or SCOPES),
    }

    args.token_output.parent.mkdir(parents=True, exist_ok=True)
    args.token_output.write_text(json.dumps(payload, indent=2))
    args.token_output.chmod(0o600)
    print(f"Wrote refresh token to {args.token_output}")
    print("Set GMAIL_OAUTH_TOKEN_PATH to that path in your environment.")


if __name__ == "__main__":
    main()
