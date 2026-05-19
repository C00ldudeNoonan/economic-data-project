"""Gmail notifier resource using OAuth user credentials.

Sends transactional alerts via the Gmail API as a single human user
(no domain-wide delegation, no service account). This is the right
path for personal `@gmail.com` accounts, which Google does not allow
service accounts to impersonate.

One-time bootstrap (see `scripts/gmail_oauth_bootstrap.py`):
  1. In GCP, create an **OAuth 2.0 Client ID** of type "Desktop app".
     Download the client secrets JSON.
  2. Add yourself as a test user under "OAuth consent screen".
  3. Run the bootstrap script — it opens a browser, you click
     "Allow gmail.send", and the script writes a refresh token to
     `GMAIL_OAUTH_TOKEN_PATH`.
  4. The sensor exchanges that refresh token for short-lived access
     tokens at send time. The refresh token never expires unless you
     revoke it or change your Google password.

Env vars:
  GMAIL_SENDER                 The Gmail address that owns the token.
  GMAIL_OAUTH_TOKEN_PATH       Path to the JSON refresh-token file.
  GMAIL_OAUTH_CLIENT_SECRETS   Path to client_secret.json (only used
                               by the bootstrap script, not at runtime).
"""

import base64
import json
import os
from email.message import EmailMessage

import dagster as dg
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from pydantic import Field

GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.send"]


class GmailNotifierResource(dg.ConfigurableResource):
    """Send email via Gmail API using a stored OAuth refresh token."""

    token_path: str = Field(
        description=(
            "Path to a JSON file produced by the OAuth bootstrap script. "
            "Must contain at minimum: refresh_token, client_id, client_secret."
        ),
    )
    sender: str = Field(
        description="Gmail address that owns the refresh token (also From:).",
    )

    def _load_credentials(self) -> Credentials:
        if not os.path.exists(self.token_path):
            raise FileNotFoundError(
                f"Gmail OAuth token file not found at {self.token_path}. "
                "Run scripts/gmail_oauth_bootstrap.py to generate it."
            )

        with open(self.token_path) as f:
            data = json.load(f)

        missing = [
            k
            for k in ("refresh_token", "client_id", "client_secret")
            if not data.get(k)
        ]
        if missing:
            raise ValueError(
                f"Token file {self.token_path} is missing required fields: {missing}. "
                "Re-run scripts/gmail_oauth_bootstrap.py."
            )

        credentials = Credentials(
            token=data.get("token"),
            refresh_token=data["refresh_token"],
            token_uri=data.get("token_uri", "https://oauth2.googleapis.com/token"),
            client_id=data["client_id"],
            client_secret=data["client_secret"],
            scopes=data.get("scopes", GMAIL_SCOPES),
        )

        if not credentials.valid:
            credentials.refresh(Request())

        return credentials

    def _build_service(self):
        credentials = self._load_credentials()
        return build("gmail", "v1", credentials=credentials, cache_discovery=False)

    def send_alert(
        self,
        to: str,
        subject: str,
        text_body: str,
        html_body: str | None = None,
    ) -> str:
        """Send a single alert email. Returns the Gmail message id."""
        message = EmailMessage()
        message["To"] = to
        message["From"] = self.sender
        message["Subject"] = subject
        message.set_content(text_body)
        if html_body:
            message.add_alternative(html_body, subtype="html")

        raw = base64.urlsafe_b64encode(message.as_bytes()).decode()
        service = self._build_service()
        sent = service.users().messages().send(userId="me", body={"raw": raw}).execute()
        return sent["id"]


gmail_notifier_resource = GmailNotifierResource(
    token_path=dg.EnvVar("GMAIL_OAUTH_TOKEN_PATH"),
    sender=dg.EnvVar("GMAIL_SENDER"),
)
