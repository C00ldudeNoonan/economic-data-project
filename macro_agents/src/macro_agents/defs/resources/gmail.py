"""Gmail notifier resource using a GCP service account.

Sends transactional alerts via the Gmail API. The service account must
have domain-wide delegation enabled with the `gmail.send` scope, and
must be authorized to impersonate `delegated_user` (typically the same
mailbox that owns the alerts inbox).

GCP setup (one-time):
  1. Enable the Gmail API in the GCP project.
  2. Create/reuse a service account; download its key file and point
     GOOGLE_APPLICATION_CREDENTIALS at it.
  3. In Google Workspace admin > Security > API controls > Domain-wide
     delegation, add the service account's client ID with scope
     `https://www.googleapis.com/auth/gmail.send`.
  4. Set GMAIL_DELEGATED_USER to the mailbox the service account will
     impersonate (e.g. "alerts@example.com").
"""

import base64
import json
import os
from email.message import EmailMessage

import dagster as dg
from google.oauth2 import service_account
from googleapiclient.discovery import build
from pydantic import Field

GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.send"]


class GmailNotifierResource(dg.ConfigurableResource):
    """Send email via Gmail API using a delegated service account."""

    credentials_json: str | None = Field(
        default=None,
        description=(
            "Service account credentials as JSON string or file path. "
            "Falls back to GOOGLE_APPLICATION_CREDENTIALS."
        ),
    )
    delegated_user: str = Field(
        description="Mailbox the service account impersonates via DWD.",
    )
    sender: str = Field(
        description="From address shown on outgoing alert emails.",
    )

    def _build_service(self):
        creds_raw = self.credentials_json or os.getenv(
            "GOOGLE_APPLICATION_CREDENTIALS", ""
        )
        if not creds_raw:
            raise ValueError(
                "credentials_json must be set or GOOGLE_APPLICATION_CREDENTIALS "
                "environment variable must be provided"
            )

        if creds_raw.strip().startswith("{"):
            info = json.loads(creds_raw)
            credentials = service_account.Credentials.from_service_account_info(
                info, scopes=GMAIL_SCOPES
            )
        elif os.path.exists(creds_raw):
            credentials = service_account.Credentials.from_service_account_file(
                creds_raw, scopes=GMAIL_SCOPES
            )
        else:
            raise ValueError(
                f"Credentials not found: '{creds_raw[:50]}...' is neither valid JSON "
                "nor an existing file path"
            )

        delegated = credentials.with_subject(self.delegated_user)
        return build("gmail", "v1", credentials=delegated, cache_discovery=False)

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
    delegated_user=dg.EnvVar("GMAIL_DELEGATED_USER"),
    sender=dg.EnvVar("GMAIL_SENDER"),
)
