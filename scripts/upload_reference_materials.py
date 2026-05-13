# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "google-auth>=2.28.0",
#     "google-cloud-storage>=2.14.0",
#     "python-dotenv>=1.0.0",
# ]
# ///
"""Upload reference materials to GCS and generate a markdown catalog.

Extracts zip archives from docs/reference_materials/, uploads all files
to the project's GCS bucket under reference_materials/, and generates
a catalog.md index that is committed to git.

Usage:
    uv run scripts/upload_reference_materials.py --dry-run
    uv run scripts/upload_reference_materials.py
    uv run scripts/upload_reference_materials.py --force
    uv run scripts/upload_reference_materials.py --bucket my-bucket
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import tempfile
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import storage
from google.oauth2 import service_account

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
REF_DIR = PROJECT_ROOT / "docs" / "reference_materials"

ALLOWED_EXTENSIONS = {".pdf", ".epub", ".mobi"}
JUNK_PATTERNS = re.compile(
    r"(__MACOSX|\.DS_Store|Thumbs\.db|"
    r"Torrent\s+[Dd]ownloaded|VISIT ME ON FACEBOOK|"
    r"\.txt$)",
    re.IGNORECASE,
)

CONTENT_TYPES = {
    ".pdf": "application/pdf",
    ".epub": "application/epub+zip",
    ".mobi": "application/x-mobipocket-ebook",
}

SOURCE_FILES = {
    "books": "Books.zip",
    "mauboussin": "Michael J. Mauboussin-20260418T161105Z-3-001.zip",
    "standalone": "Edwin_LeFevre_Reminiscences_of_a_Stock_Operator.pdf",
}


@dataclass
class FileEntry:
    local_path: Path
    gcs_path: str
    category: str
    file_size: int
    original_name: str


def normalize_path_segment(segment: str) -> str:
    """Lowercase, replace spaces/special chars with underscores."""
    segment = segment.strip().lower()
    segment = re.sub(r"[^\w.\-]", "_", segment)
    segment = re.sub(r"_+", "_", segment)
    return segment.strip("_")


def is_junk(name: str) -> bool:
    return bool(JUNK_PATTERNS.search(name))


def extract_zip(zip_path: Path, dest: Path, category: str, gcs_prefix: str) -> list[FileEntry]:
    """Extract a zip and return FileEntry list for uploadable files."""
    entries: list[FileEntry] = []
    with zipfile.ZipFile(zip_path, "r") as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue
            if is_junk(info.filename):
                continue

            ext = Path(info.filename).suffix.lower()
            if ext not in ALLOWED_EXTENSIONS:
                continue

            # Strip the top-level directory from the zip path
            parts = Path(info.filename).parts
            if len(parts) > 1:
                relative = Path(*parts[1:])
            else:
                relative = Path(parts[0])

            # Build normalized GCS path
            normalized_parts = [normalize_path_segment(p) for p in relative.parts]
            gcs_path = f"{gcs_prefix}/{'/'.join(normalized_parts)}"

            # Extract file
            extracted = dest / info.filename
            zf.extract(info, dest)

            entries.append(FileEntry(
                local_path=extracted,
                gcs_path=gcs_path,
                category=category,
                file_size=info.file_size,
                original_name=relative.name,
            ))

    return entries


def collect_files(tmp_dir: Path) -> list[FileEntry]:
    """Extract zips and collect all uploadable files."""
    entries: list[FileEntry] = []

    # Books.zip
    books_zip = REF_DIR / SOURCE_FILES["books"]
    if books_zip.exists():
        log.info("Extracting Books.zip...")
        entries.extend(extract_zip(
            books_zip, tmp_dir / "books", "Books", "reference_materials/books",
        ))
    else:
        log.warning("Books.zip not found, skipping")

    # Mauboussin zip
    maub_zip = REF_DIR / SOURCE_FILES["mauboussin"]
    if maub_zip.exists():
        log.info("Extracting Mauboussin papers...")
        entries.extend(extract_zip(
            maub_zip, tmp_dir / "mauboussin", "Mauboussin Research Papers",
            "reference_materials/mauboussin",
        ))
    else:
        log.warning("Mauboussin zip not found, skipping")

    # Standalone PDF
    standalone = REF_DIR / SOURCE_FILES["standalone"]
    if standalone.exists():
        gcs_path = f"reference_materials/standalone/{normalize_path_segment(standalone.name)}"
        entries.append(FileEntry(
            local_path=standalone,
            gcs_path=gcs_path,
            category="Standalone",
            file_size=standalone.stat().st_size,
            original_name=standalone.name,
        ))
    else:
        log.warning("Standalone PDF not found, skipping")

    entries.sort(key=lambda e: (e.category, e.gcs_path))
    return entries


def format_size(size_bytes: int) -> str:
    """Format bytes as human-readable size."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    if size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    if size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"


def upload_files(
    client: storage.Client,
    bucket_name: str,
    entries: list[FileEntry],
    *,
    dry_run: bool = False,
    force: bool = False,
) -> tuple[int, int, int]:
    """Upload files to GCS. Returns (uploaded, skipped, failed) counts."""
    bucket = client.bucket(bucket_name)
    uploaded = skipped = failed = 0
    total = len(entries)

    for i, entry in enumerate(entries, 1):
        prefix = f"[{i}/{total}]"
        blob = bucket.blob(entry.gcs_path)
        ext = Path(entry.gcs_path).suffix.lower()
        content_type = CONTENT_TYPES.get(ext, "application/octet-stream")

        if not force and blob.exists():
            log.info(f"{prefix} Skipped (exists): {entry.gcs_path}")
            skipped += 1
            continue

        if dry_run:
            log.info(f"{prefix} Would upload: {entry.gcs_path} ({format_size(entry.file_size)})")
            skipped += 1
            continue

        try:
            blob.upload_from_filename(str(entry.local_path), content_type=content_type)
            log.info(f"{prefix} Uploaded: {entry.gcs_path} ({format_size(entry.file_size)})")
            uploaded += 1
        except Exception:
            log.exception(f"{prefix} Failed: {entry.gcs_path}")
            failed += 1

    return uploaded, skipped, failed


def generate_catalog(entries: list[FileEntry], bucket_name: str, output_path: Path) -> None:
    """Generate a markdown catalog of all uploaded reference materials."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Group by category
    categories: dict[str, list[FileEntry]] = {}
    for entry in entries:
        categories.setdefault(entry.category, []).append(entry)

    lines = [
        "# Reference Materials Catalog",
        "",
        f"> Auto-generated by `scripts/upload_reference_materials.py` on {now}.",
        f"> Files stored in GCS bucket `{bucket_name}` under `reference_materials/` prefix.",
        "",
        "## Summary",
        "",
        "| Category | File Count | Total Size |",
        "|----------|-----------|------------|",
    ]

    total_count = 0
    total_size = 0
    for cat_name in sorted(categories):
        cat_entries = categories[cat_name]
        count = len(cat_entries)
        size = sum(e.file_size for e in cat_entries)
        total_count += count
        total_size += size
        lines.append(f"| {cat_name} | {count} | {format_size(size)} |")

    lines.append(f"| **Total** | **{total_count}** | **{format_size(total_size)}** |")
    lines.append("")

    for cat_name in sorted(categories):
        cat_entries = categories[cat_name]
        lines.append(f"## {cat_name}")
        lines.append("")
        lines.append("| File | GCS Path | Size |")
        lines.append("|------|----------|------|")
        for entry in sorted(cat_entries, key=lambda e: e.original_name.lower()):
            gcs_uri = f"`gs://{bucket_name}/{entry.gcs_path}`"
            lines.append(f"| {entry.original_name} | {gcs_uri} | {format_size(entry.file_size)} |")
        lines.append("")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Upload reference materials to GCS and generate catalog",
    )
    parser.add_argument(
        "--bucket",
        default=os.getenv("GCS_BUCKET_NAME"),
        help="GCS bucket name (default: $GCS_BUCKET_NAME)",
    )
    parser.add_argument(
        "--credentials",
        default=os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
        help="Path to service account JSON (default: $GOOGLE_APPLICATION_CREDENTIALS)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List files without uploading",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-upload even if blob already exists",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if not args.bucket:
        sys.exit("Error: --bucket or GCS_BUCKET_NAME env var required")

    if not args.dry_run and not args.credentials:
        sys.exit("Error: --credentials or GOOGLE_APPLICATION_CREDENTIALS env var required")

    if not REF_DIR.exists():
        sys.exit(f"Error: reference materials directory not found: {REF_DIR}")

    if args.dry_run:
        client = None  # type: ignore[assignment]
    else:
        creds_value = args.credentials
        if creds_value.strip().startswith("{"):
            creds_info = json.loads(creds_value)
            credentials = service_account.Credentials.from_service_account_info(creds_info)
        else:
            credentials = service_account.Credentials.from_service_account_file(creds_value)
        client = storage.Client(credentials=credentials)

    with tempfile.TemporaryDirectory() as tmp_dir:
        entries = collect_files(Path(tmp_dir))
        log.info(f"\nFound {len(entries)} files to process\n")

        if not entries:
            sys.exit("No files found to upload")

        if args.dry_run:
            for i, entry in enumerate(entries, 1):
                log.info(
                    f"[{i}/{len(entries)}] {entry.gcs_path} "
                    f"({format_size(entry.file_size)})"
                )

            log.info(f"\nDry run complete. {len(entries)} files would be uploaded.")

            catalog_path = REF_DIR / "catalog.md"
            generate_catalog(entries, args.bucket, catalog_path)
            log.info(f"Catalog preview written to {catalog_path}")
            return

        uploaded, skipped, failed = upload_files(
            client, args.bucket, entries, dry_run=False, force=args.force,
        )
        log.info(f"\nResults: {uploaded} uploaded, {skipped} skipped, {failed} failed")

        catalog_path = REF_DIR / "catalog.md"
        generate_catalog(entries, args.bucket, catalog_path)
        log.info(f"Catalog written to {catalog_path}")


if __name__ == "__main__":
    main()
