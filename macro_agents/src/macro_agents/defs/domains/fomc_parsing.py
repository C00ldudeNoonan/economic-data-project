"""Pure parsing helpers for FOMC transcripts.

Speaker-attributed sectioning and deterministic id hashing, extracted from
fomc_transcripts.py so they can be unit-tested without Dagster, GCS, or a PDF.
"""

import hashlib
import re


def generate_id(prefix: str, *components: str) -> str:
    """Generate a unique ID by hashing components."""
    hash_input = "_".join(str(c) for c in components)
    hash_digest = hashlib.sha256(hash_input.encode()).hexdigest()[:12]
    return f"{prefix}_{hash_digest}"


def _parse_transcript_sections(full_text: str, transcript_id: str) -> list[dict]:
    """Parse transcript text into speaker-attributed sections.

    FOMC transcripts follow a pattern where speakers are identified by
    all-caps names (e.g., "CHAIRMAN BERNANKE.", "MR. DUDLEY.") followed
    by their remarks.
    """
    # Match speaker lines: "CHAIRMAN POWELL.", "CHAIR YELLEN.", "MR. CLARIDA.", etc.
    # CHAIR/CHAIRMAN/VICE CHAIR(MAN) have no period after title; MR./MS./MRS./DR. do.
    speaker_pattern = re.compile(
        r"^((?:VICE CHAIR(?:MAN)?|CHAIR(?:MAN)?|(?:MR|MS|MRS|DR)\.)\s+[A-Z]+\.)",
        re.MULTILINE,
    )

    splits = speaker_pattern.split(full_text)
    sections = []
    order = 0

    # First chunk is preamble (before any speaker)
    if splits and splits[0].strip():
        order += 1
        sections.append(
            {
                "section_id": generate_id("section", transcript_id, "preamble"),
                "section_order": order,
                "section_type": "preamble",
                "speaker": None,
                "speaker_role": None,
                "content": splits[0].strip(),
            }
        )

    # Remaining chunks alternate: speaker_name, content, speaker_name, content, ...
    for i in range(1, len(splits) - 1, 2):
        speaker_raw = splits[i].strip().rstrip(".")
        content = splits[i + 1].strip() if i + 1 < len(splits) else ""
        if not content:
            continue

        order += 1
        speaker_role = None
        if "VICE CHAIR" in speaker_raw:
            speaker_role = "Vice Chair"
        elif "CHAIR" in speaker_raw:
            speaker_role = "Chair"

        sections.append(
            {
                "section_id": generate_id("section", transcript_id, str(order)),
                "section_order": order,
                "section_type": "discussion",
                "speaker": speaker_raw,
                "speaker_role": speaker_role,
                "content": content,
            }
        )

    return sections
