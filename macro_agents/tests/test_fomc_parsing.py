"""Tests for the pure FOMC transcript parsing helpers (issue #136).

Speaker sectioning and id hashing moved out of fomc_transcripts.py into
fomc_parsing.py; this covers the regex speaker attribution that previously had
no direct tests.
"""

from macro_agents.defs.domains.fomc_parsing import (
    _parse_transcript_sections,
    generate_id,
)


class TestGenerateId:
    def test_is_deterministic(self):
        assert generate_id("section", "t1", "3") == generate_id("section", "t1", "3")

    def test_prefix_and_shape(self):
        out = generate_id("transcript", "2018", "2018-03-21")
        assert out.startswith("transcript_")
        assert len(out.split("_", 1)[1]) == 12  # 12-char sha256 slice

    def test_distinct_components_differ(self):
        assert generate_id("section", "t1", "1") != generate_id("section", "t1", "2")


class TestParseTranscriptSections:
    def test_preamble_before_first_speaker(self):
        text = "Meeting called to order.\nCHAIRMAN POWELL. Welcome everyone."
        sections = _parse_transcript_sections(text, "t1")
        assert sections[0]["section_type"] == "preamble"
        assert sections[0]["speaker"] is None
        assert "called to order" in sections[0]["content"]

    def test_speaker_attribution_and_roles(self):
        text = (
            "CHAIRMAN POWELL. Opening remarks here.\n"
            "VICE CHAIR CLARIDA. My comments follow.\n"
            "MR. DUDLEY. And mine after that."
        )
        sections = _parse_transcript_sections(text, "t1")
        discussion = [s for s in sections if s["section_type"] == "discussion"]

        by_speaker = {s["speaker"]: s for s in discussion}
        assert by_speaker["CHAIRMAN POWELL"]["speaker_role"] == "Chair"
        assert by_speaker["VICE CHAIR CLARIDA"]["speaker_role"] == "Vice Chair"
        assert by_speaker["MR. DUDLEY"]["speaker_role"] is None
        assert by_speaker["MR. DUDLEY"]["content"] == "And mine after that."

    def test_section_order_is_sequential(self):
        text = "Preamble.\nCHAIRMAN POWELL. One.\nMR. WILLIAMS. Two."
        orders = [s["section_order"] for s in _parse_transcript_sections(text, "t1")]
        assert orders == [1, 2, 3]

    def test_empty_speaker_content_skipped(self):
        text = "CHAIRMAN POWELL. \nMR. WILLIAMS. Real content."
        sections = _parse_transcript_sections(text, "t1")
        speakers = [s["speaker"] for s in sections]
        assert "CHAIRMAN POWELL" not in speakers
        assert "MR. WILLIAMS" in speakers

    def test_no_speakers_yields_single_preamble(self):
        sections = _parse_transcript_sections("Just narrative text.", "t1")
        assert len(sections) == 1
        assert sections[0]["section_type"] == "preamble"
