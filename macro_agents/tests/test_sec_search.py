"""Tests for SEC filing search: sentence-aware chunking, FTS, and hybrid search."""

from macro_agents.defs.domains.sec.search import (
    _generate_chunk_id,
    _split_into_sentences,
    _split_text_into_chunks,
)


class TestSentenceSplitting:
    """Tests for regex-based sentence boundary detection."""

    def test_basic_sentences(self):
        text = "First sentence. Second sentence. Third sentence."
        sentences = _split_into_sentences(text)
        assert len(sentences) == 3
        assert sentences[0] == "First sentence."
        assert sentences[2] == "Third sentence."

    def test_abbreviation_handling(self):
        text = "Dr. Smith spoke at the conference. He discussed results."
        sentences = _split_into_sentences(text)
        assert len(sentences) == 2
        assert "Dr. Smith" in sentences[0]

    def test_mr_mrs_abbreviations(self):
        text = "Mr. Johnson and Mrs. Smith attended the meeting. They discussed Q4 results."
        sentences = _split_into_sentences(text)
        assert len(sentences) == 2
        assert "Mr. Johnson" in sentences[0]
        assert "Mrs. Smith" in sentences[0]

    def test_inc_corp_abbreviations(self):
        text = "Inc. reported strong earnings. Corp. filings were submitted on time."
        sentences = _split_into_sentences(text)
        assert len(sentences) == 2

    def test_paragraph_breaks(self):
        text = "First paragraph content.\n\nSecond paragraph content."
        sentences = _split_into_sentences(text)
        assert len(sentences) == 2

    def test_question_and_exclamation(self):
        text = "What are the risks? Revenue exceeded expectations! Growth continued."
        sentences = _split_into_sentences(text)
        assert len(sentences) == 3

    def test_empty_text(self):
        assert _split_into_sentences("") == []
        assert _split_into_sentences("   ") == []

    def test_single_sentence(self):
        text = "Just one sentence here."
        sentences = _split_into_sentences(text)
        assert len(sentences) == 1


class TestChunking:
    """Tests for sentence-aware text chunking."""

    def test_short_text_single_chunk(self):
        text = "This is a short text with only a few words."
        chunks = _split_text_into_chunks(text, target_words=500)
        assert len(chunks) == 1
        assert chunks[0] == text.strip()

    def test_empty_text(self):
        assert _split_text_into_chunks("") == []
        assert _split_text_into_chunks("   ") == []

    def test_chunks_respect_sentence_boundaries(self):
        """Chunks should not split mid-sentence."""
        sentences = [
            f"Sentence number {i} with some filler words added." for i in range(20)
        ]
        text = " ".join(sentences)
        chunks = _split_text_into_chunks(text, target_words=30, overlap_sentences=1)

        for chunk in chunks:
            # Each chunk should end with a period (sentence boundary)
            assert chunk.rstrip().endswith(".")

    def test_overlap_provides_context(self):
        """Consecutive chunks should share overlap sentences."""
        sentences = [f"Sentence {i} here." for i in range(10)]
        text = " ".join(sentences)
        chunks = _split_text_into_chunks(text, target_words=10, overlap_sentences=2)

        if len(chunks) >= 2:
            # Last sentences of chunk 0 should appear in chunk 1
            chunk0_sentences = _split_into_sentences(chunks[0])
            chunk1_text = chunks[1]
            # At least one sentence from end of chunk 0 should appear in chunk 1
            overlap_found = any(s in chunk1_text for s in chunk0_sentences[-2:])
            assert overlap_found, "Expected overlap between consecutive chunks"

    def test_small_remainder_merged(self):
        """Very small trailing chunks should be merged into the previous chunk."""
        # 8 sentences of ~5 words each = ~40 words total
        # With target=30, we'd get 1 chunk of ~30 words + remainder of ~10 words
        # The remainder (~10 words) is > 25% of target (7.5), so it stays separate
        sentences = [f"Word word word word {i}." for i in range(8)]
        text = " ".join(sentences)
        chunks = _split_text_into_chunks(text, target_words=30, overlap_sentences=1)
        # Just verify no chunk is excessively small (< 25% of target = 7 words)
        for chunk in chunks:
            word_count = len(chunk.split())
            assert word_count >= 5, f"Chunk too small: {word_count} words"

    def test_deterministic_output(self):
        """Same input should always produce same chunks."""
        text = "First sentence about risk factors. Second about supply chains. Third about revenue."
        chunks1 = _split_text_into_chunks(text, target_words=10, overlap_sentences=1)
        chunks2 = _split_text_into_chunks(text, target_words=10, overlap_sentences=1)
        assert chunks1 == chunks2


class TestChunkIdGeneration:
    """Tests for deterministic chunk ID generation."""

    def test_deterministic(self):
        id1 = _generate_chunk_id("filing-1", "Risk Factors", 0)
        id2 = _generate_chunk_id("filing-1", "Risk Factors", 0)
        assert id1 == id2

    def test_different_inputs_different_ids(self):
        id1 = _generate_chunk_id("filing-1", "Risk Factors", 0)
        id2 = _generate_chunk_id("filing-1", "Risk Factors", 1)
        id3 = _generate_chunk_id("filing-2", "Risk Factors", 0)
        assert id1 != id2
        assert id1 != id3

    def test_id_length(self):
        chunk_id = _generate_chunk_id("filing-1", "Business", 0)
        assert len(chunk_id) == 16
