"""DSPy signatures for Fed communications sentiment analysis."""

import dspy


class FedSentimentSignature(dspy.Signature):
    """Score the hawkish/dovish sentiment of a Federal Reserve communication section.

    Analyze the text for monetary policy stance signals. Hawkish language favors
    tighter policy (higher rates, inflation concern). Dovish language favors
    looser policy (lower rates, growth/employment concern).
    """

    speaker: str = dspy.InputField(
        desc="Speaker name and role (e.g. 'CHAIRMAN POWELL', 'MR. CLARIDA')"
    )
    content: str = dspy.InputField(desc="Text content of the transcript section")
    meeting_date: str = dspy.InputField(desc="FOMC meeting date (YYYY-MM-DD)")

    hawkish_score: str = dspy.OutputField(
        desc="Score from 0.0 to 1.0 indicating strength of hawkish signals"
    )
    dovish_score: str = dspy.OutputField(
        desc="Score from 0.0 to 1.0 indicating strength of dovish signals"
    )
    key_phrases: str = dspy.OutputField(
        desc='JSON array of objects: [{"phrase": "...", "sentiment": "hawkish"|"dovish", "context": "..."}]'
    )
    reasoning: str = dspy.OutputField(
        desc="Brief explanation of the scoring rationale (2-3 sentences)"
    )


class FedTopicExtractionSignature(dspy.Signature):
    """Extract economic topics and subtopics from a Federal Reserve communication section.

    Identify the main topics discussed (inflation, employment, growth, housing,
    financial conditions, etc.) with relevance scores and sentiment labels.
    """

    content: str = dspy.InputField(desc="Text content of the transcript section")
    speaker: str = dspy.InputField(desc="Speaker name (e.g. 'CHAIRMAN POWELL')")

    topics: str = dspy.OutputField(
        desc=(
            'JSON array of objects: [{"topic": "inflation", "subtopic": "core PCE", '
            '"relevance_score": 0.85, "sentiment": "hawkish"|"dovish"|"neutral"}]'
        )
    )
