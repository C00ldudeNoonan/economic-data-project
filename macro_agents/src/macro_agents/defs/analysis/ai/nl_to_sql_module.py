"""Natural Language to SQL Module - Converts user questions to SQL queries using DSPy."""

import dspy


class NaturalLanguageToSQLSignature(dspy.Signature):
    """Convert natural language questions about economic data into SQL queries."""

    question: str = dspy.InputField(
        desc="User's natural language question about economic data"
    )

    data_dictionary: str = dspy.InputField(
        desc="""Available tables and columns with metadata (JSON format).
        Includes table names, column names, data types, descriptions, and CRITICAL: additivity_type.
        - ADDITIVE columns (counts, days) can be summed: SUM(trading_days)
        - NON_ADDITIVE columns (percentages, rates) need AVG or other aggregate: AVG(total_return_pct)
        - SEMI_ADDITIVE columns (dates, timestamps) are for time-based operations only"""
    )

    conversation_history: str = dspy.InputField(
        default="",
        desc="Previous questions and SQL queries from this conversation for context",
    )

    sql_query: str = dspy.OutputField(
        desc="""DuckDB-compatible SQL query (SELECT only, no DROP/DELETE/UPDATE).
        CRITICAL RULES:
        1. Only SELECT queries allowed
        2. Respect column additivity:
           - ADDITIVE: Use SUM() for aggregation
           - NON_ADDITIVE: Use AVG(), MIN(), MAX(), or FIRST() - NEVER SUM
           - SEMI_ADDITIVE: Use for GROUP BY, ORDER BY, WHERE only
        3. Always include proper GROUP BY for aggregations
        4. Use date filters intelligently (WHERE date >= '2023-01-01')
        5. Table and column names must exactly match data_dictionary
        6. LIMIT results to max 1000 rows if not specified
        """
    )

    explanation: str = dspy.OutputField(
        desc="Clear, concise explanation of what the query does and what insights it provides"
    )

    relevant_tables: str = dspy.OutputField(
        desc="Comma-separated list of table names used in the query"
    )

    confidence: float = dspy.OutputField(
        desc="""Confidence score 0.0-1.0 indicating query quality and relevance.
        1.0 = High confidence, query exactly matches question with correct tables/columns
        0.7-0.9 = Good confidence, query is reasonable but may have assumptions
        0.5-0.7 = Moderate confidence, question is ambiguous or data may not fully support it
        < 0.5 = Low confidence, should ask clarifying questions"""
    )

    clarifying_questions: str = dspy.OutputField(
        default="",
        desc="""Optional clarifying questions to ask user if confidence is low or question is ambiguous.
        Provide as comma-separated list. Leave empty if confidence is high.""",
    )


class NaturalLanguageToSQLModule(dspy.Module):
    """DSPy module for converting natural language to SQL using chain-of-thought reasoning."""

    def __init__(self):
        super().__init__()
        self.generate_sql = dspy.ChainOfThought(NaturalLanguageToSQLSignature)

    def forward(
        self, question: str, data_dictionary: str, conversation_history: str = ""
    ):
        """
        Convert natural language question to SQL query.

        Args:
            question: User's natural language question
            data_dictionary: JSON string of available tables/columns
            conversation_history: Previous conversation context

        Returns:
            DSPy prediction with sql_query, explanation, relevant_tables, confidence, clarifying_questions
        """
        result = self.generate_sql(
            question=question,
            data_dictionary=data_dictionary,
            conversation_history=conversation_history,
        )

        return result


class SQLValidator:
    """Validates SQL queries for safety and correctness."""

    # Keywords that are forbidden in user-generated SQL
    FORBIDDEN_KEYWORDS = [
        "DROP",
        "DELETE",
        "UPDATE",
        "INSERT",
        "ALTER",
        "CREATE",
        "TRUNCATE",
        "GRANT",
        "REVOKE",
        "EXEC",
        "EXECUTE",
    ]

    @staticmethod
    def validate_query(sql: str) -> tuple[bool, str]:
        """
        Validate SQL query for safety and correctness.

        Args:
            sql: SQL query string to validate

        Returns:
            Tuple of (is_valid, error_message)
            - is_valid: True if query passes all checks, False otherwise
            - error_message: Empty string if valid, error description if invalid
        """
        if not sql or not sql.strip():
            return False, "SQL query is empty"

        sql_stripped = sql.strip()
        sql_upper = sql_stripped.upper()

        # Must start with SELECT
        if not sql_upper.startswith("SELECT"):
            return (
                False,
                "Only SELECT queries are allowed. Query must start with SELECT.",
            )

        # Check for forbidden keywords
        for keyword in SQLValidator.FORBIDDEN_KEYWORDS:
            # Use word boundaries to avoid false positives (e.g., "SELECT" contains "ELECT")
            if f" {keyword} " in f" {sql_upper} " or sql_upper.startswith(
                f"{keyword} "
            ):
                return (
                    False,
                    f"Forbidden keyword detected: {keyword}. Only SELECT queries are allowed.",
                )

        # Check for semicolons (could indicate SQL injection attempt)
        semicolon_count = sql_stripped.count(";")
        if semicolon_count > 1:
            return (
                False,
                "Multiple SQL statements detected (multiple semicolons). Only single SELECT queries allowed.",
            )

        # Remove trailing semicolon for validation if present
        if sql_stripped.endswith(";"):
            sql_for_validation = sql_stripped[:-1].strip()
        else:
            sql_for_validation = sql_stripped

        # Basic length check (prevent extremely long queries)
        if len(sql_for_validation) > 10000:
            return False, "Query is too long (max 10,000 characters)"

        return True, ""

    @staticmethod
    def add_safety_limits(sql: str, max_rows: int = 1000) -> str:
        """
        Add LIMIT clause to SQL query if not present for safety.

        Args:
            sql: SQL query string
            max_rows: Maximum number of rows to return (default 1000)

        Returns:
            SQL query with LIMIT clause added
        """
        sql_upper = sql.upper()

        # If query already has LIMIT, return as-is
        if "LIMIT" in sql_upper:
            return sql

        # Add LIMIT clause
        sql_stripped = sql.strip()
        if sql_stripped.endswith(";"):
            # Insert LIMIT before semicolon
            return sql_stripped[:-1] + f" LIMIT {max_rows};"
        else:
            # Add LIMIT at the end
            return sql_stripped + f" LIMIT {max_rows}"
