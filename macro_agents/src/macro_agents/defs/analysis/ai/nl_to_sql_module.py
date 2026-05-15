"""Natural Language to SQL Module - Converts user questions to SQL queries using DSPy."""

import dspy
import sqlglot
from sqlglot import exp


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
    """Validates SQL queries for safety and correctness.

    Uses sqlglot to parse the query into an AST and verify it is a single
    read-only SELECT. The prior implementation matched forbidden keywords
    as substrings of the raw string, which was bypassable by comments,
    mixed case, and embedded newlines.
    """

    # AST node types that mutate or escape data — reject if present anywhere.
    FORBIDDEN_NODES: tuple[type[exp.Expression], ...] = (
        exp.Insert,
        exp.Update,
        exp.Delete,
        exp.Drop,
        exp.Create,
        exp.Alter,
        exp.TruncateTable,
        exp.Grant,
        exp.Command,  # Generic fallback (e.g. REVOKE, EXECUTE) that sqlglot doesn't model explicitly.
        exp.Transaction,
        exp.Rollback,
        exp.Commit,
    )

    MAX_QUERY_LENGTH = 10000

    @staticmethod
    def validate_query(sql: str) -> tuple[bool, str]:
        """Validate SQL query for safety and correctness.

        Returns ``(is_valid, error_message)``. Parses to AST and rejects any
        statement that isn't a single SELECT — bypass attempts via comments,
        case mixing, or whitespace tricks cannot succeed because the parser
        normalizes them before validation runs.
        """
        if not sql or not sql.strip():
            return False, "SQL query is empty"

        if len(sql) > SQLValidator.MAX_QUERY_LENGTH:
            return False, f"Query is too long (max {SQLValidator.MAX_QUERY_LENGTH} characters)"

        try:
            statements = sqlglot.parse(sql, dialect="duckdb")
        except sqlglot.errors.ParseError as e:
            return False, f"SQL parse error: {e}"

        non_empty = [s for s in statements if s is not None]
        if len(non_empty) == 0:
            return False, "SQL query is empty"
        if len(non_empty) > 1:
            return False, "Multiple SQL statements detected. Only single SELECT queries allowed."

        root = non_empty[0]
        if not isinstance(root, exp.Select) and not (
            isinstance(root, (exp.Union, exp.Subquery)) and root.find(exp.Select) is not None
        ):
            return False, "Only SELECT queries are allowed."

        for node in root.walk():
            if isinstance(node, SQLValidator.FORBIDDEN_NODES):
                return False, f"Forbidden statement type: {type(node).__name__}"

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
