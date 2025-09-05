import dagster as dg
import dspy
from pydantic import BaseModel

lm = dspy.LM("openai/gpt-4o-mini", api_key=dg.EnvVar("OPENAI_API_KEY"))

dspy.configure(lm=lm)

class FinancialConditionIndex(BaseModel):
    """
    Financial Condition Index
    """
    financial_condition_index: float
    financial_condition_index_description: str

class

def analyze_market_data(datasets: List[str]):
    for dataset in datasets:
        pass

    
