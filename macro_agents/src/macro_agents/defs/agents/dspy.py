import dagster as dg
import dspy

lm = dspy.LM("openai/gpt-4o-mini", api_key=dg.EnvVar("OPENAI_API_KEY"))

dspy.configure(lm=lm)
