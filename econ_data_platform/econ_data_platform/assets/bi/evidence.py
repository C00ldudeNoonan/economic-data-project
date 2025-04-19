import dagster as dg
import subprocess
import os


@dg.asset(compute_kind="evidence", group_name="reporting",)
def evidence_dashboard():
    """Dashboard built using Evidence showing  metrics."""
    evidence_project_path = dg.file_relative_path(__file__, "../../evidence_project")
    subprocess.run(["npm", "--prefix", evidence_project_path, "install"])
    subprocess.run(["npm", "--prefix", evidence_project_path, "run", "sources"])
    subprocess.run(["npm", "--prefix", evidence_project_path, "run", "build"])