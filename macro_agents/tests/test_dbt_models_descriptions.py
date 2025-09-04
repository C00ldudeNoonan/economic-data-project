"""
Test to ensure all dbt models have descriptions in their schema.yml files.
"""
import pytest
import yaml
from pathlib import Path


def test_all_dbt_models_have_descriptions():
    """Test that all dbt models have descriptions in their schema.yml files."""
    dbt_project_path = Path(__file__).parent.parent.parent / "dbt_project"
    models_path = dbt_project_path / "models"
    
    models_without_descriptions = []
    
    # Find all .sql files in the models directory
    sql_files = list(models_path.rglob("*.sql"))
    
    for sql_file in sql_files:
        # Skip if it's in a subdirectory that doesn't have a schema.yml
        model_dir = sql_file.parent
        schema_file = model_dir / "schema.yml"
        
        if not schema_file.exists():
            # Check if there's a schema.yml in a parent directory
            parent_schema = model_dir.parent / "schema.yml"
            if parent_schema.exists():
                schema_file = parent_schema
            else:
                models_without_descriptions.append(f"{sql_file.relative_to(dbt_project_path)} (no schema.yml found)")
                continue
        
        # Read the schema.yml file
        try:
            with open(schema_file, 'r') as f:
                schema_content = yaml.safe_load(f)
        except Exception as e:
            models_without_descriptions.append(f"{sql_file.relative_to(dbt_project_path)} (error reading schema.yml: {e})")
            continue
        
        # Get the model name (without .sql extension)
        model_name = sql_file.stem
        
        # Check if the model has a description in the schema
        model_found = False
        model_has_description = False
        
        if 'models' in schema_content:
            for model in schema_content['models']:
                if model.get('name') == model_name:
                    model_found = True
                    if model.get('description') and model.get('description').strip():
                        model_has_description = True
                    break
        
        if not model_found:
            models_without_descriptions.append(f"{sql_file.relative_to(dbt_project_path)} (not found in schema.yml)")
        elif not model_has_description:
            models_without_descriptions.append(f"{sql_file.relative_to(dbt_project_path)} (missing description)")
    
    if models_without_descriptions:
        pytest.fail(
            f"The following dbt models are missing descriptions: {', '.join(models_without_descriptions)}"
        )
    
    # If we get here, all models have descriptions
    assert len(models_without_descriptions) == 0
