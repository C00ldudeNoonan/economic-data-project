# econ_data_platform

This is a [Dagster](https://dagster.io/) project scaffolded with [`dagster project scaffold`](https://docs.dagster.io/getting-started/create-new-project).

## Getting started

First, install your Dagster code location as a Python package. By using the --editable flag, pip will install your Python package in ["editable mode"](https://pip.pypa.io/en/latest/topics/local-project-installs/#editable-installs) so that as you develop, local code changes will automatically apply.

```bash
pip install -e ".[dev]"
```

Then, start the Dagster UI web server:

```bash
dagster dev
```

Open http://localhost:3000 with your browser to see the project.


## Google Drive Setup
1. Create a new project in the [Google Cloud Console](https://console.cloud.google.com/).
2. Enable the Google Drive API for your project.
3. Create Service account credentials and download the JSON key file. and save in enviroment variable
4. Share the Google Drive folder with the service account email address.

## Project Structure