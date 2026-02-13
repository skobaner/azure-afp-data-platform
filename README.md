# Azure CSV Data Platform (Terraform + Python)

This project gives you a full starting point for your requirement:

- Blob storage container for raw input CSV files
- Azure SQL DB for processed output rows
- Pipeline that reads CSV from Blob, manipulates rows in Python, and writes to SQL
- API #1 to upload CSV files to Blob
- API #2 to view rows from Azure SQL

## Architecture

1. `POST /upload-csv` (FastAPI) uploads a `.csv` file to Blob container `input-data`.
2. Blob trigger (Azure Function) runs when a new file appears.
3. Function normalizes each CSV row (trim + uppercase + add `processed_utc`) and stores JSON payload rows into SQL table `dbo.processed_rows`.
4. `GET /records` (FastAPI) reads processed rows from SQL.

## Project structure

- `infra/terraform`: Azure infrastructure as code
- `api`: FastAPI service for upload + query
- `pipeline`: Azure Function blob-trigger pipeline
- `.vscode`: recommended VS Code extensions + tasks

## Prerequisites

- Azure subscription
- Terraform >= 1.6
- Azure CLI logged in (`az login`)
- Python 3.11
- Azure Functions Core Tools (for local testing)

## 1) Deploy infrastructure

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars
# edit terraform.tfvars with your values
terraform init
terraform apply
```

Capture outputs:

```bash
terraform output
```

You need:
- `resource_group_name`
- `api_app_name`
- `function_app_name`

## 2) Deploy API app code

From workspace root:

```bash
cd api
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
zip -r ../api.zip . -x "*.venv*" "*__pycache__*"
cd ..
```

Deploy zip:

```bash
az webapp deploy \
  --resource-group <resource_group_name> \
  --name <api_app_name> \
  --src-path api.zip \
  --type zip
```

## 3) Deploy Function pipeline code

From workspace root:

```bash
cd pipeline
zip -r ../pipeline.zip . -x "*__pycache__*"
cd ..
```

Deploy zip:

```bash
az functionapp deployment source config-zip \
  --resource-group <resource_group_name> \
  --name <function_app_name> \
  --src pipeline.zip
```

## 4) Test APIs

Get API URL from Terraform output (`api_app_url`):

### Upload CSV

```bash
curl -X POST "https://<api-host>/upload-csv" \
  -F "file=@/path/to/sample.csv"
```

### View processed SQL rows

```bash
curl "https://<api-host>/records?limit=50"
```

## CI/CD (GitHub Actions)

Use the prebuilt workflows in:

- `.github/workflows/infra.yml`
- `.github/workflows/deploy-apps.yml`

Setup instructions:

- `docs/cicd-github-actions.md`

## Notes

- SQL table is auto-created by the API/function if it does not exist.
- For production, use Key Vault + Managed Identity instead of storing SQL password in app settings.
- Current transformation logic is in `pipeline/function_app.py` (`normalize_row`).
