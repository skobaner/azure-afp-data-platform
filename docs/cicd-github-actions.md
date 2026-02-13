# CI/CD with GitHub Actions (Option 1)

This repo now includes two workflows:

- `.github/workflows/infra.yml`: Terraform plan/apply
- `.github/workflows/deploy-apps.yml`: deploy FastAPI + Azure Function

## 1) Create Terraform remote state resources (one-time)

Use a globally unique storage account name.

```bash
az group create --name rg-tfstate-csvplatform --location eastus

az storage account create \
  --name sttfstatecsvplat001 \
  --resource-group rg-tfstate-csvplatform \
  --location eastus \
  --sku Standard_LRS

az storage container create \
  --name tfstate \
  --account-name sttfstatecsvplat001 \
  --auth-mode login
```

## 2) Create Azure AD app + service principal for GitHub OIDC

```bash
az ad app create --display-name "gh-csvplatform-cicd"
```

Capture the app id:

```bash
APP_ID=$(az ad app list --display-name gh-csvplatform-cicd --query "[0].appId" -o tsv)
```

Create service principal:

```bash
az ad sp create --id "$APP_ID"
```

Get subscription + tenant:

```bash
SUBSCRIPTION_ID=$(az account show --query id -o tsv)
TENANT_ID=$(az account show --query tenantId -o tsv)
```

Grant permissions for your deploy resource group(s) and state resource group:

```bash
az role assignment create \
  --assignee "$APP_ID" \
  --role Contributor \
  --scope "/subscriptions/$SUBSCRIPTION_ID"
```

Create federated credential (replace `<ORG>`, `<REPO>`):

```bash
az ad app federated-credential create --id "$APP_ID" --parameters '{
  "name": "github-main",
  "issuer": "https://token.actions.githubusercontent.com",
  "subject": "repo:<ORG>/<REPO>:ref:refs/heads/main",
  "description": "GitHub Actions main branch",
  "audiences": ["api://AzureADTokenExchange"]
}'
```

## 3) Add GitHub repository secrets

GitHub repo -> `Settings` -> `Secrets and variables` -> `Actions`.

Create these **secrets**:

- `AZURE_CLIENT_ID` = `$APP_ID`
- `AZURE_TENANT_ID` = `$TENANT_ID`
- `AZURE_SUBSCRIPTION_ID` = `$SUBSCRIPTION_ID`
- `TF_STATE_RESOURCE_GROUP` = `rg-tfstate-csvplatform`
- `TF_STATE_STORAGE_ACCOUNT` = `sttfstatecsvplat001`
- `TF_STATE_CONTAINER` = `tfstate`
- `TF_STATE_KEY` = `csvplatform-dev.tfstate`
- `SQL_ADMIN_USERNAME` = SQL admin login you want Terraform to use
- `SQL_ADMIN_PASSWORD` = SQL admin password

Create these **repository variables**:

- `PROJECT_NAME` = `csvplatform`
- `AZURE_LOCATION` = `eastus`
- `ENVIRONMENT` = `dev`
- `ALLOWED_IP_ADDRESS` = optional public IP (or empty string)

## 4) Run workflows

- Infra:
  - push changes to `infra/terraform/**` on `main`, or
  - run workflow manually with `apply=true`
- App deploy:
  - push changes to `api/**` or `pipeline/**` on `main`, or
  - run manually from Actions tab

## 5) Recommended sequence

1. Run `Infra (Terraform)` first and ensure apply succeeds.
2. Run `Deploy API + Function`.
3. Test `POST /upload-csv` and `GET /records`.
