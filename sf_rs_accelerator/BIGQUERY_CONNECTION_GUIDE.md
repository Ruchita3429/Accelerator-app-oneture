# Connecting DataBridge to Google BigQuery

DataBridge supports the following BigQuery authentication methods:

1. **Service-account JSON** — upload a JSON key directly in the application.
2. **Application Default Credentials (ADC)** — authenticate the Google Cloud Python client through your local Google Cloud CLI session or an attached Google Cloud service account.

For a developer running this POC locally, ADC with a user account is usually the simplest method. Service-account JSON is useful for a dedicated demo identity or when interactive user login is unavailable. Google recommends avoiding long-lived service-account keys when a safer credential method is available.

## Information required for either method

Obtain the following from the Google Cloud project owner:

- **Project ID** — for example, `example-data-project`. Use the project ID, not its display name or numeric project number.
- The datasets and tables that the migration is allowed to read.
- The BigQuery location used by those datasets, such as `US`, `EU`, or a regional location.

You can find the project ID in the project selector at the top of the [Google Cloud console](https://console.cloud.google.com/) or, after installing the CLI, with:

```bash
gcloud projects list
```

The BigQuery API must be enabled in the project used to run queries:

```bash
gcloud services enable bigquery.googleapis.com --project=YOUR_PROJECT_ID
```

If that command is denied, ask the project administrator to enable **BigQuery API**. Enabling an API is an administrative setup action and is not required on every login.

## Required BigQuery access

Grant the user or service account only the access required for the selected source:

| Scope | Role | Purpose |
| --- | --- | --- |
| Query/execution project | BigQuery Job User (`roles/bigquery.jobUser`) | Create the query jobs used to read source data |
| Source dataset or source project | BigQuery Data Viewer (`roles/bigquery.dataViewer`) | List tables, read table metadata, and query table data |

Granting Data Viewer on individual source datasets is preferable when the client does not want the identity to read every dataset in the project. Datasets that are not granted to the identity will not appear in DataBridge.

The DDL metadata browser uses the same read-only table metadata access. The offline Query Migrator and paste/upload mode of the DDL Migrator do not need a BigQuery connection.

## Method 1: service-account JSON

This method does not require the Google Cloud CLI on the machine running DataBridge.

### Step 1: create or select a service account

An administrator should perform these steps:

1. Open **Google Cloud console → IAM & Admin → Service Accounts**.
2. Select the correct project.
3. Click **Create service account**.
4. Enter a name such as `databridge-poc-reader` and an explanatory description.
5. Finish creating the account.
6. Grant **BigQuery Job User** on the project where query jobs run.
7. Grant **BigQuery Data Viewer** on each source dataset, or on the source project for a simple POC.

If an approved service account already exists, reuse it instead of creating another one.

### Step 2: create and download the JSON key

1. Open the service account from the **Service Accounts** page.
2. Open the **Keys** tab.
3. Select **Add key → Create new key**.
4. Select **JSON**.
5. Click **Create**. The browser downloads the JSON file once; Google does not provide the private key for download again.

Some organizations disable service-account key creation. In that case, use ADC with user credentials or the impersonation option described later rather than trying to bypass the organization policy.

### Step 3: connect in DataBridge

1. Start DataBridge:

   ```bash
   venv/bin/python -m streamlit run app_final.py
   ```

2. Select either BigQuery pipeline route under **Pipeline Destination**.
3. Expand **Google Cloud — BigQuery** in the sidebar.
4. Enter the **Project ID** where DataBridge will create BigQuery query jobs.
5. Choose **Service-account JSON** under **Authentication**.
6. Upload the downloaded JSON file.
7. Click **Connect BigQuery**.
8. Confirm that the application reports `BigQuery connected` and shows the number of visible datasets.

The application reads the uploaded JSON in memory and constructs a BigQuery client. It does not intentionally write the uploaded credential to the project directory.

### Step 4: optional command-line verification

If the CLI is installed and an administrator permits local activation, verify the same key with:

```bash
gcloud auth activate-service-account SERVICE_ACCOUNT_EMAIL \
  --key-file=/absolute/path/to/service-account.json \
  --project=YOUR_PROJECT_ID

bq ls --project_id=YOUR_PROJECT_ID

bq query --project_id=YOUR_PROJECT_ID --use_legacy_sql=false --dry_run \
  'SELECT 1'
```

After testing, remove that service account from the CLI's local credential store if it is no longer needed:

```bash
gcloud auth revoke SERVICE_ACCOUNT_EMAIL
```

Do not commit the JSON file, place it in a shared folder, paste it into logs, or send it through chat or email. Ask the project administrator to delete the key if it is exposed or no longer required.

## Method 2: Application Default Credentials

ADC lets the Python BigQuery library locate credentials automatically. On a local workstation, `gcloud auth application-default login` creates a separate local credential file for client libraries.

`gcloud auth login` and `gcloud init` authenticate the **CLI**. They do not, by themselves, create the ADC credentials used by DataBridge. You must also run `gcloud auth application-default login`.

### Step 1: install the Google Cloud CLI

#### macOS with Homebrew

```bash
brew update
brew install --cask gcloud-cli
```

Close and reopen the terminal if `gcloud` is not immediately on `PATH`, then verify:

```bash
gcloud version
bq version
```

If the CLI was downloaded as Google's archive instead, run its `install.sh` script and open a new terminal after it updates the shell configuration.

#### Windows

1. Download and run the official [Google Cloud CLI installer](https://cloud.google.com/sdk/docs/install-sdk#windows).
2. Keep **Install bundled Python** selected unless a supported Python installation is already configured.
3. Allow the installer to add `gcloud` to `PATH`.
4. Open a new PowerShell or Google Cloud CLI Shell window.
5. Run `gcloud version` and `bq version`.

#### Debian or Ubuntu

Use Google's signed package repository instructions in the [official Linux installation guide](https://cloud.google.com/sdk/docs/install-sdk#deb). Install the `google-cloud-cli` package, then run:

```bash
gcloud version
bq version
```

### Step 2: install or verify the relevant CLI components

Google calls these **components**, not extensions. Do not install every available component: most are unrelated emulators, deployment tools, or command groups and add no value to this connection.

For this project:

| Component | Need | Reason |
| --- | --- | --- |
| `core` / `gcloud` | Required for local ADC | Creates and manages the ADC login |
| `bq` | Recommended | Tests BigQuery visibility and query-job permission from the terminal |
| `gsutil` | Optional | Legacy Google Cloud Storage tool; DataBridge writes to Amazon S3 and does not use it |
| `gcloud-crc32c` | Optional | Speeds/checks Cloud Storage transfers; not used by this POC |
| `alpha`, `beta`, `preview` | Not required | The connection uses generally available authentication and BigQuery APIs |

List every component available for your installed CLI version and its status:

```bash
gcloud components list --show-versions
```

Update all currently installed components:

```bash
gcloud components update
```

For an archive, interactive-installer, Windows-installer, or current Homebrew installation, install all components relevant to this POC with:

```bash
gcloud components install bq gsutil gcloud-crc32c
```

Only `bq` is useful for BigQuery connection diagnostics; `gsutil` and `gcloud-crc32c` are included above for a more complete general cloud workstation. The application connection itself does not require those two components.

If `gcloud components install` reports that the component manager is disabled, the CLI was installed through a package manager that manages components itself, commonly APT or YUM. Update/install the corresponding operating-system packages instead. Do not mix the component manager with an APT/YUM-managed installation.

### Step 3: initialize the CLI user and project

```bash
gcloud init
gcloud config set project YOUR_PROJECT_ID
gcloud auth list
gcloud config get-value project
```

During `gcloud init`, choose the Google account that has BigQuery access and select the correct project. `gcloud auth list` should mark the intended CLI account as active.

### Step 4: create local ADC credentials

Run:

```bash
gcloud auth application-default login
```

Complete the browser sign-in using the Google account that has the required BigQuery roles. On macOS and Linux, the resulting ADC file is normally stored at:

```text
~/.config/gcloud/application_default_credentials.json
```

On Windows, it is normally stored below:

```text
%APPDATA%\gcloud\application_default_credentials.json
```

Set the project used for client-library quota and billing:

```bash
gcloud auth application-default set-quota-project YOUR_PROJECT_ID
```

If that command reports a `serviceusage.services.use` permission error, ask the project administrator for **Service Usage Consumer** (`roles/serviceusage.serviceUsageConsumer`) on the quota project, or ask them which approved quota project to use.

### Step 5: make sure an old environment variable is not overriding ADC

ADC checks `GOOGLE_APPLICATION_CREDENTIALS` before the local ADC file. A stale path can therefore override a successful login.

On macOS or Linux:

```bash
printenv GOOGLE_APPLICATION_CREDENTIALS
unset GOOGLE_APPLICATION_CREDENTIALS
```

On PowerShell:

```powershell
$env:GOOGLE_APPLICATION_CREDENTIALS
Remove-Item Env:GOOGLE_APPLICATION_CREDENTIALS -ErrorAction SilentlyContinue
```

Only unset it when you intend to use the user credentials created by `gcloud auth application-default login`.

### Step 6: verify ADC using the same Python environment as DataBridge

From the project folder, run the following after replacing the project ID:

```bash
venv/bin/python - <<'PY'
from google.cloud import bigquery

project_id = "YOUR_PROJECT_ID"
client = bigquery.Client(project=project_id)
datasets = list(client.list_datasets(project=project_id))
print("Authenticated project:", client.project)
print("Visible datasets:", len(datasets))
for dataset in datasets[:10]:
    print("-", dataset.dataset_id)
PY
```

This verifies the same ADC lookup and Python BigQuery library used by the application. It lists metadata only and does not scan table rows.

Optionally verify the CLI's separate credential path and query-job permission:

```bash
bq ls --project_id=YOUR_PROJECT_ID

bq query --project_id=YOUR_PROJECT_ID --use_legacy_sql=false --dry_run \
  'SELECT 1'
```

### Step 7: connect in DataBridge

1. Start DataBridge from the same operating-system user account where ADC was configured.
2. Select a BigQuery pipeline route.
3. Expand **Google Cloud — BigQuery**.
4. Enter `YOUR_PROJECT_ID`.
5. Choose **Application Default Credentials**.
6. Click **Connect BigQuery**.
7. Confirm that the sidebar reports `BigQuery connected`.

There is no credential file to upload in this mode.

### Optional: ADC through service-account impersonation

For organizations that do not permit keys, an authorized user can create local ADC that impersonates a service account:

```bash
gcloud auth application-default login \
  --impersonate-service-account=SERVICE_ACCOUNT_EMAIL
```

The signed-in user needs **Service Account Token Creator** (`roles/iam.serviceAccountTokenCreator`) on that service account. Python is among the client-library environments supported by this ADC impersonation flow.

## Troubleshooting

| Symptom | Likely cause | Resolution |
| --- | --- | --- |
| `Your default credentials were not found` | ADC login was not run for the user starting Streamlit | Run `gcloud auth application-default login`, then restart Streamlit |
| CLI login succeeded but DataBridge still cannot connect | CLI credentials and ADC credentials are separate | Run the application-default login command and the Python verification above |
| DataBridge uses the wrong identity | `GOOGLE_APPLICATION_CREDENTIALS` points to another credential file | Inspect and, when appropriate, unset the environment variable |
| `gcloud: command not found` | CLI install directory is missing from `PATH` | Open a new terminal or rerun the installer and enable shell/PATH integration |
| `403 ... API has not been used or is disabled` | BigQuery API is disabled in the execution project | Ask an administrator to enable `bigquery.googleapis.com` |
| `Access Denied: Project ... bigquery.jobs.create` | Missing query-job permission | Grant BigQuery Job User on the execution project |
| Datasets or tables are missing | The identity lacks dataset metadata/data access | Grant BigQuery Data Viewer on the intended source datasets |
| Quota-project warning or `serviceusage.services.use` denial | ADC cannot charge quota to the selected project | Set an approved quota project and grant Service Usage Consumer there |
| Service-account key option is unavailable | Organization policy disables key creation | Use user ADC or approved service-account impersonation |
| Browser login completed but an older Streamlit process still fails | The process started before credentials were created or uses another OS account | Stop and restart Streamlit from the authenticated account and project folder |

To remove local user ADC credentials when the POC is finished:

```bash
gcloud auth application-default revoke
```

This revokes/removes ADC credentials. It is separate from removing the account used by the `gcloud` CLI.

## Official references

- [Install the Google Cloud CLI](https://cloud.google.com/sdk/docs/install-sdk)
- [Install the Google Cloud CLI with Homebrew](https://cloud.google.com/sdk/docs/downloads-homebrew)
- [Manage Google Cloud CLI components](https://cloud.google.com/sdk/docs/components)
- [Set up ADC for local development](https://cloud.google.com/docs/authentication/set-up-adc-local-dev-environment)
- [How ADC finds credentials](https://cloud.google.com/docs/authentication/application-default-credentials)
- [Create and manage service-account keys](https://cloud.google.com/iam/docs/keys-create-delete)
- [BigQuery IAM roles and permissions](https://cloud.google.com/bigquery/docs/access-control)
- [BigQuery `bq` command reference](https://cloud.google.com/bigquery/docs/reference/bq-cli-reference)
