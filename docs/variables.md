# Environment Variables

IceFrame uses environment variables for configuration, authentication, and AI agent settings. You can set these in your shell or in a `.env` file in your project root.

## Catalog Configuration

These variables configure the connection to your Iceberg catalog.

| Variable | Description | Required | Default |
| :--- | :--- | :--- | :--- |
| `ICEBERG_CATALOG_URI` | The URI of your Iceberg catalog (e.g., `https://catalog.dremio.cloud/api/iceberg`). | Yes | - |
| `ICEBERG_CATALOG_TYPE` | The type of catalog to use. | No | `rest` |
| `ICEBERG_WAREHOUSE` | The warehouse location (e.g., `s3://my-bucket/warehouse`). | No | - |
| `ICEBERG_TOKEN` | Bearer token for authentication (alias: `ICEBERG_CATALOG_TOKEN`). | No | - |
| `ICEBERG_OAUTH2_SERVER_URI` | URI for the OAuth2 server if using OAuth. | No | - |
| `ICEBERG_CREDENTIAL` | Credential for catalog authentication (used in MCP). | No | - |
| `ICEBERG_CREDENTIAL_VENDING` | Value for `X-Iceberg-Access-Delegation` header if using credential vending. | No | - |

### Custom Headers

You can pass custom headers to the catalog request by prefixing environment variables with `ICEBERG_HEADER_`. Underscores in the key are replaced with hyphens.

**Example:**
`ICEBERG_HEADER_X_Custom_Auth=secret` becomes header `X-Custom-Auth: secret`.

## Dependency Environment Variables

IceFrame relies on several underlying libraries that may use their own environment variables, particularly for cloud storage and authentication.

### Cloud Storage (AWS / S3)
Used by `pyiceberg`, `s3fs`, `deltalake`, `pyarrow`.

| Variable | Description |
| :--- | :--- |
| `AWS_ACCESS_KEY_ID` | AWS Access Key. |
| `AWS_SECRET_ACCESS_KEY` | AWS Secret Key. |
| `AWS_SESSION_TOKEN` | AWS Session Token (for temporary credentials). |
| `AWS_REGION` | AWS Region (e.g., `us-east-1`). |
| `AWS_PROFILE` | Name of the AWS profile to use from `~/.aws/credentials`. |
| `AWS_ENDPOINT_URL` | Custom endpoint URL (useful for MinIO or S3-compatible services). |

### Cloud Storage (Google Cloud / GCS)
Used by `gcsfs`, `pyiceberg`, `deltalake`.

| Variable | Description |
| :--- | :--- |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to the JSON key file for the service account. |

### Cloud Storage (Azure)
Used by `adlfs`, `pyiceberg`, `deltalake`.

| Variable | Description |
| :--- | :--- |
| `AZURE_STORAGE_CONNECTION_STRING` | Connection string for the storage account. |
| `AZURE_STORAGE_ACCOUNT_NAME` | Storage account name. |
| `AZURE_STORAGE_ACCOUNT_KEY` | Storage account key. |
| `AZURE_CLIENT_ID` | Client ID for service principal. |
| `AZURE_CLIENT_SECRET` | Client Secret for service principal. |
| `AZURE_TENANT_ID` | Tenant ID for service principal. |

### HuggingFace
Used by `datasets`.

| Variable | Description |
| :--- | :--- |
| `HF_TOKEN` | Authentication token for private datasets or higher rate limits. |
| `HF_HOME` | Directory where datasets and models are cached (default: `~/.cache/huggingface`). |

### HTTP Proxies
Used by `requests` and most other network libraries.

| Variable | Description |
| :--- | :--- |
| `HTTP_PROXY` | Proxy URL for HTTP requests. |
| `HTTPS_PROXY` | Proxy URL for HTTPS requests. |
| `NO_PROXY` | Comma-separated list of hosts to bypass the proxy. |

## AI Agent Configuration

These variables configure the AI Agent and LLM provider.

| Variable | Description | Default |
| :--- | :--- | :--- |
| `ICEFRAME_LLM_PROVIDER` | Explicitly set the LLM provider (`openai`, `anthropic`, `gemini`). If not set, it is auto-detected from API keys. | Auto-detect |
| `ICEFRAME_LLM_MODEL` | Explicitly set the model name to use. | Provider default |
| `OPENAI_API_KEY` | API key for OpenAI. | - |
| `ANTHROPIC_API_KEY` | API key for Anthropic. | - |
| `GOOGLE_API_KEY` | API key for Google Gemini (alias: `GEMINI_API_KEY`). | - |

## Example .env File

```env
# Catalog
ICEBERG_CATALOG_URI=https://catalog.example.com
ICEBERG_TOKEN=your-catalog-token
ICEBERG_WAREHOUSE=s3://my-datalake/warehouse

# AI Agent
OPENAI_API_KEY=sk-...
```
