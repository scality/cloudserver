# cloudserver

This is a **Node.js implementation of the S3 protocol**. It contains:

- S3 API route handlers (`lib/api/`) — 80+ operations
- Authentication and authorization layers (`lib/auth/`)
- Stream-based data path for object storage (`lib/data/`)
- Multi-backend support: file, memory, AWS S3, Azure, GCP, Sproxyd
- Metadata abstraction: LevelDB, MongoDB, bucketd (`lib/metadata/`)
- Git-based internal deps: arsenal, vaultclient, bucketclient, werelogs, utapi, scubaclient
- CommonJS modules, callback-based async (migrating to async/await), Mocha tests

This file provides guidance to Claude Code (claude.ai/code) when working with
code in this repository.

## Project Overview

CloudServer is Scality's open-source, AWS S3-compatible object storage server.
It provides a single S3 API interface to access multiple storage backends
(Scality RING, file, memory, AWS S3, Azure, GCP, etc.). Part of the
S3C/Federation and Zenko stacks.

- **Node.js >= 22** required
- **Package Manager**: Yarn with `--frozen-lockfile`

## Common Commands

### Development

```bash
yarn install --frozen-lockfile     # Install dependencies
yarn start                         # Start S3 + metadata + data servers
yarn dev                           # Development mode with auto-restart (nodemon)
yarn mem_backend                   # Start with in-memory backend (S3BACKEND=mem)
yarn start_mongo                   # Start with MongoDB metadata backend
```

### Testing

```bash
# Unit tests
yarn test                          # Run unit tests (S3BACKEND=mem)
yarn cover test                    # Run unit tests with coverage

# Functional tests (require running server)
yarn ft_awssdk                     # AWS SDK functional tests
yarn ft_awssdk_buckets             # Bucket-specific AWS SDK tests
yarn ft_awssdk_versioning          # Versioning tests
yarn ft_s3cmd                      # S3cmd CLI tests
yarn ft_healthchecks               # Health check tests
yarn ft_test                       # Run all functional tests

# Multi-backend tests
yarn multiple_backend_test         # Multi-backend unit tests
yarn ft_awssdk_external_backends   # External backend functional tests
```

### Linting

```bash
yarn lint                          # ESLint on all JS files
yarn lint_md                       # Markdown linting
```

## Architecture

### Entry Points

- `index.js` → Main S3 API server (port 8000)
- `mdserver.js` → Metadata server (port 9990)
- `dataserver.js` → Data storage server (port 9991)
- `pfsserver.js` → Passthrough filesystem server (port 9992)
- `managementAgent.js` → WebSocket management agent (port 8010)

### Core Components

```
lib/
├── server.js          # S3Server class - main HTTP server with cluster support
├── Config.js          # Configuration management (env vars, config.json)
├── api/               # S3 API implementations (80+ operations)
│   ├── api.js         # Main request router and auth handler
│   └── apiUtils/      # Shared utilities (auth, bucket ops, object ops, quotas)
├── auth/              # Authentication (vault.js for IAM, in_memory/ for local)
├── data/              # Data backend abstraction (file, memory, multiple)
├── metadata/          # Metadata backend abstraction
├── routes/            # Special routes (Backbeat, metadata service, Veeam)
├── kms/               # Key management (file, memory, AWS, KMIP)
├── management/        # Management API and configuration
└── utilities/         # Logging, health checks, XML parsing
```

### Backend Configuration

Set via environment variables. Each backend type abstracts multiple
implementations:

### Data Backends (`S3DATA`)

| Backend | Port | Description |
|---------|------|-------------|
| `file` | 9991 | Local filesystem via `dataserver.js` |
| `multiple` | - | Multi-backend gateway (AWS S3, Azure, GCP, Sproxyd) |
| `mem` | - | In-memory (testing only) |

`scality` is an alias for `multiple`. With `multiple`, objects route to
backends defined in `locationConfig.json` based on location constraints.

### Metadata Backends (`S3METADATA`)

| Backend | Port | Description |
|---------|------|-------------|
| `file` (default) | 9990 | Local LevelDB via `mdserver.js` |
| `scality` | 9000 | External bucketd service (production Scality RING) |
| `mongodb` | 27017+ | MongoDB replica set |
| `mem` | - | In-memory (testing only) |

**file vs scality**: The `file` backend runs a self-contained metadata server
(`mdserver.js`) for development. The `scality` backend connects to external
**bucketd** which uses Raft-based distributed metadata (repd) for production
HA deployments.

### Auth Backends (`S3VAULT`)

| Backend | Port | Description |
|---------|------|-------------|
| `mem` (default) | - | In-memory accounts from `conf/authdata.json` |
| `vault` | 8500 | External Vault IAM service (vaultd) |

### KMS Backends (`S3KMS`)

| Backend | Description |
|---------|-------------|
| `file` (default) | Local file-based key storage |
| `mem` | In-memory (testing only) |
| `kmip` | External KMIP server |
| `aws` | AWS KMS |
| `scality` | Scality KMS |

### Path Configuration

- `S3DATAPATH`: Data storage directory (default: `./localData`)
- `S3METADATAPATH`: Metadata storage directory (default: `./localMetadata`)

### Key Files

- `config.json` - Default configuration
- `constants.js` - Global constants (splitters, limits, service accounts)
- `locationConfig.json` - Location constraints for multi-backend

## Testing Conventions

- **Framework**: Mocha with Sinon for mocking
- **Timeout**: 40000ms global
- **Coverage**: NYC (Istanbul), 80% patch target
- **Reporters**: mocha-multi-reporters (spec + junit XML)

Run a single test file:

```bash
S3BACKEND=mem yarn mocha tests/unit/path/to/test.js --exit
```

Run tests matching a pattern:

```bash
S3BACKEND=mem yarn mocha --grep "pattern" tests/unit --recursive --exit
```

## Code Style

- ESLint with `@scality/eslint-config-scality`
- `mocha/no-exclusive-tests: error` - Never commit `.only()` tests
- Many formatting rules disabled for legacy compatibility
- ECMAScript 2021, CommonJS modules (`sourceType: "script"`)
