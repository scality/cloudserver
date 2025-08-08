# Backend Mocks for CloudServer Optimization

This module provides mocking functionality for backend calls to improve development and testing performance. When enabled, these mocks will call the real backend functions once to cache results, then return cached responses with configurable delays for subsequent calls.

## Environment Variables

### Mock Control Flags (Independent)
- `MOCK_DOAUTH=true` - Enable doAuth mocking
- `MOCK_METADATA=true` - Enable metadata validation mocking  
- `MOCK_STORE_OBJECT=true` - Enable store object mocking

### Base Optimization Flag (Optional)
- `OPTIM_CLOUDSERVER_PUT_OBJECT=true` - Enables existing optimizations (independent of mocks)

### Delay Configuration
- `MOCK_DOAUTH_DELAY_MS` - Delay for doAuth mock (default: 10ms)
- `MOCK_METADATA_DELAY_MS` - Delay for metadata mock (default: 8ms)
- `MOCK_STORE_OBJECT_DELAY_MS` - Delay for store object mock (default: 6ms)

## Usage

### Enable All Mocks with Default Delays
```bash
export MOCK_DOAUTH=true
export MOCK_METADATA=true
export MOCK_STORE_OBJECT=true
```

### Enable with Custom Delays
```bash
export MOCK_DOAUTH=true
export MOCK_DOAUTH_DELAY_MS=5
export MOCK_METADATA=true
export MOCK_METADATA_DELAY_MS=3
export MOCK_STORE_OBJECT=true
export MOCK_STORE_OBJECT_DELAY_MS=2
```

### Enable Specific Mocks Only
```bash
# Only mock doAuth
export MOCK_DOAUTH=true

# Only mock metadata operations
export MOCK_METADATA=true

# Combine with base optimizations if desired
export OPTIM_CLOUDSERVER_PUT_OBJECT=true
export MOCK_DOAUTH=true
```

## How It Works

1. **First Call**: The mock calls the real backend function and caches the result
2. **Subsequent Calls**: Returns the cached result after the specified delay
3. **Error Handling**: Errors are also cached and returned appropriately
4. **Logging**: Debug logs show when mocks are used and cache status

## Functions Mocked

### doAuth (auth.server.doAuth)
- **Location**: `lib/api/api.js`
- **Purpose**: Authentication and authorization
- **Default Delay**: 10ms
- **Cache Key**: Single global cache (assumes consistent auth for session)

### standardMetadataValidateBucketAndObj
- **Location**: `lib/metadata/metadataUtils.js`
- **Purpose**: Bucket and object metadata validation
- **Default Delay**: 8ms
- **Cache Key**: Single global cache (first successful result)

### createAndStoreObject
- **Location**: `lib/api/apiUtils/object/createAndStoreObject.js`
- **Purpose**: Object creation and storage
- **Default Delay**: 6ms
- **Cache Key**: Single global cache (first successful result)

## Development Notes

- Mocks are independent and only require their specific flag to be enabled (e.g., `MOCK_DOAUTH=true`)
- The `OPTIM_CLOUDSERVER_PUT_OBJECT` flag is for existing optimizations and works separately from mocks
- Cache is per-process, so each worker maintains its own cache
- For testing, use `resetMockCaches()` to clear all cached results
- Use `getMockConfig()` to inspect current mock configuration

## Testing

```javascript
const { resetMockCaches, getMockConfig } = require('./backendMocks');

// Reset all caches
resetMockCaches();

// Check configuration
console.log(getMockConfig());
```

## Performance Impact

When enabled, these mocks can significantly reduce response times:
- First request: Normal speed (real backend calls)
- Subsequent requests: ~10ms total for all three operations vs potentially 100ms+ for real backend calls

This is particularly useful for:
- Development environments
- Load testing
- Integration testing where backend speed is not the focus 