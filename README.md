## Warning & Error Code Catalog

| Code | Level | Event Name | Description | Key Context Fields |
| :--- | :--- | :--- | :--- | :--- |
| `warn_001` | `WARN` | `link_expired` | Short link has reached its configured TTL | `requestId`, `shortCode` |
| `warn_002` | `WARN` | `link_not_found` | Requested short link code does not exist in MongoDB | `requestId`, `shortCode` |
| `warn_003` | `WARN` | `validation_failed` | URL format invalid or payload validation failed | `requestId`, `message`, `errors` |
| `warn_004` | `WARN` | `duplicate_code` | Requested custom alias already exists in MongoDB | `requestId`, `shortCode` |
| `error_001`| `ERROR`| `link_persistence_failed` | MongoDB read, write, or update operation failed | `requestId`, `exception` |
| `error_002`| `ERROR`| `kafka_dispatch_failed` | Failure publishing link event to Kafka topic | `requestId`, `topic`, `exception` |
| `error_003`| `ERROR`| `internal_server_error` | Unhandled runtime exception encountered | `requestId`, `exception`, `message` |