# URL Shortener

A Spring Boot service for creating short links, retrieving their metadata, redirecting to their destinations, and publishing asynchronous link-creation events.

## Requirements

- Java 21
- Docker Engine with the Docker Compose plugin (for the local MongoDB, Redis, and Kafka dependencies below)
- Gradle is not required separately; the repository includes the Gradle Wrapper

The app listens on port `8080`. MongoDB is required for startup and persistent link operations. Redis is optional at runtime; lookups fall back to MongoDB if Redis is unavailable. Kafka is required for event publishing and asynchronous processing.

## Run locally

1. Start the local dependencies from the repository root:

   ```powershell
   docker compose up -d
   docker compose ps
   ```

   Compose starts MongoDB at `localhost:27017`, Redis at `localhost:6379`, and a single-node Kafka broker at `localhost:9092`. Wait for Kafka to finish starting before launching the app. Inspect startup output with `docker compose logs kafka` if the broker is not ready.

2. Run the app with the Gradle Wrapper:

   ```powershell
   .\gradlew.bat bootRun
   ```

   On macOS/Linux, use `./gradlew bootRun`.

   To build an executable JAR instead:

   ```powershell
   .\gradlew.bat bootJar
   java -jar .\build\libs\url-shortner-0.0.1-SNAPSHOT.jar
   ```

3. Open the interactive API documentation at [http://localhost:8080/swagger-ui.html](http://localhost:8080/swagger-ui.html). The OpenAPI document is at [http://localhost:8080/v3/api-docs](http://localhost:8080/v3/api-docs).

Stop the app with `Ctrl+C`, then stop the dependencies with `docker compose down`. MongoDB and Kafka data are kept in named Docker volumes. To remove those volumes and their data as well, run `docker compose down --volumes`.

In an IDE, import the project as a Gradle project, select a Java 21 JDK, and run `com.example.url_shortner.UrlShortnerApplication`. MongoDB must be available when the application starts because it creates indexes during startup.
Start the Compose dependencies first. When running the app in an IDE on the same machine, the default `localhost` service addresses work unchanged.

## Configuration

The defaults are defined in `src/main/resources/application.yml`. Override them with environment variables:

| Variable | Default | Purpose |
| --- | --- | --- |
| `SERVER_PORT` | `8080` | HTTP server port |
| `MONGODB_URI` | `mongodb://localhost:27017/linkdb` | MongoDB connection URI |
| `REDIS_HOST` | `localhost` | Redis host |
| `REDIS_PORT` | `6379` | Redis port |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address used by the producer and consumer |
| `APP_BASE_URL` | `http://localhost:8080` | Base URL included in generated short links |
| `LINK_CACHE_TTL` | `3m` | TTL for cached link lookups |
| `APP_CLEANUP_CRON` | `0 0 * * * *` | Cron schedule for expired-link cleanup |

For example, in PowerShell:

```powershell
$env:MONGODB_URI = "mongodb://localhost:27017/linkdb"
$env:REDIS_HOST = "localhost"
$env:REDIS_PORT = "6379"
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
$env:APP_BASE_URL = "http://localhost:8080"
.\gradlew.bat bootRun
```

The link API uses MongoDB as its persistent store. Redis is a cache and can be unavailable without preventing MongoDB-backed lookups. Kafka must be available to use the event endpoint; it publishes to the `link-create-requests` topic, which the application also consumes.

## Write and redirect flows

MongoDB is the source of truth for link records, expiration timestamps, and click counts. There are two ways to create a link:

1. **Synchronous REST write:** `POST /api/v1/links` → `LinkApiController` → `MongoLinkServiceImpl` → MongoDB. The response is `201 Created` after the link document is saved.
2. **Asynchronous Kafka write:** Java `NativeKafkaCliProducer` (exposed by `POST /api/v1/events/link-create-requests`) → Kafka topic `link-create-requests` → Spring `LinkCreateRequestedConsumer` → MongoDB. The HTTP response is `202 Accepted` once Kafka acknowledges publication; it does not mean the consumer has saved the link. The consumer acknowledges the Kafka message only after persistence succeeds.

Both flows ultimately persist link documents in MongoDB. Kafka is the queue for asynchronous creation, not a second link database. The consumer evicts the relevant Redis entry after saving a link or recognizing an already-processed custom code.

Redirects use a Redis read-through cache:

1. `GET /{code}` checks Redis for the destination URL.
2. On a cache hit, the service returns the URL from Redis and increments the MongoDB click count.
3. On a cache miss, the service reads the link from MongoDB, rejects missing or expired links, caches an active destination, and redirects.
4. Cache entries expire after `LINK_CACHE_TTL` or sooner if the link expires. If Redis is unavailable or a cache operation fails, the service falls back to MongoDB; MongoDB remains authoritative.

The link metadata endpoint reads MongoDB directly. Redis is a disposable cache, not a source of truth.

## Failure behavior and recovery

- Invalid URLs and request validation errors return `400`; duplicate aliases in the synchronous link API also return `400`. Unknown or expired codes return `404`. Unexpected request or persistence failures are logged and returned as `500`.
- A Redis read failure is treated as a cache miss and the lookup continues against MongoDB. Cache write or eviction failures are logged; they do not fail the request or undo a MongoDB write. A failed eviction can leave a stale redirect cached until its TTL expires.
- A MongoDB failure during synchronous link creation fails the request. During event processing, the consumer does not acknowledge before the MongoDB save succeeds, so a failed save is retried by Kafka's consumer error handler.
- The consumer error handler retries a failed message three times with a one-second fixed backoff, then publishes it to `<source-topic>.DLT` (for example, `link-create-requests.DLT`). Configure the dead-letter topic with enough partitions for the source topic; the handler routes to the same partition number. Monitor and arrange redrive or manual review of dead-letter messages operationally; this service does not provide a DLT inspection or replay API.
- Re-delivery is possible. If a custom code already exists for the same destination URL, the consumer treats the event as already processed, evicts its cache entry, and acknowledges it without another save. If the code exists for a different URL, processing fails and the message follows the retry/dead-letter path.
- Redirects continue if updating the click count fails: the failure is logged, but the redirect succeeds. Click counts are therefore best-effort rather than guaranteed analytics.

## Event contract changes

Kafka event payloads are JSON records with `eventId`, `schemaVersion`, `customCode`, `targetUrl`, `ttlSeconds`, and `createdAt`. Producers set `schemaVersion` to `1`; the event is keyed by `customCode` when present. The consumer checks that `eventId`, `targetUrl`, and `ttlSeconds` are present, but it does not currently branch on or reject unknown schema versions.

There is no schema registry, version-specific deserializer, or automatic event migration. When changing the contract, keep existing fields and meanings compatible with deployed consumers, add new fields as optional where possible, and coordinate breaking changes with all producers and consumers. Test old and new payloads before rollout; a malformed or otherwise unprocessable record is retried and can end up in the DLT.

## API usage

### Create a short link

`POST /api/v1/links` creates a link. `targetUrl` is required; `customCode` is optional and, when supplied, must contain 4–16 letters, digits, underscores, or hyphens. `ttlSeconds` is optional and must be between 60 and 31,536,000.

```sh
curl -X POST http://localhost:8080/api/v1/links \
  -H "Content-Type: application/json" \
  -d '{"targetUrl":"https://example.com/articles/long-path","customCode":"read-more","ttlSeconds":86400}'
```

Returns `201 Created` with the short code, short URL, destination, timestamps, and click count. Omit `customCode` to have the service generate a code.

### Get link metadata

```sh
curl http://localhost:8080/api/v1/links/read-more
```

Returns `200 OK` with link metadata, or `404 Not Found` if the code does not exist or has expired.

### Follow a short link

```sh
curl -i http://localhost:8080/read-more
```

Returns `302 Found` with a `Location` header pointing to the destination. Use `curl -i` without `-L` to inspect the redirect rather than follow it.

### Publish an asynchronous link-creation event

`POST /api/v1/events/link-create-requests` requires a target URL and a positive TTL in seconds. The optional custom code follows the same format as the link API.

```sh
curl -X POST http://localhost:8080/api/v1/events/link-create-requests \
  -H "Content-Type: application/json" \
  -d '{"targetUrl":"https://example.com/articles/long-path","customCode":"read-later","ttlSeconds":86400}'
```

Returns `202 Accepted` with the published event after Kafka acknowledges it. The consumer processes the event asynchronously and stores the resulting link.

## Tests

Run the test suite with the Gradle Wrapper:

```powershell
.\gradlew.bat test
```

On macOS/Linux, run `./gradlew test`.

`UrlShortnerEndToEndTest` exercises HTTP link creation and redirects, Redis cache hits, Kafka event publication and persistence, and idempotent duplicate-message delivery. It uses in-process mocks for MongoDB, Redis, and the Kafka broker, so these tests do not require external services.

## Current limitations

- MongoDB is required at startup for index creation and for persistent link operations. Redis is optional at runtime, but Kafka is needed for the asynchronous event API and consumer.
- The asynchronous API has no status endpoint to check when a published event has been processed. Callers may need to poll link metadata or otherwise coordinate out of band.
- Duplicate event handling is idempotent for a repeated custom code and matching destination, but it does not reconcile changed TTLs or other event fields. A different destination for an existing custom code is a conflict, not an update.
- Click counting is not guaranteed under database failures, and cache invalidation failures can temporarily serve stale destinations until cache expiry.
- Dead-letter messages require operator monitoring and replay procedures; no built-in DLT management, alerting, or schema migration workflow is provided.

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