# URL Shortener

A Spring Boot service for creating short links, retrieving their metadata, redirecting to their destinations, and publishing asynchronous link-creation events.

## Requirements

- Java 21
- MongoDB, available at the configured MongoDB URI
- Redis (optional for caching; cache operations fall back to MongoDB when Redis is unavailable)
- Kafka (required for the event-publishing API and asynchronous event processing)
- Gradle is not required separately; the repository includes the Gradle Wrapper

The default local endpoints are MongoDB at `localhost:27017`, Redis at `localhost:6379`, and Kafka at `localhost:9092`. The app listens on port `8080`.

## Run locally

1. Start MongoDB, Redis, and Kafka. Ensure Kafka is reachable from the app and accepts connections on the configured bootstrap server.
2. From the repository root, run the app with the Gradle Wrapper:

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

In an IDE, import the project as a Gradle project, select a Java 21 JDK, and run `com.example.url_shortner.UrlShortnerApplication`. MongoDB must be available when the application starts because it creates indexes during startup.

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

`UrlShortnerEndToEndTest` exercises HTTP link creation and redirects, Redis cache hits, Kafka event publication and persistence, and idempotent duplicate-message delivery. It uses in-process mocks for MongoDB, Redis, and the Kafka broker, so these tests do not require external services.

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