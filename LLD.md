# API Gateway: Low-Level Design

**Author:** Udith | **Status:** Draft v1 | **Date:** 25 Nov 2026 | **Companion to:** API Gateway High-Level Design | **Stack:** Java 21, Spring Boot, WebFlux, Reactor Netty, Redis (Lettuce), Micrometer

## 1. Scope

This document turns the HLD into an implementable design: modules and packages, class and interface contracts, algorithms, data structures, config schema, error model, concurrency rules, and a test plan per component. Each section ends with the HLD phase it belongs to, so it can be built in order.

## 2. Project Layout

```
api-gateway/
  pom.xml                       (parent, dependency management)
  gateway/                      (the product)
    src/main/java/com/udith/gateway/
      GatewayApplication.java
      config/       GatewayProperties, RouteTable, RouteTableHolder, ConfigLoader, ConfigValidator, ConfigWatcher
      core/         GatewayFilter, FilterChain, DefaultFilterChain, GatewayContext, GatewayHandler
      route/        Route, RouteMatcher
      filter/       RequestIdFilter, AccessLogFilter, SanityFilter, RouteFilter, AuthFilter,
                    AuthzFilter, RateLimitFilter, ProxyFilter
      auth/         Authenticator, JwtAuthenticator, ApiKeyAuthenticator, Principal, JwksCache
      ratelimit/    RateLimiter, RedisTokenBucketLimiter, LocalTokenBucketLimiter, RateLimitResult
      lb/           LoadBalancer, RoundRobinBalancer, WeightedBalancer, LeastOutstandingBalancer,
                    Instance, InstancePool, HealthChecker
      resilience/   CircuitBreaker, CircuitBreakerRegistry, RetryPolicy, Bulkhead, LoadShedder
      upstream/     UpstreamClient, HeaderPolicy
      error/        ErrorCode, GatewayException, ErrorWriter
      metrics/      GatewayMetrics
      admin/        AdminHandler
    src/main/resources/{application.yml, routes.yml, lua/token_bucket.lua}
  service-users/ service-orders/   (demo backends)
  docker-compose.yml  prometheus.yml  grafana/
```

Dependency rule: `core` depends on nothing project-specific except `error`. Feature packages (`AuthPolicy`, `ratelimit`, `lb`, `resilience`) never depend on each other directly; `filter` wires them together. This keeps each mechanism independently testable.

## 3. Core Abstractions (Phase 2)

### 3.1 Entry point

A single `WebHandler` receives every request that no actuator or admin mapping claims. It builds the context and starts the chain.

```java
@Component
public class GatewayHandler implements WebHandler {
    private final List<GatewayFilter> filters; // injected, sorted by order()

    public GatewayHandler(List<GatewayFilter> filters) {
        this.filters = filters.stream()
            .sorted(Comparator.comparingInt(GatewayFilter::order)).toList();
    }

    @Override
    public Mono<Void> handle(ServerWebExchange exchange) {
        GatewayContext ctx = new GatewayContext();
        exchange.getAttributes().put(GatewayContext.KEY, ctx);
        return new DefaultFilterChain(filters, 0, ctx).next(exchange);
    }
}
```

### 3.2 Filter contract

```java
public interface GatewayFilter {
    int order();
    Mono<Void> filter(ServerWebExchange exchange, GatewayContext ctx, FilterChain chain);
}

public interface FilterChain {
    Mono<Void> next(ServerWebExchange exchange);
}

public final class DefaultFilterChain implements FilterChain {
    private final List<GatewayFilter> filters;
    private final int index;
    private final GatewayContext ctx;
    // constructor omitted

    @Override
    public Mono<Void> next(ServerWebExchange exchange) {
        if (index >= filters.size()) return Mono.empty();
        GatewayFilter f = filters.get(index);
        return f.filter(exchange, ctx, new DefaultFilterChain(filters, index + 1, ctx));
    }
}
```

Pre-processing runs before `chain.next(...)`; post-processing is chained after it with `.then(...)` or `doFinally(...)`. A filter rejects a request by returning `Mono.error(new GatewayException(...))` without calling `next`.

### 3.3 Per-request context

```java
public final class GatewayContext {
    public static final String KEY = "gateway.ctx";
    String requestId;
    Route route;              // set by RouteFilter
    Principal principal;      // set by AuthFilter (null for public routes)
    Instance target;          // set by ProxyFilter
    long startNanos = System.nanoTime();
    int attempts;             // upstream attempts made
    String rejectionReason;   // set on rejection, used by logs and metrics
    // plain mutable fields are safe: one request is processed by one logical flow
}
```

### 3.4 Filter order constants

| Order | Filter | Notes |
| --- | --- | --- |
| 10 | RequestIdFilter | Accept or generate `X-Request-Id`, put in MDC-like context |
| 20 | AccessLogFilter | Wraps the rest; logs in `doFinally`, success or failure |
| 30 | SanityFilter | Header count and size, body size limit, coarse per-IP limit |
| 40 | RouteFilter | Match route, set `ctx.route`, 404 on miss |
| 50 | AuthFilter | Skipped when `route.auth == NONE` |
| 60 | AuthzFilter | Scope and role check |
| 70 | RateLimitFilter | Per-route policy |
| 100 | ProxyFilter | Terminal: balancer, breaker, bulkhead, upstream call, retries |

## 4. Configuration (Phases 1 and 8)

### 4.1 Schema (`routes.yml`)

```yaml
routes:
  - id: users
    match:
      path-prefix: /api/users
      methods: [GET, POST, PUT, DELETE]   # optional; default all
      host: null                          # optional
    strip-prefix: true
    targets:
      - { url: http://localhost:8081, weight: 1 }
      - { url: http://localhost:8082, weight: 1 }
    balancer: round-robin                 # round-robin | weighted | least-outstanding
    auth: { mode: JWT, scopes: [users.read] }   # NONE | JWT | API_KEY | JWT_OR_API_KEY
    rate-limit: { capacity: 100, refill-per-sec: 50, key: USER, on-redis-failure: FAIL_OPEN }
    timeout: { connect-ms: 1000, response-ms: 3000 }
    retry: { max-attempts: 2, backoff-ms: 100, budget-percent: 10 }
    circuit-breaker: { failure-rate-percent: 50, window-size: 20, min-calls: 10, open-ms: 10000, half-open-calls: 3 }
    bulkhead: { max-concurrent: 200 }
    health-check: { path: /actuator/health, interval-ms: 5000, fail-threshold: 3, success-threshold: 2 }
```

### 4.2 Types

```java
public record Route(String id, Match match, boolean stripPrefix, List<Target> targets,
                    String balancer, AuthPolicy auth, RateLimitPolicy rateLimit,
                    TimeoutPolicy timeout, RetryPolicy retry, BreakerPolicy breaker,
                    BulkheadPolicy bulkhead, HealthPolicy health) {}

public final class RouteTable {              // immutable
    private final List<Route> prefixRoutes;  // sorted by prefix length, descending
    private final Map<String, InstancePool> pools; // routeId -> pool
    public Optional<Route> match(String host, HttpMethod method, String path) { ... }
}

@Component
public class RouteTableHolder {
    private final AtomicReference<RouteTable> ref = new AtomicReference<>();
    public RouteTable get() { return ref.get(); }
    public void swap(RouteTable next) { ref.set(next); }
}
```

### 4.3 Matching algorithm

1. Filter routes whose method set and host (if given) match.
2. A path matches a prefix when `path.equals(prefix) || path.startsWith(prefix + "/")` (so `/api/users2` does not match `/api/users`).
3. Longest prefix wins. Complexity is O(R); switch to a path trie when routes exceed a few hundred.
4. Downstream path: if `strip-prefix`, remove the prefix; an empty result becomes `/`. Raw (still-encoded) path and query are forwarded unchanged to avoid double decoding.

### 4.4 Validation rules (`ConfigValidator`)

- Route IDs unique; at least one target per route; target URLs absolute http or https.
- Weights are positive; limits positive; `refill-per-sec` at most `capacity * 10`.
- No two routes with identical (host, method set, prefix).
- `retry.max-attempts` at most 3; timeouts positive; `response-ms` at least `connect-ms`.
- Auth `JWT` requires a configured issuer and audience.

Validation returns a list of errors, never throws mid-way. Any error rejects the whole file.

### 4.5 Hot reload (`ConfigWatcher`)

```text
watch routes.yml (java.nio WatchService, debounce 500 ms) or POST /admin/reload
  -> parse -> validate -> on errors: log, metric gateway_config_reload_failed, keep old table
  -> build new RouteTable, REUSE InstancePool, breaker, and bulkhead objects for routes whose
     config is unchanged (so health and breaker state survive reloads)
  -> holder.swap(newTable)
  -> close pools/breakers of removed routes after a grace period
```

In-flight requests keep the `RouteTable` reference they started with, so a swap never changes a request midway.

## 5. Cross-Cutting Components (Phase 2)

### 5.1 Request ID and Access Log

- `RequestIdFilter`: read `X-Request-Id`; accept only if it matches `^[A-Za-z0-9-]{8,64}$`, else generate a UUID. Store in context, set on the upstream request and on the response.
- `AccessLogFilter` logs in `doFinally` so rejected and failed requests are recorded. It logs one JSON line: `ts, requestId, method, path, route, status, latencyMs, upstream, attempts, clientId, rejection`. It never logs the query string, `Authorization`, `Cookie`, or `X-Api-Key`.
- Reactor does not propagate thread-locals. Put the request ID in the Reactor `Context` (`contextWrite`) if logging from inside operators; otherwise read it from `GatewayContext`.

### 5.2 Error model

```java
public enum ErrorCode {
    ROUTE_NOT_FOUND(404), UNAUTHENTICATED(401), FORBIDDEN(403), PAYLOAD_TOO_LARGE(413),
    RATE_LIMITED(429), UPSTREAM_ERROR(502), NO_HEALTHY_UPSTREAM(503), CIRCUIT_OPEN(503),
    OVERLOADED(503), UPSTREAM_TIMEOUT(504), INTERNAL(500);
    final int status;
}

public class GatewayException extends RuntimeException {
    final ErrorCode code; final Map<String,String> headers; // e.g. Retry-After
}
```

`ErrorWriter` converts any `GatewayException` (and any unexpected `Throwable`, mapped to `INTERNAL`) into a uniform JSON body and never leaks stack traces or upstream details:

```json
{ "error": { "code": "RATE_LIMITED", "message": "Too many requests", "requestId": "b3f1c9..." } }
```

A `WebExceptionHandler` registered with highest precedence guarantees this is the only error path.

### 5.3 Concurrency rules (enforced in code review)

1. **Never block** on a Netty event-loop thread: no `.block()`, no `Thread.sleep`, no blocking JDBC or file I/O.
2. CPU-heavy or blocking work (BCrypt, file reads) goes to `Schedulers.boundedElastic()`. API keys are high-entropy random strings, so they are hashed with HMAC-SHA-256 plus a server-side pepper, which is fast enough to run inline.
3. Shared mutable state uses atomics, `ConcurrentHashMap`, or immutable snapshots. Locks are held only for tiny critical sections.
4. Enable Reactor's blocking-call detection (BlockHound) in tests.

## 6. Authentication and Authorisation (Phase 3)

### 6.1 Contracts

```java
public record Principal(String subject, String clientId, Set<String> scopes, Set<String> roles) {}

public interface Authenticator {
    boolean supports(ServerHttpRequest request);               // header present?
    Mono<Principal> authenticate(ServerHttpRequest request);   // error -> UNAUTHENTICATED
}
```

### 6.2 `JwtAuthenticator`

1. Extract `Authorization: Bearer <token>`; reject missing or malformed values.
2. Parse the header only to read `kid` and `alg`. **Reject any `alg` not in the configured allow-list** (RS256, ES256) and never accept `none`.
3. Look up the key in `JwksCache` by `kid`. On a miss, refresh at most once per 60 s (prevents refresh storms from forged `kid` values).
4. Verify the signature, then `exp`, `nbf` (with 30 s clock skew), `iss`, `aud`.
5. Build `Principal` from `sub`, `scope` (space-separated), and `roles` claims.

`JwksCache`: `ConcurrentHashMap<String, PublicKey>`, scheduled refresh every 10 min, keeps the last good set if a refresh fails (stale-while-error), and exposes `gateway_jwks_age_seconds`.

For the demo, a small `/auth/login` handler in a separate module issues RS256 tokens and publishes a JWKS endpoint, so the gateway validates tokens the same way it would for an external identity provider.

### 6.3 `ApiKeyAuthenticator`

- Header `X-Api-Key`. Compute `HMAC-SHA256(pepper, key)` and look it up in a `Caffeine` cache backed by the key store (a YAML file in v1). Cache TTL 60 s; negative results cached for 10 s.
- Compare hashes with `MessageDigest.isEqual` (constant time).
- Key records hold `clientId`, `scopes`, `expiresAt`, `disabled`.

### 6.4 `AuthFilter` and `AuthzFilter`

```text
AuthFilter:
  if route.auth.mode == NONE -> next
  pick authenticators by mode (JWT_OR_API_KEY tries the one whose header is present)
  success -> ctx.principal = principal -> next
  failure -> 401 UNAUTHENTICATED (WWW-Authenticate: Bearer)

AuthzFilter:
  required = route.auth.scopes
  if !principal.scopes.containsAll(required) -> 403 FORBIDDEN
```

Before proxying, `HeaderPolicy` removes every inbound `X-User-*` and `X-Client-Id` header and sets trusted ones from `ctx.principal`.

## 7. Rate Limiting (Phase 4)

### 7.1 Contract

```java
public interface RateLimiter {
    Mono<RateLimitResult> tryAcquire(String key, RateLimitPolicy policy, int cost);
}
public record RateLimitResult(boolean allowed, long remaining, long retryAfterMs, boolean degraded) {}
```

### 7.2 Redis implementation

Key: `rl:{routeId}:{subject}` where `subject` is `principal.clientId`, `principal.subject`, or the client IP, per `policy.key` (`USER`, `CLIENT`, `IP`). Client IP comes from the socket address, or from `X-Forwarded-For` only when the request arrived from a configured trusted-proxy CIDR (to prevent spoofing the limiter key).

`lua/token_bucket.lua`:

```lua
-- KEYS[1] bucket key
-- ARGV[1] capacity, ARGV[2] refill tokens per second, ARGV[3] cost
local t = redis.call('TIME')
local now_ms = t[1] * 1000 + math.floor(t[2] / 1000)
local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])

local data = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])
if tokens == nil then tokens = capacity; ts = now_ms end

local elapsed = math.max(0, now_ms - ts)
tokens = math.min(capacity, tokens + (elapsed / 1000.0) * rate)

local allowed, retry_ms = 0, 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
else
  retry_ms = math.ceil(((cost - tokens) / rate) * 1000)
end

redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now_ms)
redis.call('PEXPIRE', KEYS[1], math.ceil(capacity / rate * 1000) * 2)
return { allowed, math.floor(tokens), retry_ms }
```

Java side:

```java
@Component
public class RedisTokenBucketLimiter implements RateLimiter {
    private final ReactiveStringRedisTemplate redis;
    private final RedisScript<List> script;          // DefaultRedisScript, resultType List.class
    private final LocalTokenBucketLimiter fallback;

    public Mono<RateLimitResult> tryAcquire(String key, RateLimitPolicy p, int cost) {
        return redis.execute(script, List.of(key),
                        List.of(String.valueOf(p.capacity()), String.valueOf(p.refillPerSec()), String.valueOf(cost)))
                .next()
                .timeout(Duration.ofMillis(30))
                .map(r -> new RateLimitResult(((Long) r.get(0)) == 1L, (Long) r.get(1), (Long) r.get(2), false))
                .onErrorResume(e -> {
                    metrics.redisError();
                    if (p.onRedisFailure() == FAIL_CLOSED) return Mono.error(new GatewayException(RATE_LIMITED, ...));
                    return fallback.tryAcquire(key, p, cost).map(RateLimitResult::asDegraded);
                });
    }
}
```

The script uses the Redis server clock so instances with skewed clocks agree. Script result tokens are returned as integers; the stored value stays fractional for accurate refill.

### 7.3 Local fallback

`LocalTokenBucketLimiter` keeps a `ConcurrentHashMap<String, Bucket>` (Caffeine with `expireAfterAccess` to bound memory). `Bucket` refill uses `System.nanoTime()` and a `synchronized` block per bucket. When degraded, each instance limits locally at `capacity / expected_instance_count` to approximate the global limit.

### 7.4 `RateLimitFilter`

```text
policy = route.rateLimit; if null -> next
key = build(route, principal, clientIp, policy.key)
limiter.tryAcquire(key, policy, 1)
  allowed  -> add X-RateLimit-Limit / Remaining on response -> next
  rejected -> 429 RATE_LIMITED with Retry-After = ceil(retryAfterMs / 1000)
```

## 8. Load Balancing and Health (Phase 5)

### 8.1 Instance and pool

```java
public final class Instance {
    final URI uri; final int weight;
    final AtomicInteger inFlight = new AtomicInteger();
    volatile HealthState state = HealthState.UP;      // UP | DOWN
    final AtomicInteger consecutiveFailures = new AtomicInteger();
    final AtomicInteger consecutiveSuccesses = new AtomicInteger();
    volatile long upSinceMs;                          // for slow start
    final CircuitBreaker breaker;                     // one per instance
}

public final class InstancePool {
    private final List<Instance> all;                 // immutable
    List<Instance> healthy() { /* filter state==UP and breaker not OPEN */ }
}
```

### 8.2 Balancers

```java
public interface LoadBalancer {
    Optional<Instance> choose(List<Instance> candidates, Set<Instance> excluded);
}
```

| Strategy | Algorithm | Complexity |
| --- | --- | --- |
| Round-robin | `AtomicInteger counter`; index = `Math.floorMod(counter.getAndIncrement(), n)` | O(1) |
| Weighted | Smooth weighted round-robin (nginx style): each pick adds `weight` to every `current`, chooses the max, subtracts the total. Guarded by a small lock per pool | O(n) |
| Least-outstanding | Pick the instance with the lowest `inFlight` (ties broken at random); "power of two choices" when n is large | O(1) to O(n) |

The `excluded` set holds instances already tried in this request, so a retry never hits the instance that just failed when another exists. Slow start scales an instance's effective weight linearly from 10% to 100% over 30 s after it becomes `UP`.

### 8.3 `HealthChecker`

- One scheduled task per route (`Flux.interval`), probing every instance with a `WebClient` GET to `health.path` with a 1 s timeout, on its own scheduler so slow probes never delay each other.
- State machine: `UP -> DOWN` after `fail-threshold` consecutive failures; `DOWN -> UP` after `success-threshold` consecutive successes. Success means 2xx.
- Passive checks: `ProxyFilter` reports every real outcome. Three consecutive connect errors or 5xx responses mark the instance `DOWN` early, so ejection does not wait for the next probe.
- When `healthy()` is empty, `ProxyFilter` returns `NO_HEALTHY_UPSTREAM` (503).

## 9. Resilience (Phase 6)

### 9.1 Circuit breaker state machine

```text
         failure rate >= threshold
  CLOSED ---------------------------> OPEN
    ^                                   |
    | all probe calls succeed           | after open-ms
    |                                   v
    +------------------------------ HALF_OPEN
          any probe call fails ----> back to OPEN
```

```java
public final class CircuitBreaker {
    enum State { CLOSED, OPEN, HALF_OPEN }
    private final BreakerPolicy policy;
    private final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);
    private final boolean[] window;          // ring buffer: true = failure
    private int idx, count, failures;        // guarded by lock
    private volatile long openedAtMs;
    private final AtomicInteger halfOpenPermits = new AtomicInteger();
    private final Object lock = new Object();

    public boolean tryAcquire() { ... }      // CLOSED: true; OPEN: true only after open-ms (moves to HALF_OPEN); HALF_OPEN: take a permit
    public void onSuccess() { ... }          // record in window; in HALF_OPEN, close after half-open-calls successes
    public void onFailure() { ... }          // record; open if count >= min-calls and failureRate >= threshold; in HALF_OPEN, reopen
}
```

What counts as a failure: connect error, timeout, and 502, 503, 504 responses. 4xx and 500 responses from the backend are **not** counted unless configured, because they usually reflect the request, not backend health.

### 9.2 Retry policy

```java
Mono<Response> callWithRetry(...) {
    return Mono.defer(() -> callOnce(ctx))   // callOnce re-chooses an instance each attempt
        .retryWhen(Retry.backoff(policy.maxAttempts(), Duration.ofMillis(policy.backoffMs()))
            .jitter(0.5)
            .filter(e -> isRetryable(e) && idempotent(method) && budget.tryConsume())
            .onRetryExhaustedThrow((spec, signal) -> signal.failure()));
}
```

- **Idempotent methods only** (GET, HEAD, PUT, DELETE, OPTIONS). POST retries only when the client sent an `Idempotency-Key` header.
- **Request body replay:** a streamed body cannot be sent twice. Retries are therefore enabled only for requests with no body, or with a body under a small limit (64 KB) that the gateway buffers once. Larger bodies are never retried.
- Retryable: connect failure, connection reset before any response bytes, 502, 503, 504. Never retry after the response has started streaming to the client.
- **Retry budget:** per-route sliding counter; retries are allowed while `retries <= budget-percent` of the last 10 s of requests (with a floor of 10 retries per 10 s so low traffic can still retry).

### 9.3 Bulkhead and load shedding

- `Bulkhead`: a per-route `Semaphore` (`max-concurrent`). `tryAcquire()` failure returns `OVERLOADED` immediately, with no queueing. The permit is released in `doFinally`.
- `LoadShedder`: a gateway-wide guard. It samples event-loop lag every 100 ms (a scheduled task measures actual vs expected wake time). If lag exceeds 100 ms for 3 consecutive samples, the shedder rejects the cheapest-to-reject traffic first (unauthenticated and low-priority routes) with `OVERLOADED` until lag recovers.

### 9.4 Upstream client

```java
ConnectionProvider provider = ConnectionProvider.builder("upstream")
    .maxConnections(500).pendingAcquireMaxCount(1000)
    .pendingAcquireTimeout(Duration.ofMillis(200))
    .maxIdleTime(Duration.ofSeconds(30)).maxLifeTime(Duration.ofMinutes(5))
    .evictInBackground(Duration.ofSeconds(60)).metrics(true).build();

HttpClient http = HttpClient.create(provider)
    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, route.timeout().connectMs())
    .responseTimeout(Duration.ofMillis(route.timeout().responseMs()))
    .compress(false).followRedirect(false);
```

`followRedirect(false)` keeps redirects visible to the client. Exceptions map as: `ConnectTimeoutException` / `ReadTimeoutException` / `ResponseTimeoutException` -> `UPSTREAM_TIMEOUT`; `ConnectException` / `PrematureCloseException` -> `UPSTREAM_ERROR`.

## 10. Proxy Filter (Phases 1 and 6)

Terminal filter. Behaviour in order:

1. `pool = table.pool(route.id)`; `candidates = pool.healthy()`; empty -> `NO_HEALTHY_UPSTREAM`.
2. `bulkhead.tryAcquire()` else `OVERLOADED`.
3. `balancer.choose(candidates, excluded)`; `breaker.tryAcquire()` else exclude and re-choose; none left -> `CIRCUIT_OPEN`.
4. Build target URI (section 4.3); build headers via `HeaderPolicy`.
5. `instance.inFlight++`; call upstream; stream the response (status, filtered headers, body as `DataBuffer` flux) to the client.
6. On completion: `breaker.onSuccess/onFailure`, passive health report, `inFlight--`, bulkhead release, metrics.
7. On retryable failure: add instance to `excluded`, increment `ctx.attempts`, loop through `RetryPolicy`.

`HeaderPolicy` rules:

| Direction | Rule |
| --- | --- |
| Request | Drop hop-by-hop headers (`Connection`, `Keep-Alive`, `Proxy-*`, `TE`, `Trailer`, `Transfer-Encoding`, `Upgrade`) and any header named in `Connection` |
| Request | Replace `Host` with the target host; append client IP to `X-Forwarded-For`; set `X-Forwarded-Proto` and `X-Forwarded-Host` |
| Request | Strip inbound `X-User-*`, `X-Client-Id`; set `X-Request-Id`, `traceparent` (propagate or create) |
| Response | Drop hop-by-hop headers and `Server`; add `X-Request-Id` |

## 11. Observability (Phase 7)

| Metric | Type | Tags |
| --- | --- | --- |
| `gateway_requests_total` | counter | route, method, status\_class, outcome |
| `gateway_request_duration_seconds` | histogram (p50/p95/p99, SLO buckets) | route |
| `gateway_upstream_duration_seconds` | histogram | route, instance |
| `gateway_rate_limit_total` | counter | route, result (allowed/rejected/degraded) |
| `gateway_redis_latency_seconds` / `gateway_redis_errors_total` | histogram / counter | operation |
| `gateway_circuit_state` | gauge (0 closed, 1 half-open, 2 open) | route, instance |
| `gateway_retries_total` | counter | route |
| `gateway_instance_up` | gauge | route, instance |
| `gateway_inflight` | gauge | route |
| `gateway_bulkhead_rejected_total` | counter | route |
| `gateway_event_loop_lag_seconds` | gauge | none |
| `gateway_config_reload_total` / `_failed_total` | counter | none |

Cardinality rule: never tag with user ID, request ID, or raw path. Route ID is the only path-derived tag.

Dashboards: request rate and error ratio per route; latency percentiles; rate-limit rejections; instance health grid; breaker states; Redis latency; JVM heap, GC, and event-loop lag.

## 12. Admin API (Phase 8)

Served on a separate port (`management.server.port` style), internal network only, authenticated with an admin API key.

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/admin/routes` | Active route table (sanitised) |
| GET | `/admin/health` | Instance health per route |
| GET | `/admin/circuits` | Breaker state and recent failure rate |
| POST | `/admin/reload` | Trigger config reload; returns validation errors if any |
| GET | `/health/liveness` | Process is alive |
| GET | `/health/readiness` | Route table loaded; Redis reachable (reported, not required) |

## 13. Security Details

- Limits set on the Netty server: max initial line 4 KB, max header size 8 KB, max header count 100, idle timeout 60 s, request body limit per route (default 1 MB).
- `X-Forwarded-For` is trusted only from configured proxy CIDRs.
- Route targets come only from validated config; no request-controlled data ever forms a target host (blocks SSRF).
- Ambiguous requests (both `Content-Length` and `Transfer-Encoding`, duplicate `Content-Length`) are rejected with 400 before reaching a backend.
- Secrets (JWT issuer config, HMAC pepper, Redis password, admin key) come from environment variables; startup fails fast if a required secret is missing.
- Logs and error bodies never include tokens, keys, or upstream stack traces.

## 14. Sequence: Authenticated, Rate-Limited Proxy Call

```text
Client      Gateway filters             Redis       Backend
  | GET /api/users/42 + Bearer |           |           |
  |--------------------------->| RequestId, Log start  |
  |                            | Route match: users    |
  |                            | JWT verify (cached JWKS)
  |                            | Scope check           |
  |                            |--EVALSHA bucket------>|
  |                            |<--allowed, remaining--|
  |                            | choose instance, breaker ok, bulkhead ok
  |                            |--GET /42 (X-User-Id, X-Request-Id)-->|
  |                            |<--------------200 stream-------------|
  |<------ 200 + X-RateLimit-* + X-Request-Id          |
  |                            | metrics + access log (doFinally)
```

## 15. Test Plan per Component

| Component | Tests |
| --- | --- |
| RouteMatcher | longest prefix, `/api/users2` not matching `/api/users`, method and host filters, strip-prefix edge cases (`/api/users` -> `/`) |
| ConfigValidator | each rule, with one failing case per rule; reload keeps old table on error |
| FilterChain | order respected, short-circuit on error, post-filters run on failure |
| JwtAuthenticator | valid, expired, not-yet-valid, wrong issuer, wrong audience, `alg=none`, HS256 with public key as secret, unknown `kid`, JWKS outage with cached keys |
| ApiKeyAuthenticator | valid, disabled, expired, unknown, timing-safe compare |
| Token bucket Lua | burst up to capacity, refill over time, exact `retry_ms`, key expiry, concurrent calls from 2 gateway instances never exceed capacity |
| RedisTokenBucketLimiter | Redis stopped: degraded and fail-open vs fail-closed per policy |
| Balancers | even distribution, weights honoured (e.g. 5:1:1 over 700 picks), excluded instances skipped, slow start ramp |
| HealthChecker | thresholds for ejection and recovery, no flapping on a single failure |
| CircuitBreaker | closed to open at threshold, min-calls respected, half-open probe success and failure paths, concurrent updates |
| RetryPolicy | idempotent only, body-size rule, budget exhaustion, no retry after streaming starts |
| ProxyFilter | WireMock: 200, 404 passthrough, 502 on connection refused, 504 on slow backend, header filtering, large body streaming with flat memory |
| End to end | Testcontainers Redis + two gateway instances + WireMock backends: kill a backend, stop Redis, push bad config, spike load; assert availability and error codes |

Tooling: JUnit 5, AssertJ, Reactor `StepVerifier`, `WebTestClient`, WireMock, Testcontainers, BlockHound, k6 for load.

## 16. Build Order and Definition of Done

| Step | Deliverable | Done when |
| --- | --- | --- |
| 1 | `config`, `route`, `ProxyFilter` basic, `UpstreamClient` | Proxy works; 404, 502, 504 correct |
| 2 | `core`, `RequestIdFilter`, `AccessLogFilter`, `error` | Uniform error JSON; ordered chain tests pass |
| 3 | `AuthPolicy` package, `AuthFilter`, `AuthzFilter`, `HeaderPolicy` | All JWT and API key negative tests pass |
| 4 | `ratelimit` package, Lua script, `RateLimitFilter` | Two-instance shared limit verified |
| 5 | `lb` package, `HealthChecker` | Killed instance ejected within threshold, restored after recovery |
| 6 | `resilience` package, retry and breaker wiring | Chaos tests pass |
| 7 | `metrics`, Prometheus, Grafana | Dashboard shows live data |
| 8 | Hot reload, admin API, hardening, Docker image, load test, README | NFR numbers measured and published |

Every step ends with passing tests, a commit, and a short note of decisions in the README before the next step starts.

## 17. Known Limitations (v1)

- Route matching is linear in the number of routes.
- Circuit and health state are per gateway instance and are not shared.
- Retries are disabled for large request bodies.
- API keys live in a file; runtime management needs the v2 admin store.
- No response caching, WebSocket, or gRPC support.
