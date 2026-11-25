package com.udith_nayak.apigateway.ratelimit;

public record RateLimitPolicy(
        int capacity,
        int refillPerSecond,
        String key,
        String onRedisFailure
) {
}
