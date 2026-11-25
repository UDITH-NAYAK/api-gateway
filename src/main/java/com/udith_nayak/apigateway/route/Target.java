package com.udith_nayak.apigateway.route;

import java.net.URI;

public record Target(
    URI uri,
    int weight
) {
}
