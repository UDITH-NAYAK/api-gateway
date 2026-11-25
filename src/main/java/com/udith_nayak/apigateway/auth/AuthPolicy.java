package com.udith_nayak.apigateway.auth;

import java.util.List;

public record AuthPolicy(
        String mode,
        List<String>scopes
) {
}
