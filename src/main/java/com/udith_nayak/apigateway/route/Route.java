package com.udith_nayak.apigateway.route;

import org.springframework.boot.actuate.endpoint.annotation.Selector.Match;

import java.util.List;

public record Route(
        String id,
        Match match,
        boolean stripPrefix,
        List<Target> targets,
        String balancer

) {
}
