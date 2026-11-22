package com.udith_nayak.apigateway.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@ConfigurationProperties(prefix = "gateway")
public record GatewayProperties(List<RouteDefinition> routes) {
    public record RouteDefinition(String id, String pathPrefix,String target, boolean stripPrefix){}
}
