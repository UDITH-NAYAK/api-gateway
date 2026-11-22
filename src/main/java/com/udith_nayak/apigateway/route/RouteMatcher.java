package com.udith_nayak.apigateway.route;

import com.udith_nayak.apigateway.config.GatewayProperties;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

@Component
public class RouteMatcher {

    private final List<GatewayProperties.RouteDefinition> routes;
    public RouteMatcher(GatewayProperties props){
        this.routes = props.routes().stream()
                .sorted(Comparator.comparingInt((GatewayProperties.RouteDefinition r)->r.pathPrefix().length()).reversed()).toList();
    }
    public Optional<GatewayProperties.RouteDefinition>match(String path){
        return routes.stream()
                .filter(route->route.pathPrefix().equals(path)||
                        path.startsWith(route.pathPrefix())).findFirst();
    }
}
