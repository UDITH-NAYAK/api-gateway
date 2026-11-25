package com.udith_nayak.apigateway.proxy;

import com.udith_nayak.apigateway.route.RouteMatcher;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebHandler;
import reactor.core.publisher.Mono;

import java.util.Set;

public class GatewayHandler implements WebHandler {

    //drop the incoming client header
        private static final Set<String> HIP_HOP_HEADERS = Set.of("" +
                "connection","keep-alive","proxy-authentication","proxy-authorization","te","trailer","transfer-encoding","upgrade","host");
        private final RouteMatcher routeMatcher;
        private final WebClient webClient = WebClient.builder()
                .build();

        public GatewayHandler(RouteMatcher routeMatcher){
            this.routeMatcher=routeMatcher;
        }

    @Override
    public Mono<Void> handle(ServerWebExchange exchange) {
        ServerHttpRequest request = exchange.getRequest();
        String path=request.getURI().getRawPath();
        var routeOut = routeMatcher.match(path);
        if(routeOut.isEmpty()){
            exchange.getResponse().setStatusCode(HttpStatus.NOT_FOUND);
            return exchange.getResponse().setComplete();
        }


            return null;
    }
}
