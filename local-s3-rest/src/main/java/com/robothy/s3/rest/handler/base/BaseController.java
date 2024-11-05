package com.robothy.s3.rest.handler.base;

import com.robothy.netty.http.HttpRequest;
import com.robothy.netty.http.HttpRequestHandler;
import com.robothy.netty.http.HttpResponse;
import com.robothy.s3.rest.security.AuthHandlerService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class BaseController implements HttpRequestHandler{

    private final AuthHandlerService authHandlerService;



    public BaseController(final AuthHandlerService authHandlerService) {
        this.authHandlerService = authHandlerService;
    }

    public AuthHandlerService.S3Auth auth(final HttpRequest httpRequest){
        return authHandlerService.process(httpRequest);
    }

    @Override
    public void handle(final HttpRequest httpRequest, final HttpResponse httpResponse) throws Exception {
        final AuthHandlerService.S3Auth auth = auth(httpRequest);
        log.info("Authorization: {}", auth);
        handle0(httpRequest, httpResponse);
    }

    abstract public void handle0(final HttpRequest httpRequest, final HttpResponse httpResponse) throws Exception;
}
