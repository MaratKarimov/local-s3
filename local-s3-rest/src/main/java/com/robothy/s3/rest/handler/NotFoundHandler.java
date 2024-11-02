package com.robothy.s3.rest.handler;

import com.robothy.netty.http.HttpRequest;
import com.robothy.netty.http.HttpResponse;
import com.robothy.s3.core.util.IdUtils;
import com.robothy.s3.datatypes.response.S3Error;
import com.robothy.s3.rest.handler.base.BaseController;
import com.robothy.s3.rest.security.AuthHandlerService;
import com.robothy.s3.rest.utils.XmlUtils;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Construct a response body when no handlers were found.
 */
class NotFoundHandler extends BaseController {

  public NotFoundHandler(final AuthHandlerService authHandlerService) {
    super(authHandlerService);
  }

  @Override
  public void handle0(HttpRequest request, HttpResponse response) throws Exception {

    S3Error error = S3Error.builder()
        .code("NotFound")
        .message("LocalS3 cannot found resource " + request.getUri())
        .requestId(IdUtils.nextUuid())
        .build();

    response.status(HttpResponseStatus.INTERNAL_SERVER_ERROR)
        .putHeader(HttpHeaderNames.CONTENT_TYPE.toString(), HttpHeaderValues.APPLICATION_XML);

    if (!HttpMethod.HEAD.equals(request.getMethod())) {
      response.write(XmlUtils.toXml(error));
    }
  }
}
