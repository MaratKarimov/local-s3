package com.robothy.s3.rest.handler;

import com.robothy.netty.http.HttpRequest;
import com.robothy.netty.http.HttpResponse;
import com.robothy.s3.core.model.answers.UploadPartAns;
import com.robothy.s3.core.model.request.UploadPartOptions;
import com.robothy.s3.core.service.ObjectService;
import com.robothy.s3.core.service.UploadPartService;
import com.robothy.s3.rest.assertions.RequestAssertions;
import com.robothy.s3.rest.handler.base.BaseController;
import com.robothy.s3.rest.model.request.DecodedAmzRequestBody;
import com.robothy.s3.rest.security.AuthHandlerService;
import com.robothy.s3.rest.service.ServiceFactory;
import com.robothy.s3.rest.utils.RequestUtils;
import com.robothy.s3.rest.utils.ResponseUtils;
import lombok.extern.slf4j.Slf4j;

/**
 * Handle <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html">UploadPart</a>
 */
@Slf4j
class UploadPartController extends BaseController {

  private final UploadPartService uploadPartService;

  UploadPartController(ServiceFactory serviceFactory, final AuthHandlerService authHandlerService) {
    super(authHandlerService);
    this.uploadPartService = serviceFactory.getInstance(ObjectService.class);
  }

  @Override
  public void handle0(HttpRequest request, HttpResponse response) throws Exception {
    String bucket = RequestAssertions.assertBucketNameProvided(request);
    String key = RequestAssertions.assertObjectKeyProvided(request);
    int partNumber = RequestAssertions.assertPartNumberIsValid(request);
    String uploadId = RequestAssertions.assertUploadIdIsProvided(request);
    DecodedAmzRequestBody decodedBody = RequestUtils.getBody(request);
    UploadPartAns uploadPartAns = uploadPartService.uploadPart(bucket, key, uploadId, partNumber, UploadPartOptions.builder()
        .contentLength(decodedBody.getDecodedContentLength())
        .data(decodedBody.getDecodedBody())
        .etag(RequestUtils.getETag(request).orElse(null))
        .build());

    ResponseUtils.addCommonHeaders(response);
    ResponseUtils.addETag(response, uploadPartAns.getEtag());
  }

}
