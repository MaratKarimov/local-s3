package com.robothy.s3.rest.handler;

import com.robothy.netty.http.HttpRequest;
import com.robothy.netty.http.HttpResponse;
import com.robothy.s3.core.service.BucketService;
import com.robothy.s3.rest.assertions.RequestAssertions;
import com.robothy.s3.rest.constants.AmzHeaderNames;
import com.robothy.s3.rest.handler.base.BaseController;
import com.robothy.s3.rest.security.AuthHandlerService;
import com.robothy.s3.rest.service.ServiceFactory;
import com.robothy.s3.rest.utils.ResponseUtils;
import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Handle <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html">HeadBucket</a>
 */
class HeadBucketController extends BaseController {

  private final BucketService bucketService;

  HeadBucketController(ServiceFactory serviceFactory, final AuthHandlerService authHandlerService) {
    super(authHandlerService);
    this.bucketService = serviceFactory.getInstance(BucketService.class);
  }

  @Override
  public void handle0(HttpRequest request, HttpResponse response) throws Exception {
    String bucketName = RequestAssertions.assertBucketNameProvided(request);
    bucketService.getBucket(bucketName);
    response.status(HttpResponseStatus.OK)
            .putHeader(AmzHeaderNames.X_AMZ_BUCKET_REGION, "local");
    ResponseUtils.addAmzRequestId(response);
    ResponseUtils.addDateHeader(response);
    ResponseUtils.addServerHeader(response);
  }

}
