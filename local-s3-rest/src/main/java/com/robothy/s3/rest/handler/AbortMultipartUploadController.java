package com.robothy.s3.rest.handler;

import com.robothy.netty.http.HttpRequest;
import com.robothy.netty.http.HttpResponse;
import com.robothy.s3.core.service.ObjectService;
import com.robothy.s3.rest.assertions.RequestAssertions;
import com.robothy.s3.rest.handler.base.BaseController;
import com.robothy.s3.rest.security.AuthHandlerService;
import com.robothy.s3.rest.service.ServiceFactory;
import com.robothy.s3.rest.utils.ResponseUtils;
import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html">AbortMultipartUpload</a>
 */
public class AbortMultipartUploadController extends BaseController {

  private final ObjectService objectService;

  public AbortMultipartUploadController(ServiceFactory serviceFactory, final AuthHandlerService authHandlerService) {
    super(authHandlerService);
    this.objectService = serviceFactory.getInstance(ObjectService.class);
  }

  @Override
  public void handle0(HttpRequest request, HttpResponse response) throws Exception {
    String bucketName = RequestAssertions.assertBucketNameProvided(request);
    String objectKey = RequestAssertions.assertObjectKeyProvided(request);
    String uploadId = RequestAssertions.assertUploadIdIsProvided(request);
    objectService.abortMultipartUpload(bucketName, objectKey, uploadId);
    response.status(HttpResponseStatus.NO_CONTENT);
    ResponseUtils.addCommonHeaders(response);
  }

}
