package com.robothy.s3.rest.handler;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.robothy.netty.http.HttpRequest;
import com.robothy.netty.http.HttpResponse;
import com.robothy.s3.core.model.Bucket;
import com.robothy.s3.core.service.BucketService;
import com.robothy.s3.datatypes.response.GetBucketResult;
import com.robothy.s3.rest.assertions.RequestAssertions;
import com.robothy.s3.rest.handler.base.BaseController;
import com.robothy.s3.rest.security.AuthHandlerService;
import com.robothy.s3.rest.service.ServiceFactory;
import java.time.Instant;

/**
 * Handle <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_control_GetBucket.html">GetBucket</a>
 */
class GetBucketController extends BaseController {

  private final BucketService bucketService;

  private final XmlMapper xmlMapper;

  GetBucketController(ServiceFactory serviceFactory, final AuthHandlerService authHandlerService) {
    super(authHandlerService);
    this.bucketService = serviceFactory.getInstance(BucketService.class);
    this.xmlMapper = serviceFactory.getInstance(XmlMapper.class);
  }

  @Override
  public void handle0(HttpRequest request, HttpResponse response) throws Exception {
    String bucketName = RequestAssertions.assertBucketNameProvided(request);
    Bucket bucket = bucketService.getBucket(bucketName);
    GetBucketResult getBucketResult = GetBucketResult.builder()
        .bucket(bucket.getName())
        .creationDate(Instant.ofEpochMilli(bucket.getCreationDate()))
        .publicAccessBlockEnabled(false)
        .build();
    response.write(xmlMapper.writeValueAsString(getBucketResult));
  }

}
