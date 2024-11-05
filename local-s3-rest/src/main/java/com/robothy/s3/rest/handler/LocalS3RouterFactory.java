package com.robothy.s3.rest.handler;

import static com.robothy.s3.rest.handler.LocalS3Router.BUCKET_KEY_PATH;
import static com.robothy.s3.rest.handler.LocalS3Router.BUCKET_PATH;
import com.robothy.netty.router.Route;
import com.robothy.netty.router.Router;
import com.robothy.s3.core.exception.LocalS3Exception;
import com.robothy.s3.core.exception.LocalS3InvalidArgumentException;
import com.robothy.s3.rest.constants.AmzHeaderNames;
import com.robothy.s3.rest.security.AWSSignatureV2Service;
import com.robothy.s3.rest.security.AWSSignatureV4Service;
import com.robothy.s3.rest.security.AuthHandlerService;
import com.robothy.s3.rest.security.S3AuthService;
import com.robothy.s3.rest.service.ServiceFactory;
import io.netty.handler.codec.http.HttpMethod;
import java.util.Objects;

public class LocalS3RouterFactory {

  /**
   * Create a new LocalS3Router instance.
   */
  public static Router create(ServiceFactory serviceFactory) {

    final S3AuthService s3AuthService = new S3AuthService();
    final AWSSignatureV2Service awsSignatureV2Service = new AWSSignatureV2Service(s3AuthService);
    final AWSSignatureV4Service awsSignatureV4Service = new AWSSignatureV4Service(s3AuthService);

    final AuthHandlerService authHandlerService = new AuthHandlerService(s3AuthService, awsSignatureV2Service, awsSignatureV4Service);

    Objects.requireNonNull(serviceFactory);
    BucketPolicyController bucketPolicy = new BucketPolicyController(serviceFactory, authHandlerService);
    BucketReplicationController bucketReplicationController = new BucketReplicationController(serviceFactory, authHandlerService);
    BucketEncryptionController bucketEncryptionController = new BucketEncryptionController(serviceFactory, authHandlerService);
    ObjectTaggingController objectTaggingController = new ObjectTaggingController(serviceFactory, authHandlerService);

    Route AbortMultipartUpload = Route.builder()
        .method(HttpMethod.DELETE)
        .path(BUCKET_KEY_PATH)
        .paramMatcher(params -> params.containsKey("uploadId"))
        .handler(new AbortMultipartUploadController(serviceFactory, authHandlerService))
        .build();

    Route CompleteMultipartUpload = Route.builder()
        .method(HttpMethod.POST)
        .path(BUCKET_KEY_PATH)
        .handler(new CompleteMultipartUploadController(serviceFactory, authHandlerService))
        .build();

    Route CopyObject = Route.builder()
        .method(HttpMethod.PUT)
        .path(BUCKET_KEY_PATH)
        .headerMatcher(headers -> headers.containsKey(AmzHeaderNames.X_AMZ_COPY_SOURCE))
        .handler(new CopyObjectController(serviceFactory, authHandlerService))
        .build();

    Route CreateBucket = Route.builder()
        .method(HttpMethod.PUT)
        .path(BUCKET_PATH)
        .handler(new CreateBucketController(serviceFactory, authHandlerService))
        .build();

    Route CreateMultipartUpload = Route.builder()
        .method(HttpMethod.POST)
        .path(BUCKET_KEY_PATH)
        .paramMatcher(params -> params.containsKey("uploads"))
        .handler(new CreateMultipartUploadController(serviceFactory, authHandlerService))
        .build();

    Route DeleteBucket = Route.builder()
        .method(HttpMethod.DELETE)
        .path(BUCKET_PATH)
        .handler(new DeleteBucketController(serviceFactory, authHandlerService))
        .build();





    Route DeleteBucketAnalyticsConfiguration = Route.builder()
        .method(HttpMethod.DELETE)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("analytics"))
        .handler(new NotImplementedOperationController(serviceFactory, "DeleteBucketAnalyticsConfiguration", authHandlerService))
        .build();

    Route DeleteBucketCors = Route.builder()
        .method(HttpMethod.DELETE)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("cors"))
        .handler(new NotImplementedOperationController(serviceFactory, "DeleteBucketCors", authHandlerService))
        .build();


    Route DeleteBucketEncryption = Route.builder()
        .method(HttpMethod.DELETE)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("encryption"))
        .handler(bucketEncryptionController::delete)
        .build();

    Route DeleteBucketIntelligentTieringConfiguration = Route.builder()
        .method(HttpMethod.DELETE)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("intelligent-tiering"))
        .handler(new NotImplementedOperationController(serviceFactory, "DeleteBucketIntelligentTieringConfiguration", authHandlerService))
        .build();

    Route DeleteBucketInventoryConfiguration = Route.builder()
        .method(HttpMethod.DELETE)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("inventory"))
        .handler(new NotImplementedOperationController(serviceFactory, "DeleteBucketInventoryConfiguration", authHandlerService))
        .build();

    Route DeleteBucketLifecycle = Route.builder()
        .method(HttpMethod.DELETE)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("lifecycle"))
        .handler(new NotImplementedOperationController(serviceFactory, "DeleteBucketLifecycle", authHandlerService))
        .build();

    Route DeleteBucketMetricsConfiguration = Route.builder()
        .method(HttpMethod.DELETE)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("metrics"))
        .handler(new NotImplementedOperationController(serviceFactory, "DeleteBucketMetricsConfiguration", authHandlerService))
        .build();


    Route DeleteBucketOwnershipControls = Route.builder()
        .method(HttpMethod.DELETE)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("ownershipControls"))
        .handler(new NotImplementedOperationController(serviceFactory, "DeleteBucketOwnershipControls", authHandlerService))
        .build();

    Route DeleteBucketPolicy = Route.builder()
        .method(HttpMethod.DELETE)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("policy"))
        .handler(bucketPolicy::delete)
        .build();

    Route DeleteBucketReplication = Route.builder()
        .method(HttpMethod.DELETE)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("replication"))
        .handler(bucketReplicationController::delete)
        .build();

    Route DeleteBucketTagging = Route.builder()
        .method(HttpMethod.DELETE)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("tagging"))
        .handler(new DeleteBucketTaggingController(serviceFactory, authHandlerService))
        .build();

    Route DeleteBucketWebsite = Route.builder()
        .method(HttpMethod.DELETE)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("website"))
        .handler(new NotImplementedOperationController(serviceFactory, "DeleteBucketWebsite", authHandlerService))
        .build();

    Route DeleteObject = Route.builder()
        .method(HttpMethod.DELETE)
        .path(BUCKET_KEY_PATH)
        .handler(new DeleteObjectController(serviceFactory, authHandlerService))
        .build();

    Route DeleteObjects = Route.builder()
        .method(HttpMethod.POST)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("delete"))
        .handler(new DeleteObjectsController(serviceFactory, authHandlerService))
        .build();

    Route DeleteObjectTagging = Route.builder()
        .method(HttpMethod.DELETE)
        .path(BUCKET_KEY_PATH)
        .paramMatcher(params -> params.containsKey("tagging"))
        .handler(objectTaggingController::delete)
        .build();

    Route DeletePublicAccessBlock = Route.builder()
        .method(HttpMethod.DELETE)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("publicAccessBlock"))
        .handler(new NotImplementedOperationController(serviceFactory, "DeletePublicAccessBlock", authHandlerService))
        .build();

    Route GetBucketAccelerateConfiguration = Route.builder()
        .method(HttpMethod.GET)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("accelerate"))
        .handler(new NotImplementedOperationController(serviceFactory, "GetBucketAccelerateConfiguration", authHandlerService))
        .build();

    Route GetBucketAcl = Route.builder()
        .method(HttpMethod.GET)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("acl"))
        .handler(new GetBucketAclController(serviceFactory, authHandlerService))
        .build();

    Route GetBucketAnalyticsConfiguration = Route.builder()
        .method(HttpMethod.GET)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("analytics"))
        .handler(new NotImplementedOperationController(serviceFactory, "GetBucketAnalyticsConfiguration", authHandlerService))
        .build();

    Route GetBucketCors = Route.builder()
        .method(HttpMethod.GET)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("cors"))
        .handler(new NotImplementedOperationController(serviceFactory, "GetBucketCors", authHandlerService))
        .build();

    Route GetBucketEncryption = Route.builder()
        .method(HttpMethod.GET)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("encryption"))
        .handler(bucketEncryptionController::get)
        .build();

    Route GetBucketIntelligentTieringConfiguration = Route.builder()
        .method(HttpMethod.GET)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("intelligent-tiering"))
        .handler(new NotImplementedOperationController(serviceFactory, "GetBucketIntelligentTieringConfiguration", authHandlerService))
        .build();

    Route GetBucketInventoryConfiguration = Route.builder()
        .method(HttpMethod.GET)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("inventory"))
        .handler(new NotImplementedOperationController(serviceFactory, "GetBucketInventoryConfiguration", authHandlerService))
        .build();

    Route GetBucketLifecycle = Route.builder()
        .method(HttpMethod.GET)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("lifecycle"))
        .handler(new NotImplementedOperationController(serviceFactory, "GetBucketLifecycle", authHandlerService))
        .build();

    Route GetBucketLifecycleConfiguration = Route.builder()
        .method(HttpMethod.GET)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("lifecycle"))
        .handler(new NotImplementedOperationController(serviceFactory, "GetBucketLifecycleConfiguration", authHandlerService))
        .build();

    Route GetBucketLocation = Route.builder()
        .method(HttpMethod.GET)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("location"))
        .handler(new GetBucketLocationController(serviceFactory, authHandlerService))
        .build();

    Route GetBucketLogging = Route.builder()
        .method(HttpMethod.GET)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("logging"))
        .handler(new NotImplementedOperationController(serviceFactory, "GetBucketLogging", authHandlerService))
        .build();

    Route GetBucketMetricsConfiguration = Route.builder()
        .method(HttpMethod.GET)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("metrics"))
        .handler(new NotImplementedOperationController(serviceFactory, "GetBucketMetricsConfiguration", authHandlerService))
        .build();

    Route GetBucketNotification = Route.builder()
        .method(HttpMethod.GET)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("notification"))
        .handler(new NotImplementedOperationController(serviceFactory, "GetBucketNotification", authHandlerService))
        .build();

    Route GetBucketNotificationConfiguration = Route.builder()
        .method(HttpMethod.GET)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("notification"))
        .handler(new NotImplementedOperationController(serviceFactory, "GetBucketNotificationConfiguration", authHandlerService))
        .build();

    Route GetBucketOwnershipControls = Route.builder()
        .method(HttpMethod.GET)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("ownershipControls"))
        .handler(new NotImplementedOperationController(serviceFactory, "GetBucketOwnershipControls", authHandlerService))
        .build();

    Route GetBucketPolicy = Route.builder()
        .method(HttpMethod.GET)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("policy"))
        .handler(bucketPolicy::get)
        .build();

    Route GetBucketPolicyStatus = Route.builder()
        .method(HttpMethod.GET)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("policyStatus"))
        .handler(new NotImplementedOperationController(serviceFactory, "GetBucketPolicyStatus", authHandlerService))
        .build();

    Route GetBucketReplication = Route.builder()
        .method(HttpMethod.GET)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("replication"))
        .handler(bucketReplicationController::get)
        .build();

    Route GetBucketRequestPayment = Route.builder()
        .method(HttpMethod.GET)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("requestPayment"))
        .handler(new NotImplementedOperationController(serviceFactory, "GetBucketRequestPayment", authHandlerService))
        .build();

    Route GetBucketTagging = Route.builder()
        .method(HttpMethod.GET)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("tagging"))
        .handler(new GetBucketTaggingController(serviceFactory, authHandlerService))
        .build();

    Route GetBucketVersioning = Route.builder()
        .method(HttpMethod.GET)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("versioning"))
        .handler(new GetBucketVersioningController(serviceFactory, authHandlerService))
        .build();

    Route GetBucketWebsite = Route.builder()
        .method(HttpMethod.GET)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("website"))
        .handler(new NotImplementedOperationController(serviceFactory, "GetBucketWebsite", authHandlerService))
        .build();

    Route GetObject = Route.builder()
        .method(HttpMethod.GET)
        .path(BUCKET_KEY_PATH)
        .handler(new GetObjectController(serviceFactory, authHandlerService))
        .build();

    Route GetObjectAcl = Route.builder()
        .method(HttpMethod.GET)
        .path(BUCKET_KEY_PATH)
        .paramMatcher(params -> params.containsKey("acl"))
        .handler(new NotImplementedOperationController(serviceFactory, "GetObjectAcl", authHandlerService))
        .build();

    Route GetObjectAttributes = Route.builder()
        .method(HttpMethod.HEAD)
        .path(BUCKET_KEY_PATH)
        .paramMatcher(params -> params.containsKey("attributes"))
        .handler(new NotImplementedOperationController(serviceFactory, "GetObjectAttributes", authHandlerService))
        .build();

    Route GetObjectLegalHold = Route.builder()
        .method(HttpMethod.GET)
        .path(BUCKET_KEY_PATH)
        .paramMatcher(params -> params.containsKey("legal-hold"))
        .handler(new NotImplementedOperationController(serviceFactory, "GetObjectLegalHold", authHandlerService))
        .build();

    Route GetObjectLockConfiguration = Route.builder()
        .method(HttpMethod.GET)
        .path(BUCKET_KEY_PATH)
        .paramMatcher(params -> params.containsKey("object-lock"))
        .handler(new NotImplementedOperationController(serviceFactory, "GetObjectLockConfiguration", authHandlerService))
        .build();

    Route GetObjectRetention = Route.builder()
        .method(HttpMethod.GET)
        .path(BUCKET_KEY_PATH)
        .paramMatcher(params -> params.containsKey("retention"))
        .handler(new NotImplementedOperationController(serviceFactory, "GetObjectRetention", authHandlerService))
        .build();

    Route GetObjectTagging = Route.builder()
        .method(HttpMethod.GET)
        .path(BUCKET_KEY_PATH)
        .paramMatcher(params -> params.containsKey("tagging"))
        .handler(objectTaggingController::get)
        .build();

    Route GetObjectTorrent = Route.builder()
        .method(HttpMethod.GET)
        .path(BUCKET_KEY_PATH)
        .paramMatcher(params -> params.containsKey("torrent"))
        .handler(new NotImplementedOperationController(serviceFactory, "GetObjectTorrent", authHandlerService))
        .build();

    Route GetPublicAccessBlock = Route.builder()
        .method(HttpMethod.GET)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("publicAccessBlock"))
        .handler(new NotImplementedOperationController(serviceFactory, "GetPublicAccessBlock", authHandlerService))
        .build();

    Route HeadBucket = Route.builder()
        .method(HttpMethod.HEAD)
        .path(BUCKET_PATH)
        .handler(new HeadBucketController(serviceFactory, authHandlerService))
        .build();

    Route HeadObject = Route.builder()
        .method(HttpMethod.HEAD)
        .path(BUCKET_KEY_PATH)
        .handler(new HeadObjectController(serviceFactory, authHandlerService))
        .build();

    Route ListBucketAnalyticsConfigurations = Route.builder()
        .method(HttpMethod.GET)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("analytics"))
        .handler(new NotImplementedOperationController(serviceFactory, "ListBucketAnalyticsConfigurations", authHandlerService))
        .build();

    Route ListBucketIntelligentTieringConfigurations = Route.builder()
        .method(HttpMethod.GET)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("intelligent-tiering"))
        .handler(new NotImplementedOperationController(serviceFactory, "ListBucketIntelligentTieringConfigurations", authHandlerService))
        .build();

    Route ListBucketInventoryConfigurations = Route.builder()
        .method(HttpMethod.GET)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("inventory"))
        .handler(new NotImplementedOperationController(serviceFactory, "ListBucketInventoryConfigurations", authHandlerService))
        .build();

    Route ListBucketMetricsConfigurations = Route.builder()
        .method(HttpMethod.GET)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("metrics"))
        .handler(new NotImplementedOperationController(serviceFactory, "ListBucketMetricsConfigurations", authHandlerService))
        .build();

    Route ListBuckets = Route.builder()
        .method(HttpMethod.GET)
        .path("/")
        .handler(new ListBucketsController(serviceFactory, authHandlerService))
        .build();

    Route ListMultipartUploads = Route.builder()
        .method(HttpMethod.GET)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("uploads"))
        .handler(new NotImplementedOperationController(serviceFactory, "ListMultipartUploads", authHandlerService))
        .build();

    Route ListObjects = Route.builder()
        .method(HttpMethod.GET)
        .path(BUCKET_PATH)
        .handler(new ListObjectsController(serviceFactory, authHandlerService))
        .build();

    Route ListObjectsV2 = Route.builder()
        .method(HttpMethod.GET)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("list-type") && params.get("list-type").get(0).equals("2"))
        .handler(new ListObjectsV2Controller(serviceFactory, authHandlerService))
        .build();

    Route ListObjectVersions = Route.builder()
        .method(HttpMethod.GET)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("versions"))
        .handler(new ListObjectVersionsController(serviceFactory, authHandlerService))
        .build();

    Route ListParts = Route.builder()
        .method(HttpMethod.GET)
        .path(BUCKET_KEY_PATH)
        .paramMatcher(params -> params.containsKey("uploadId"))
        .handler(new ListPartsController(serviceFactory, authHandlerService))
        .build();

    Route PutBucketAccelerateConfiguration = Route.builder()
        .method(HttpMethod.PUT)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("accelerate"))
        .handler(new NotImplementedOperationController(serviceFactory, "PutBucketAccelerateConfiguration", authHandlerService))
        .build();

    Route PutBucketAcl = Route.builder()
        .method(HttpMethod.PUT)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("acl"))
        .handler(new PutBucketAclController(serviceFactory, authHandlerService))
        .build();

    Route PutBucketAnalyticsConfiguration = Route.builder()
        .method(HttpMethod.PUT)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("analytics"))
        .handler(new NotImplementedOperationController(serviceFactory, "PutBucketAnalyticsConfiguration", authHandlerService))
        .build();

    Route PutBucketCors = Route.builder()
        .method(HttpMethod.PUT)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("cors"))
        .handler(new NotImplementedOperationController(serviceFactory, "PutBucketCors", authHandlerService))
        .build();

    Route PutBucketEncryption = Route.builder().method(HttpMethod.PUT)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("encryption"))
        .handler(bucketEncryptionController::put)
        .build();

    Route PutBucketIntelligentTieringConfiguration = Route.builder()
        .method(HttpMethod.PUT)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("intelligent-tiering"))
        .handler(new NotImplementedOperationController(serviceFactory, "PutBucketIntelligentTieringConfiguration", authHandlerService))
        .build();

    Route PutBucketInventoryConfiguration = Route.builder()
        .method(HttpMethod.PUT)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("inventory"))
        .handler(new NotImplementedOperationController(serviceFactory, "PutBucketInventoryConfiguration", authHandlerService))
        .build();

    Route PutBucketLifecycle = Route.builder()
        .method(HttpMethod.PUT)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("lifecycle"))
        .handler(new NotImplementedOperationController(serviceFactory, "PutBucketLifecycle", authHandlerService))
        .build();

    Route PutBucketLifecycleConfiguration = Route.builder()
        .method(HttpMethod.PUT)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("lifecycle"))
        .handler(new NotImplementedOperationController(serviceFactory, "PutBucketLifecycleConfiguration", authHandlerService))
        .build();

    Route PutBucketLogging = Route.builder()
        .method(HttpMethod.PUT)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("logging"))
        .handler(new NotImplementedOperationController(serviceFactory, "PutBucketLogging", authHandlerService))
        .build();

    Route PutBucketMetricsConfiguration = Route.builder()
        .method(HttpMethod.PUT)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("metrics"))
        .handler(new NotImplementedOperationController(serviceFactory, "PutBucketMetricsConfiguration", authHandlerService))
        .build();

    Route PutBucketNotification = Route.builder()
        .method(HttpMethod.PUT)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("notification"))
        .handler(new NotImplementedOperationController(serviceFactory, "PutBucketNotification", authHandlerService))
        .build();

    Route PutBucketNotificationConfiguration = Route.builder()
        .method(HttpMethod.PUT)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("notification"))
        .handler(new NotImplementedOperationController(serviceFactory, "PutBucketNotificationConfiguration", authHandlerService))
        .build();

    Route PutBucketOwnershipControls = Route.builder()
        .method(HttpMethod.PUT)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("ownershipControls"))
        .handler(new NotImplementedOperationController(serviceFactory, "PutBucketOwnershipControls", authHandlerService))
        .build();

    Route PutBucketPolicy = Route.builder()
        .method(HttpMethod.PUT)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("policy"))
        .handler(bucketPolicy::put)
        .build();

    Route PutBucketReplication = Route.builder().method(HttpMethod.PUT)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("replication"))
        .handler(bucketReplicationController::put)
        .build();

    Route PutBucketRequestPayment = Route.builder()
        .method(HttpMethod.PUT)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("requestPayment"))
        .handler(new NotImplementedOperationController(serviceFactory, "PutBucketRequestPayment", authHandlerService))
        .build();

    Route PutBucketTagging = Route.builder()
        .method(HttpMethod.PUT)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("tagging"))
        .handler(new PutBucketTaggingController(serviceFactory, authHandlerService))
        .build();

    Route PutBucketVersioning = Route.builder()
        .method(HttpMethod.PUT)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("versioning"))
        .handler(new PutBucketVersioningController(serviceFactory, authHandlerService))
        .build();

    Route PutBucketWebsite = Route.builder()
        .method(HttpMethod.PUT)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("website"))
        .handler(new NotImplementedOperationController(serviceFactory, "PutBucketWebsite", authHandlerService))
        .build();

    Route PutObject = Route.builder()
        .method(HttpMethod.PUT)
        .path(BUCKET_KEY_PATH)
        .handler(new PutObjectController(serviceFactory, authHandlerService))
        .build();

    Route PutObjectAcl = Route.builder()
        .method(HttpMethod.PUT)
        .path(BUCKET_KEY_PATH)
        .paramMatcher(params -> params.containsKey("acl"))
        .handler(new NotImplementedOperationController(serviceFactory, "PutObjectAcl", authHandlerService))
        .build();

    Route PutObjectLegalHold = Route.builder()
        .method(HttpMethod.PUT)
        .path(BUCKET_KEY_PATH)
        .paramMatcher(params -> params.containsKey("legal-hold"))
        .handler(new NotImplementedOperationController(serviceFactory, "PutObjectLegalHold", authHandlerService))
        .build();

    Route PutObjectLockConfiguration = Route.builder()
        .method(HttpMethod.PUT)
        .path(BUCKET_KEY_PATH)
        .paramMatcher(params -> params.containsKey("object-lock"))
        .handler(new NotImplementedOperationController(serviceFactory, "PutObjectLockConfiguration", authHandlerService))
        .build();

    Route PutObjectRetention = Route.builder()
        .method(HttpMethod.PUT)
        .path(BUCKET_KEY_PATH)
        .paramMatcher(params -> params.containsKey("retention"))
        .handler(new NotImplementedOperationController(serviceFactory, "PutObjectRetention", authHandlerService))
        .build();

    Route PutObjectTagging = Route.builder()
        .method(HttpMethod.PUT)
        .path(BUCKET_KEY_PATH)
        .paramMatcher(params -> params.containsKey("tagging"))
        .handler(objectTaggingController::put)
        .build();

    Route PutPublicAccessBlock = Route.builder()
        .method(HttpMethod.PUT)
        .path(BUCKET_PATH)
        .paramMatcher(params -> params.containsKey("publicAccessBlock"))
        .handler(new NotImplementedOperationController(serviceFactory, "PutPublicAccessBlock", authHandlerService))
        .build();

    Route RestoreObject = Route.builder()
        .method(HttpMethod.POST)
        .path(BUCKET_KEY_PATH)
        .paramMatcher(params -> params.containsKey("restore"))
        .handler(new NotImplementedOperationController(serviceFactory, "RestoreObject", authHandlerService))
        .build();

    Route SelectObjectContent = Route.builder()
        .method(HttpMethod.POST)
        .path(BUCKET_KEY_PATH)
        .paramMatcher(params -> params.containsKey("select"))
        .handler(new NotImplementedOperationController(serviceFactory, "SelectObjectContent", authHandlerService))
        .build();

    Route UploadPart = Route.builder()
        .method(HttpMethod.PUT)
        .path(BUCKET_KEY_PATH)
        .paramMatcher(params -> params.containsKey("uploadId") && params.containsKey("partNumber"))
        .handler(new UploadPartController(serviceFactory, authHandlerService))
        .build();

    Route UploadPartCopy = Route.builder()
        .method(HttpMethod.PUT)
        .path(BUCKET_KEY_PATH)
        .paramMatcher(params -> params.containsKey("uploadId") && params.containsKey("partNumber"))
        .headerMatcher(headers -> headers.containsKey(AmzHeaderNames.X_AMZ_COPY_SOURCE))
        .handler(new NotImplementedOperationController(serviceFactory, "UploadPartCopy", authHandlerService))
        .build();

    Route WriteGetObjectResponse = Route.builder()
        .method(HttpMethod.POST)
        .path("/WriteGetObjectResponse")
        .handler(new NotImplementedOperationController(serviceFactory, "WriteGetObjectResponse", authHandlerService))
        .build();



//    Route GetBucket = Route.builder()
//        .method(HttpMethod.GET)
//        .path("/v20180820/bucket/{bucket}")
//        .handler(new GetBucketController(serviceFactory))
//        .build();

    return new LocalS3Router()
        .route(AbortMultipartUpload)
        .route(CompleteMultipartUpload)
        .route(CopyObject)
        .route(CreateBucket)
        .route(CreateMultipartUpload)
        .route(DeleteBucket)
        .route(DeleteBucketAnalyticsConfiguration)
        .route(DeleteBucketCors)
        .route(DeleteBucketEncryption)
        .route(DeleteBucketIntelligentTieringConfiguration)
        .route(DeleteBucketInventoryConfiguration)
        .route(DeleteBucketLifecycle)
        .route(DeleteBucketMetricsConfiguration)
        .route(DeleteBucketOwnershipControls)
        .route(DeleteBucketPolicy)
        .route(DeleteBucketReplication)
        .route(DeleteBucketTagging)
        .route(DeleteBucketWebsite)
        .route(DeleteObject)
        .route(DeleteObjects)
        .route(DeleteObjectTagging)
        .route(DeletePublicAccessBlock)
        .route(GetBucketAccelerateConfiguration)
        .route(GetBucketAcl)
        .route(GetBucketAnalyticsConfiguration)
        .route(GetBucketCors)
        .route(GetBucketEncryption)
        .route(GetBucketIntelligentTieringConfiguration)
        .route(GetBucketInventoryConfiguration)
        .route(GetBucketLifecycle)
        .route(GetBucketLifecycleConfiguration)
        .route(GetBucketLocation)
        .route(GetBucketLogging)
        .route(GetBucketMetricsConfiguration)
        .route(GetBucketNotification)
        .route(GetBucketNotificationConfiguration)
        .route(GetBucketOwnershipControls)
        .route(GetBucketPolicy)
        .route(GetBucketPolicyStatus)
        .route(GetBucketReplication)
        .route(GetBucketRequestPayment)
        .route(GetBucketTagging)
        .route(GetBucketVersioning)
        .route(GetBucketWebsite)
        .route(GetObject)
        .route(GetObjectAcl)
        .route(GetObjectAttributes)
        .route(GetObjectLegalHold)
        .route(GetObjectLockConfiguration)
        .route(GetObjectRetention)
        .route(GetObjectTagging)
        .route(GetObjectTorrent)
        .route(GetPublicAccessBlock)
        .route(HeadBucket)
        .route(HeadObject)
        .route(ListBucketAnalyticsConfigurations)
        .route(ListBucketIntelligentTieringConfigurations)
        .route(ListBucketInventoryConfigurations)
        .route(ListBucketMetricsConfigurations)
        .route(ListBuckets)
        .route(ListMultipartUploads)
        .route(ListObjects)
        .route(ListObjectsV2)
        .route(ListObjectVersions)
        .route(ListParts)
        .route(PutBucketAccelerateConfiguration)
        .route(PutBucketAcl)
        .route(PutBucketAnalyticsConfiguration)
        .route(PutBucketCors)
        .route(PutBucketEncryption)
        .route(PutBucketIntelligentTieringConfiguration)
        .route(PutBucketInventoryConfiguration)
        .route(PutBucketLifecycle)
        .route(PutBucketLifecycleConfiguration)
        .route(PutBucketLogging)
        .route(PutBucketMetricsConfiguration)
        .route(PutBucketNotification)
        .route(PutBucketNotificationConfiguration)
        .route(PutBucketOwnershipControls)
        .route(PutBucketPolicy)
        .route(PutBucketReplication)
        .route(PutBucketRequestPayment)
        .route(PutBucketTagging)
        .route(PutBucketVersioning)
        .route(PutBucketWebsite)
        .route(PutObject)
        .route(PutObjectAcl)
        .route(PutObjectLegalHold)
        .route(PutObjectLockConfiguration)
        .route(PutObjectRetention)
        .route(PutObjectTagging)
        .route(PutPublicAccessBlock)
        .route(RestoreObject)
        .route(SelectObjectContent)
        .route(UploadPart)
        .route(UploadPartCopy)
        .route(WriteGetObjectResponse)
        //.route(GetBucket)

        .notFound(new NotFoundHandler(authHandlerService))
        .exceptionHandler(LocalS3Exception.class, new LocalS3ExceptionHandler(serviceFactory))
        .exceptionHandler(IllegalArgumentException.class, new IllegalArgumentExceptionHandler())
        .exceptionHandler(LocalS3InvalidArgumentException.class, new LocalS3InvalidArgumentExceptionHandler())
        .exceptionHandler(Exception.class, new ExceptionHandler())
        ;
  }

}
