package com.robothy.s3.core.service;

import com.amazonaws.services.s3.AmazonS3;
import com.robothy.s3.core.model.Bucket;
import com.robothy.s3.core.model.internal.LocalS3Metadata;

public class S3GatewayBucketService implements BucketService, CreateBucketService {

    private final AmazonS3 amazonS3Client;
    private final LocalS3Metadata localS3Metadata;

    public S3GatewayBucketService(
            final AmazonS3 amazonS3Client,
            final LocalS3Metadata localS3Metadata
    ) {
        this.amazonS3Client = amazonS3Client;
        this.localS3Metadata = localS3Metadata;
    }

    @Override
    public Bucket createBucket(
            final String bucketName,
            final String region
    ) {
        amazonS3Client.createBucket(bucketName);
        return Bucket
                .builder()
                .name(bucketName)
                .build();
    }

    @Override
    public Bucket createBucket(final String bucketName) {
        amazonS3Client.createBucket(bucketName);
        return Bucket
                .builder()
                .name(bucketName)
                .build();
    }

    @Override
    public Bucket deleteBucket(final String bucketName) {
        amazonS3Client.deleteBucket(bucketName);
        return null;
    }

    @Override
    public Bucket getBucket(final String bucketName) {
        final boolean isBucketExists = amazonS3Client.doesBucketExistV2(bucketName);
        return Bucket
                .builder()
                .name(isBucketExists ? bucketName : null)
                .build();
    }

    @Override
    public Bucket setVersioningEnabled(final String bucketName, final boolean versioningEnabled) {
        return null;
    }

    @Override
    public Boolean getVersioningEnabled(final String bucketName) {
        return null;
    }

    @Override
    public LocalS3Metadata localS3Metadata() {
        return localS3Metadata;
    }
}
