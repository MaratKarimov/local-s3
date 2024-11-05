package com.robothy.s3.core.service.manager;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.robothy.s3.core.model.internal.BucketMetadata;
import com.robothy.s3.core.model.internal.LocalS3Metadata;
import com.robothy.s3.core.service.BucketService;
import com.robothy.s3.core.service.ObjectService;
import com.robothy.s3.core.service.S3GatewayBucketService;
import com.robothy.s3.core.service.S3GatewayObjectService;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class S3GatewayLocalS3Manager implements LocalS3Manager {

    private final AmazonS3 amazonS3Client;
    private final LocalS3Metadata localS3Metadata;

    public S3GatewayLocalS3Manager(){
        final ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setSignerOverride("AWSS3V4SignerType");
        this.amazonS3Client = AmazonS3ClientBuilder
                .standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(findEndpoint(), Regions.US_EAST_1.name()))
                .withCredentials(new EnvironmentVariableCredentialsProvider())
                .withPathStyleAccessEnabled(Boolean.TRUE)
                .withClientConfiguration(clientConfiguration)
                .build();
        localS3Metadata = new S3GatewayLocalS3Metadata();
    }

    @Override
    public BucketService bucketService() {
        return new S3GatewayBucketService(amazonS3Client, localS3Metadata);
    }

    @Override
    public ObjectService objectService() {
        return new S3GatewayObjectService(amazonS3Client, localS3Metadata);
    }

    private String findEndpoint(){
        return System.getenv("S3_GATEWAY_ENDPOINT");
    }

    public class S3GatewayLocalS3Metadata extends LocalS3Metadata {
        public List<BucketMetadata> listBuckets(final Comparator<BucketMetadata> comparator) {
            final List<Bucket> buckets = amazonS3Client.listBuckets();
            final List<BucketMetadata> bucketList = buckets.stream().map(this::convert).collect(Collectors.toList());
            bucketList.sort(comparator);
            return bucketList;
        }

        private BucketMetadata convert(Bucket bucket) {
            final BucketMetadata bucketMetadata = new BucketMetadata();
            bucketMetadata.setBucketName(bucket.getName());
            bucketMetadata.setCreationDate(bucket.getCreationDate().toInstant().toEpochMilli());
            return bucketMetadata;
        }
    }
}