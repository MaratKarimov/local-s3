package com.robothy.s3.core.service;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.robothy.s3.core.model.answers.*;
import com.robothy.s3.core.model.internal.LocalS3Metadata;
import com.robothy.s3.core.model.request.*;
import com.robothy.s3.core.storage.Storage;
import com.robothy.s3.datatypes.request.DeleteObjectsRequest;
import com.robothy.s3.datatypes.response.DeleteResult;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class S3GatewayObjectService implements ObjectService {

    private final AmazonS3 amazonS3Client;
    private final LocalS3Metadata localS3Metadata;

    public S3GatewayObjectService(
            final AmazonS3 amazonS3Client,
            final LocalS3Metadata localS3Metadata
    ) {
        this.amazonS3Client = amazonS3Client;
        this.localS3Metadata = localS3Metadata;
    }

    @Override
    public UploadPartAns uploadPart(
            final String bucket,
            final String key,
            final String uploadId,
            final Integer partNumber,
            final UploadPartOptions options
    ) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public PutObjectAns putObject(
            final String bucketName,
            final String key,
            final PutObjectOptions options
    ) {
        final ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentType(options.getContentType());
        objectMetadata.setContentLength(options.getSize());
        final PutObjectResult putObjectResult = amazonS3Client.putObject(
                bucketName,
                key,
                options.getContent(),
                objectMetadata
        );
        return PutObjectAns
                .builder()
                .key(key)
                .etag(putObjectResult.getETag())
                .creationDate(System.currentTimeMillis())
                .build();
    }

    @Override
    public String putObjectTagging(
            final String bucketName,
            final String key,
            final String versionId,
            final String[][] tagging
    ) {
        final List<Tag> tagSet = new ArrayList<>(tagging.length);
        for (final String[] strings : tagging) {
            tagSet.add(new Tag(strings[0], strings[1]));
        }
        return amazonS3Client.setObjectTagging(
                new SetObjectTaggingRequest(bucketName, key, new ObjectTagging(tagSet))
        ).getVersionId();
    }

    @Override
    public GetObjectTaggingAns getObjectTagging(
            final String bucketName,
            final String key,
            final String versionId
    ) {
        final GetObjectTaggingRequest getObjectTaggingRequest = new GetObjectTaggingRequest(bucketName, key, versionId);
        final GetObjectTaggingResult getObjectTaggingResult = amazonS3Client.getObjectTagging(getObjectTaggingRequest);
        final String[][] tagging = getObjectTaggingResult
                .getTagSet()
                .stream()
                .map(tag -> new String[]{tag.getKey(), tag.getValue()})
                .toArray(String[][]::new);
        return GetObjectTaggingAns
                .builder()
                .tagging(tagging)
                .versionId(versionId)
                .build();
    }

    @Override
    public String deleteObjectTagging(
            final String bucketName,
            final String key,
            final String versionId
    ) {
        final DeleteObjectTaggingRequest deleteObjectTaggingRequest = new DeleteObjectTaggingRequest(bucketName, key);
        deleteObjectTaggingRequest.setVersionId(versionId);
        return amazonS3Client.deleteObjectTagging(deleteObjectTaggingRequest).getVersionId();
    }

    @Override
    public ListPartsAns listParts(
            final String bucket,
            final String key,
            final String uploadId,
            final Integer maxParts,
            final Integer partNumberMarker
    ) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public ListObjectsAns listObjects(
            final String bucket,
            final String delimiter,
            final String encodingType,
            final String marker,
            final int maxKeys,
            final String prefix
    ) {
        final ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
        listObjectsRequest.setBucketName(bucket);
        listObjectsRequest.setDelimiter(delimiter);
        listObjectsRequest.setEncodingType(encodingType);
        listObjectsRequest.setMarker(marker);
        listObjectsRequest.setMaxKeys(maxKeys);
        listObjectsRequest.setPrefix(prefix);
        final ObjectListing objectListing = amazonS3Client.listObjects(listObjectsRequest);
        final List<com.robothy.s3.datatypes.response.S3Object> objects = objectListing.getObjectSummaries()
                .stream()
                .map(this::convert)
                .collect(Collectors.toList());
        return ListObjectsAns
                .builder()
                .encodingType(objectListing.getEncodingType())
                .delimiter(delimiter)
                .marker(marker)
                .nextMarker(objectListing.getNextMarker())
                .maxKeys(objectListing.getMaxKeys())
                .isTruncated(objectListing.isTruncated())
                .prefix(objectListing.getPrefix())
                .commonPrefixes(objectListing.getCommonPrefixes())
                .objects(objects)
                .build();
    }

    private com.robothy.s3.datatypes.response.S3Object convert(final S3ObjectSummary s){
        final com.robothy.s3.datatypes.response.S3Object s3Object = new com.robothy.s3.datatypes.response.S3Object();
        s3Object.setSize(s.getSize());
        s3Object.setKey(s.getKey());
        s3Object.setEtag(s.getETag());
        s3Object.setLastModified(s.getLastModified().toInstant());
        return s3Object;
    }

    @Override
    public ListObjectsV2Ans listObjectsV2(
            final String bucket,
            final String continuationToken,
            final String delimiter,
            final String encodingType,
            final boolean fetchOwner,
            final int maxKeys,
            final String prefix,
            final String startAfter
    ) {
        final ListObjectsV2Request listObjectsV2Request = new ListObjectsV2Request();
        listObjectsV2Request.setBucketName(bucket);
        listObjectsV2Request.setContinuationToken(continuationToken);
        listObjectsV2Request.setDelimiter(delimiter);
        listObjectsV2Request.setEncodingType(encodingType);
        listObjectsV2Request.setFetchOwner(fetchOwner);
        listObjectsV2Request.setMaxKeys(maxKeys);
        listObjectsV2Request.setPrefix(prefix);
        listObjectsV2Request.setStartAfter(startAfter);
        final ListObjectsV2Result listObjectsV2Result = amazonS3Client.listObjectsV2(listObjectsV2Request);
        final List<com.robothy.s3.datatypes.response.S3Object> objects = listObjectsV2Result.getObjectSummaries()
                .stream()
                .map(this::convert)
                .collect(Collectors.toList());
        return ListObjectsV2Ans
                .builder()
                .startAfter(listObjectsV2Result.getStartAfter())
                .delimiter(listObjectsV2Result.getDelimiter())
                .commonPrefixes(listObjectsV2Result.getCommonPrefixes())
                .continuationToken(listObjectsV2Result.getContinuationToken())
                .encodingType(listObjectsV2Result.getEncodingType())
                .isTruncated(listObjectsV2Result.isTruncated())
                .keyCount(listObjectsV2Result.getKeyCount())
                .maxKeys(listObjectsV2Result.getMaxKeys())
                .nextContinuationToken(listObjectsV2Result.getNextContinuationToken())
                .objects(objects)
                .build();
    }

    @Override
    public ListObjectVersionsAns listObjectVersions(
            final String bucket,
            final String delimiter,
            final String keyMarker,
            final int maxKeys,
            final String prefix,
            final String versionIdMarker
    ) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public GetObjectAns getObject(
            final String bucketName,
            final String key,
            final GetObjectOptions options
    ) {
        final GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, key, options.getVersionId().orElse(null));
        final S3Object s3Object = amazonS3Client.getObject(getObjectRequest);
        final ObjectMetadata objectMetadata = s3Object.getObjectMetadata();
        final InputStream is = s3Object.getObjectContent() != null ? s3Object.getObjectContent().getDelegateStream() : null;
        return GetObjectAns
                .builder()
                .content(is)
                .contentType(objectMetadata != null ? objectMetadata.getContentType() : null)
                .size(objectMetadata != null ? objectMetadata.getContentLength() : 0L)
                .lastModified(objectMetadata != null && objectMetadata.getLastModified() != null ? objectMetadata.getLastModified().toInstant().toEpochMilli() : 0L)
                .taggingCount(s3Object.getTaggingCount())
                .versionId(objectMetadata != null ? objectMetadata.getVersionId() : null)
                .bucketName(s3Object.getBucketName())
                .key(s3Object.getKey())
                .etag(objectMetadata != null ? objectMetadata.getETag() : null)
                .build();
    }

    @Override
    public GetObjectAns headObject(
            final String bucketName,
            final String key,
            final GetObjectOptions options
    ) {
        final ObjectMetadata objectMetadata = amazonS3Client.getObjectMetadata(bucketName, key);
        return GetObjectAns
                .builder()
                .etag(objectMetadata.getETag())
                .bucketName(bucketName)
                .key(key)
                .userMetadata(objectMetadata.getUserMetadata())
                .size(objectMetadata.getContentLength())
                .versionId(objectMetadata.getVersionId())
                .build();
    }

    @Override
    public DeleteObjectAns deleteObject(
            final String bucketName,
            final String key
    ) {
        final GetObjectAns getObjectAns = headObject(bucketName, key, null);
        amazonS3Client.deleteObject(bucketName, key);
        return DeleteObjectAns.builder()
                .versionId(getObjectAns.getVersionId())
                .build();
    }

    @Override
    public DeleteObjectAns deleteObject(
            final String bucketName,
            final String key,
            final String versionId
    ) {
        amazonS3Client.deleteVersion(bucketName, key, versionId);
        return DeleteObjectAns.builder()
                .versionId(versionId)
                .build();
    }

    @Override
    public List<Object> deleteObjects(
            final String bucketName,
            final DeleteObjectsRequest request
    ) {
        final com.amazonaws.services.s3.model.DeleteObjectsRequest deleteObjectsRequest = new com.amazonaws.services.s3.model.DeleteObjectsRequest(bucketName);
        final List<com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion> keyVersions = request.getObjects()
                .stream()
                .map(it -> new com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion(it.getKey(), it.getVersionId().orElse(null)))
                .collect(Collectors.toList());
        deleteObjectsRequest.setKeys(keyVersions);
        final DeleteObjectsResult deleteObjectsResult = amazonS3Client.deleteObjects(deleteObjectsRequest);
        return deleteObjectsResult.getDeletedObjects().stream().map(this::convert).collect(Collectors.toList());
    }

    private DeleteResult.Deleted convert(final DeleteObjectsResult.DeletedObject deletedObject) {
        final DeleteResult.Deleted deleted = new DeleteResult.Deleted();
        deleted.setKey(deletedObject.getKey());
        deleted.setVersionId(deletedObject.getVersionId());
        if (deletedObject.isDeleteMarker()) {
            deleted.setDeleteMarker(true);
            deleted.setDeleteMarkerVersionId(deletedObject.getVersionId());
        }
        return deleted;
    }

    @Override
    public String createMultipartUpload(
            final String bucket,
            final String key,
            final CreateMultipartUploadOptions options
    ) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public CopyObjectAns copyObject(
            final String bucket,
            final String key,
            final CopyObjectOptions options
    ) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public CompleteMultipartUploadAns completeMultipartUpload(
            final String bucket,
            final String key,
            final String uploadId,
            final List<CompleteMultipartUploadPartOption> completeParts
    ) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void abortMultipartUpload(
            final String bucketName,
            final String objectKey,
            final String uploadId
    ) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Storage storage() {
        return null;
    }

    @Override
    public LocalS3Metadata localS3Metadata() {
        return localS3Metadata;
    }
}
