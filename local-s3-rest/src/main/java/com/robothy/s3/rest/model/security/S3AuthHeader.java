package com.robothy.s3.rest.model.security;

import com.robothy.s3.rest.model.security.enums.S3AuthType;
import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class S3AuthHeader {
    private final S3AuthType s3AuthType;
    private final String hmacAlgorithm;
    private final String hashAlgorithm;
    private final String region;
    private final String date;
    private final String service;
    private final String identity;
    private final String signature;
    private final List<String> signedHeaders;
    private final String sessionToken;
}