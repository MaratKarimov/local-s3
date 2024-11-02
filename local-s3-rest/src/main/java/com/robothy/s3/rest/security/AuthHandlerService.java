package com.robothy.s3.rest.security;

import com.robothy.netty.http.HttpRequest;
import com.robothy.s3.core.exception.InvalidSecurityException;
import com.robothy.s3.rest.constants.AmzHeaderNames;
import com.robothy.s3.rest.constants.AmzHeaderValues;
import com.robothy.s3.rest.model.request.DecodedAmzRequestBody;
import com.robothy.s3.rest.model.security.S3AuthHeader;
import com.robothy.s3.rest.model.security.enums.S3AuthType;
import com.robothy.s3.rest.utils.AwsChunkedDecodingInputStream;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.apache.commons.codec.binary.Base16;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RequiredArgsConstructor
public class AuthHandlerService {

    private final S3AuthService s3AuthService;
    private final AWSSignatureV2Service awsSignatureV2Service;
    private final AWSSignatureV4Service awsSignatureV4Service;

    private static final long MAX_REQUEST_TIME_SKEW_MINUTES = 15 * 60;

    private static final String ACCESS_KEY_ID_HEADER_NAME = "AWSAccessKeyId";

    private final DateFormat iso8601Formatter = new SimpleDateFormat("yyyyMMdd'T'HHmmss'Z'") {{
        setTimeZone(TimeZone.getTimeZone("UTC"));
    }};

    public S3Auth process(final HttpRequest request) {
        if(findIsAuthEnabled()){
            // find is anonymous
            final boolean isAnonymous = isAnonymous(request);
            // handle anonymous request
            if(isAnonymous){
                return S3Auth.builder().isAnonymous(true).build();
            }
            // parse s3 auth header
            final S3AuthHeader s3AuthHeader = s3AuthService.generateS3Auth(request);
            // check request time
            checkRequestTime(request, s3AuthHeader.getS3AuthType());
            final String contentDigest;
            // check content SHA256
            if(s3AuthHeader.getS3AuthType().equals(S3AuthType.V4)){
                contentDigest = calculateContentDigest(request, s3AuthHeader);
                checkContentSha256(request, contentDigest);
            } else {
                contentDigest = null;
            }
            // check key
            checkKeyValid(request, s3AuthHeader, contentDigest);

            final boolean isAdmin = s3AuthHeader.getIdentity().equals(findAccessKey());
            return S3Auth.builder()
                    .isAnonymous(false)
                    .isAdmin(isAdmin)
                    .identity(s3AuthHeader.getIdentity())
                    .build();
        } else {
            return S3Auth.builder().isAnonymous(true).build();
        }
    }

    public void checkKeyValid(final HttpRequest request, final S3AuthHeader s3AuthHeader, final String contentDigest) {
        if(findAccessKey().equals(s3AuthHeader.getIdentity())){
            final boolean isSessionTokenEmpty = s3AuthHeader.getSessionToken() == null || s3AuthHeader.getSessionToken().isEmpty();
            final boolean isIdentityEmpty = s3AuthHeader.getIdentity() == null || s3AuthHeader.getIdentity().isEmpty();
            if(isIdentityEmpty && isSessionTokenEmpty) {
                throw new InvalidSecurityException("Access key and session token are empty");
            }
            final String expectedSignature;
            if(s3AuthHeader.getS3AuthType().equals(S3AuthType.V2)) {
                expectedSignature = awsSignatureV2Service.createAuthorizationSignature(request, findSecretKey());
            } else if(s3AuthHeader.getS3AuthType().equals(S3AuthType.V4)) {
                try {
                    expectedSignature = awsSignatureV4Service.createAuthorizationSignature(request, s3AuthHeader, contentDigest, findSecretKey());
                } catch (final Exception e) {
                    throw new InvalidSecurityException("Invalid signature");
                }
            } else {
                throw new InvalidSecurityException("Unknown S3 auth version");
            }
            // AWS does not check signatures with OPTIONS verb
            if (!request.getMethod().equals(HttpMethod.OPTIONS) && !constantTimeEquals(expectedSignature, s3AuthHeader.getSignature())) {
                throw new InvalidSecurityException("Signature does not match");
            }
        } else {
            throw new InvalidSecurityException("Invalid access key");
        }
    }

    private boolean constantTimeEquals(final String x, final String y) {
        return MessageDigest.isEqual(
                x.getBytes(StandardCharsets.UTF_8),
                y.getBytes(StandardCharsets.UTF_8)
        );
    }

    public boolean findIsAuthEnabled(){
        final String source = System.getenv("AWS_S3_AUTH_ENABLED");
        if(source != null && !source.isEmpty()){
            return Boolean.parseBoolean(source);
        } else {
            return false;
        }
    }

    public String findAccessKey(){
        return System.getenv("AWS_ACCESS_KEY_ID");
    }

    public String findSecretKey(){
        return System.getenv("AWS_SECRET_ACCESS_KEY");
    }

    public byte[] calculateContentDigest(final DecodedAmzRequestBody body, final S3AuthHeader s3AuthHeader) {
        try(final DigestInputStream digestInputStream = new DigestInputStream(
                body.getDecodedBody(), MessageDigest.getInstance(s3AuthHeader.getHashAlgorithm()))
        ){
            final byte[] bytes = new byte[8192];
            try {
                while(digestInputStream.read(bytes) != -1) {}
            } catch (final Exception e) {
                throw new InvalidSecurityException("Can't read request body");
            }
            return digestInputStream.getMessageDigest().digest();
        } catch (final IOException e) {
            throw new InvalidSecurityException("Can't read request body");
        } catch (final NoSuchAlgorithmException e) {
            throw new InvalidSecurityException("Invalid signature");
        }
    }

    private DecodedAmzRequestBody getDuplicateBody(final HttpRequest request) {
        final DecodedAmzRequestBody result = new DecodedAmzRequestBody();
        final ByteBuf duplicate = request.getBody().duplicate();
        if (request.header(AmzHeaderNames.X_AMZ_CONTENT_SHA256)
                .map(AmzHeaderValues.STREAMING_AWS4_HMAC_SHA_256_PAYLOAD::equals).orElse(false)) {
            result.setDecodedBody(new AwsChunkedDecodingInputStream(new ByteBufInputStream(duplicate)));
            result.setDecodedContentLength(request.header(AmzHeaderNames.X_AMZ_DECODED_CONTENT_LENGTH).map(Long::parseLong)
                    .orElseThrow(() -> new IllegalArgumentException(AmzHeaderNames.X_AMZ_DECODED_CONTENT_LENGTH + "header not exist.")));
        } else {
            result.setDecodedBody(new ByteBufInputStream(duplicate));
            final Optional<String> contentLengthOpt = request.header(HttpHeaderNames.CONTENT_LENGTH.toString());
            if(contentLengthOpt.isPresent() && !contentLengthOpt.get().isEmpty()){
                final Long contentLengthOptSource = Long.getLong(contentLengthOpt.get());
                if(contentLengthOptSource != null) result.setDecodedContentLength(contentLengthOptSource);
            }
        }
        return result;
    }

    private void checkContentSha256(final HttpRequest request, final String contentDigest) {
        final Optional<String> contentSha256Optional = s3AuthService.findHeaderValue(request, AmzHeaderNames.X_AMZ_CONTENT_SHA256);
        if(contentSha256Optional.isPresent()) {
            final String contentSha256 = contentSha256Optional.get();
            if  (!contentSha256.equals(contentDigest)) {
                throw new InvalidSecurityException("The provided 'x-amz-content-sha256' header does not match what was computed.");
            }
        }
    }

    private String calculateContentDigest(final HttpRequest request, final S3AuthHeader s3AuthHeader) {
        final DecodedAmzRequestBody decodedAmzRequestBody = getDuplicateBody(request);
        final byte[] contentDigest = calculateContentDigest(decodedAmzRequestBody, s3AuthHeader);
        return (new Base16(true)).encodeToString(contentDigest);
    }

    private void checkRequestTime(final HttpRequest request, final S3AuthType s3AuthType) {
        // parse date headers
        final Optional<String> dateHeaderValueOptional = s3AuthService.findHeaderValue(request, HttpHeaderNames.DATE.toString());
        final Optional<String> xAmzDateHeaderValueOptional = s3AuthService.findHeaderValue(request, AmzHeaderNames.X_AMZ_DATE);

        // check date headers
        if(dateHeaderValueOptional.isEmpty() && xAmzDateHeaderValueOptional.isEmpty()){
            throw new InvalidSecurityException("AWS authentication requires a valid Date or X-Amz-Date header");
        }

        // check request date
        final Optional<Long> requestTimeOpt = parseRequestTime(
                dateHeaderValueOptional,
                xAmzDateHeaderValueOptional,
                s3AuthType
        );

        if(requestTimeOpt.isPresent()){
            final long requestTime = requestTimeOpt.get();
            if (requestTime > 0) {
                final long now = System.currentTimeMillis() / 1000;
                if (now + MAX_REQUEST_TIME_SKEW_MINUTES < requestTime || now - MAX_REQUEST_TIME_SKEW_MINUTES > requestTime) {
                    throw new InvalidSecurityException("Request time is too skewed");
                }
            } else {
                throw new InvalidSecurityException("Access denied");
            }
        }
    }

    private Optional<Long> parseRequestTime(
            final Optional<String> dateHeaderValueOptional,
            final Optional<String> xAmzDateHeaderValueOptional,
            final S3AuthType s3AuthType
    ){
        if(s3AuthType.equals(S3AuthType.V2)) {
            return parseRequestTimeFromDate(dateHeaderValueOptional);
        } else if (s3AuthType.equals(S3AuthType.V4)) {
            if (xAmzDateHeaderValueOptional.isPresent()) {
                return Optional.of(parseIso8601(xAmzDateHeaderValueOptional.get()));
            } else if (dateHeaderValueOptional.isPresent()) {
                return parseRequestTimeFromDate(dateHeaderValueOptional);
            }
        } else {
            throw new InvalidSecurityException("Access denied");
        }
        return Optional.empty();
    }

    private Optional<Long> parseRequestTimeFromDate(final Optional<String> dateHeaderValueOptional){
        if (dateHeaderValueOptional.isPresent()) {
            final String d = dateHeaderValueOptional.get();
            try {
                final long t = Long.getLong(d);
                return Optional.of(t / 1000);
            } catch (final IllegalArgumentException iae) {
                try {
                    return Optional.of(parseIso8601(d));
                } catch (final IllegalArgumentException iae2) {
                    throw new InvalidSecurityException("Access denied");
                }
            }
        }
        return Optional.empty();
    }

    /** Parse ISO 8601 timestamp into seconds since 1970. */
    private long parseIso8601(final String date) {
        try {
            return iso8601Formatter.parse(date).getTime() / 1000;
        } catch (final ParseException pe) {
            throw new InvalidSecurityException("Access denied");
        }
    }

    private void handleAnonymous(final HttpRequest request){
    }

    private boolean isAnonymous(final HttpRequest request){
        return findIsMethodAllowAnonymous(request)
                // http auth header
                && !hasAuth(request)
                // v4
                && !s3AuthService.hasHeader(request, AmzHeaderNames.X_AMZ_ALGORITHM)
                // v2
                && !s3AuthService.hasHeader(request, ACCESS_KEY_ID_HEADER_NAME)
                // sts
                && !s3AuthService.hasHeader(request, AmzHeaderNames.X_AMZ_SECURITY_TOKEN);
    }

    private boolean hasAuth(final HttpRequest request){
        return s3AuthService.hasHeader(request, HttpHeaderNames.AUTHORIZATION.toString());
    }

    private boolean findIsMethodAllowAnonymous(final HttpRequest request){
        return request.getMethod().equals(HttpMethod.GET)
                || request.getMethod().equals(HttpMethod.HEAD)
                || request.getMethod().equals(HttpMethod.OPTIONS);
    }

    @Data
    @RequiredArgsConstructor
    @Builder
    public static class S3Auth {
        private final boolean isAnonymous;
        private final boolean isAdmin;
        private final String identity;
    }
}