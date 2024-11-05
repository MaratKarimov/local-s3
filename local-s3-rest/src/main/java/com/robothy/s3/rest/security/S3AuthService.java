package com.robothy.s3.rest.security;

import com.robothy.netty.http.HttpRequest;
import com.robothy.s3.core.exception.InvalidSecurityException;
import com.robothy.s3.rest.constants.AmzHeaderNames;
import com.robothy.s3.rest.model.security.S3AuthHeader;
import com.robothy.s3.rest.model.security.enums.S3AuthType;
import io.netty.handler.codec.http.HttpHeaderNames;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class S3AuthService {

    private static final String SIGNATURE_FIELD = "Signature=";
    private static final String CREDENTIAL_FIELD = "Credential=";
    private static final String SIGNED_HEADERS_FIELD = "SignedHeaders=";

    private static final String V2_SIGN = "AWS ";
    private static final String V4_SIGN = "AWS4-HMAC";

    private static final Map<String, String> DIGEST_MAP = Map.of(
            "SHA256", "SHA-256",
            "SHA1", "SHA-1",
            "MD5", "MD5"
    );

    public final S3AuthHeader generateS3Auth(final HttpRequest request){
        final String authHeader = request.getHeaders().get(HttpHeaderNames.AUTHORIZATION.toString());
        final S3AuthType s3AuthType = findS3AuthType(authHeader);
        if (s3AuthType.equals(S3AuthType.V4)) {
            return generateS3AuthV4(request, authHeader);
        } else if (s3AuthType.equals(S3AuthType.V2)){
            return generateS3AuthV2(request, authHeader);
        } else {
            throw new InvalidSecurityException("Invalid auth header");
        }
    }

    public final S3AuthType findS3AuthType(final String authHeader){
        if(authHeader.startsWith(V2_SIGN)){
            return S3AuthType.V2;
        } else if (authHeader.startsWith(V4_SIGN)) {
            return S3AuthType.V4;
        } else {
            throw new InvalidSecurityException("Invalid auth header");
        }
    }

    public final S3AuthHeader generateS3AuthV2(final HttpRequest request, final String authHeader){
        if(authHeader.startsWith(V2_SIGN)){
            final String[] authHeaderSplitted = authHeader.split(" ");
            if(authHeaderSplitted.length != 2){
                throw new InvalidSecurityException("Invalid auth header");
            }
            final String[] identitySplitted = authHeaderSplitted[1].split(":");
            if(identitySplitted.length != 2){
                throw new InvalidSecurityException("Invalid auth header");
            }
            final String sessionToken = findHeaderValue(request, AmzHeaderNames.X_AMZ_SECURITY_TOKEN).orElse(null);
            return S3AuthHeader
                    .builder()
                    .s3AuthType(S3AuthType.V2)
                    .identity(identitySplitted[0])
                    .signature(identitySplitted[1])
                    .sessionToken(sessionToken)
                    .build();
        } else {
            throw new InvalidSecurityException("Invalid auth header");
        }
    }

    public final S3AuthHeader generateS3AuthV4(final HttpRequest request, final String authHeader){
        if(authHeader.startsWith(V4_SIGN)){
            final String[] credentialHeaderParams = extractCredentialHeaderParam(authHeader);
            final String signatureHeaderParam = extractSignatureHeaderParam(authHeader);
            final List<String> signedHeadersHeaderParam = extractSignedHeadersHeaderParam(authHeader);

            final String awsSignatureVersion = authHeader.substring(0, authHeader.indexOf(' '));
            final String sessionToken = findHeaderValue(request, AmzHeaderNames.X_AMZ_SECURITY_TOKEN).orElse(null);
            return S3AuthHeader.builder()
                    .s3AuthType(S3AuthType.V4)
                    .identity(credentialHeaderParams[0])
                    .date(credentialHeaderParams[1])
                    .region(credentialHeaderParams[2])
                    .service(credentialHeaderParams[3])
                    .signature(signatureHeaderParam)
                    .hashAlgorithm(DIGEST_MAP.get(awsSignatureVersion.split("-")[2]))
                    .hmacAlgorithm("Hmac" + awsSignatureVersion.split("-")[2])
                    .signedHeaders(signedHeadersHeaderParam)
                    .sessionToken(sessionToken)
                    .build();
        } else {
            throw new InvalidSecurityException("Invalid auth header");
        }
    }

    private List<String> extractSignedHeadersHeaderParam(final String header) {
        final String source = extractHeaderParam(header, SIGNED_HEADERS_FIELD);
        final String[] splittedSource = source.split(";");
        final List<String> signedHeaders = Arrays.asList(splittedSource);
        //if(!signedHeaders.contains("host")){
        //    throw new InvalidSecurityException("Signed headers must contain host");
        //}
        return signedHeaders;
    }

    private String[] extractCredentialHeaderParam(final String header) {
        final String source = extractHeaderParam(header, CREDENTIAL_FIELD);
        final String[] splittedSource = source.split("/");
        if (splittedSource.length != 5) {
            throw new InvalidSecurityException("Invalid Credential: " + source);
        }
        return splittedSource;
    }

    private String extractSignatureHeaderParam(final String header) {
        return extractHeaderParam(header, SIGNATURE_FIELD);
    }

    private String extractHeaderParam(final String header, final String param) {
        int paramIndex = header.indexOf(param);
        if (paramIndex < 0) {
            throw new InvalidSecurityException("Invalid header param" + param);
        }
        paramIndex += param.length();
        final int paramEnd = header.indexOf(',', paramIndex);
        if (paramEnd < 0) {
            return header.substring(paramIndex);
        } else {
            return header.substring(paramIndex, paramEnd);
        }
    }

    public final boolean hasHeader(final HttpRequest request, final String toFind){
        return findHeaderValue(request, toFind).isPresent();
    }

    public final Optional<String> findHeaderValue(final HttpRequest request, final String headerKey){
        for(final CharSequence it: request.getHeaders().keySet()){
            if(it.toString().equalsIgnoreCase(headerKey)){
                final String value = request.getHeaders().get(it);
                if(value != null && !value.isEmpty()){
                    return Optional.of(value);
                }
            }
        }
        return Optional.empty();
    }
}