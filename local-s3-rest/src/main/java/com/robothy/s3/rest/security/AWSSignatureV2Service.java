package com.robothy.s3.rest.security;

import com.robothy.netty.http.HttpRequest;
import com.robothy.s3.core.exception.InvalidSecurityException;
import com.robothy.s3.rest.constants.AmzHeaderNames;
import io.netty.handler.codec.http.HttpHeaderNames;
import lombok.RequiredArgsConstructor;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

@RequiredArgsConstructor
public class AWSSignatureV2Service {

    private final S3AuthService s3AuthService;

    /**
     * Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/RESTAuthentication.html
     */
    public String createAuthorizationSignature(final HttpRequest request, final String secretKey){
        final String canonicalRequest = createCanonicalRequest(request, collectCanonicalHeaders(request));
        return createAuthorizationSignature(secretKey, canonicalRequest);
    }

    public String createAuthorizationSignature(final String secretKey, final String canonicalRequest){
        final Mac mac;
        try {
            mac = Mac.getInstance("HmacSHA1");
            mac.init(new SecretKeySpec(secretKey.getBytes(StandardCharsets.UTF_8), "HmacSHA1"));
        } catch (final InvalidKeyException | NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        return Base64.getEncoder()
                .encodeToString(
                        mac.doFinal(canonicalRequest.getBytes(StandardCharsets.UTF_8))
                );
    }

    public Map<String, String> collectCanonicalHeaders(final HttpRequest request){
        final Map<String, String> canonicalHeaders = new TreeMap<>();

        final boolean isAmzDateHeaderPresent = s3AuthService.hasHeader(request, AmzHeaderNames.X_AMZ_DATE);
        final boolean isDateHeaderPresent = s3AuthService.hasHeader(request, HttpHeaderNames.DATE.toString());

        final Set<CharSequence> headerNames = request.getHeaders().keySet();
        for(final CharSequence headerName:  headerNames){
            if(!headerName.toString().toLowerCase().startsWith("x-amz-")){
                continue;
            }
            if(isAmzDateHeaderPresent && isDateHeaderPresent && headerName.equals(HttpHeaderNames.DATE)){
                continue;
            }
            final String headerKey = headerName.toString().toLowerCase();
            final String headerValue = s3AuthService.findHeaderValue(request, headerName.toString()).orElse("");
            canonicalHeaders.put(headerKey, headerValue);
        }
        return canonicalHeaders;
    }

    public String createCanonicalRequest(final HttpRequest request, final Map<String, String> canonicalHeaders){
        final boolean isAmzDateHeaderPresent = s3AuthService.hasHeader(request, AmzHeaderNames.X_AMZ_DATE);
        final boolean isDateHeaderPresent = s3AuthService.hasHeader(request, HttpHeaderNames.DATE.toString());
        // Build string to sign
        final StringBuilder builder = new StringBuilder()
                .append(request.getMethod())
                .append('\n')
                .append(s3AuthService.findHeaderValue(request, HttpHeaderNames.CONTENT_MD5.toString()).orElse(""))
                .append('\n')
                .append(s3AuthService.findHeaderValue(request, HttpHeaderNames.CONTENT_TYPE.toString()).orElse(""))
                .append('\n');

        if(!(isAmzDateHeaderPresent && isDateHeaderPresent)){
            if(!canonicalHeaders.containsKey(AmzHeaderNames.X_AMZ_DATE)){
                builder.append(s3AuthService.findHeaderValue(request, HttpHeaderNames.DATE.toString()));
            }
        } else {
            if(!canonicalHeaders.containsKey(AmzHeaderNames.X_AMZ_DATE)){
                builder.append(s3AuthService.findHeaderValue(request, AmzHeaderNames.X_AMZ_DATE));
            } else {
                throw new InvalidSecurityException("Invalid date parameter");
            }
        }
        builder.append('\n');
        for (final Map.Entry<String, String> entry : canonicalHeaders.entrySet()) {
            builder.append(entry.getKey()).append(':').append(entry.getValue()).append('\n');
        }
        builder.append(request.getUri());
        return builder.toString();
    }
}
