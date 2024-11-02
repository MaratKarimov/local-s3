package com.robothy.s3.rest.security;

import com.google.common.base.Joiner;
import com.google.common.net.PercentEscaper;
import com.robothy.netty.http.HttpRequest;
import com.robothy.s3.core.exception.InvalidSecurityException;
import com.robothy.s3.rest.constants.AmzHeaderNames;
import com.robothy.s3.rest.model.security.S3AuthHeader;
import lombok.RequiredArgsConstructor;
import org.apache.commons.codec.binary.Base16;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.regex.Pattern;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.hash.Hashing.sha256;
import static java.util.stream.Collectors.toCollection;

@RequiredArgsConstructor
public class AWSSignatureV4Service {

    private final S3AuthService s3AuthService;

    private static final Pattern REPEATING_WHITESPACE = Pattern.compile("\\s+");

    private static final PercentEscaper AWS_URL_PARAMETER_ESCAPER = new PercentEscaper("-_.~", false);

    /**
     * Reference: https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv.html
     */
    public String createAuthorizationSignature(
            final HttpRequest request,
            final S3AuthHeader s3AuthHeader,
            final String contentDigest,
            final String secretKey
    ) throws NoSuchAlgorithmException, InvalidKeyException {
        final String algorithm = s3AuthHeader.getHmacAlgorithm();
        final byte[] dateKey = signMessage(s3AuthHeader.getDate().getBytes(StandardCharsets.UTF_8), ("AWS4" + secretKey).getBytes(StandardCharsets.UTF_8), algorithm);
        final byte[] dateRegionKey = signMessage(s3AuthHeader.getRegion().getBytes(StandardCharsets.UTF_8), dateKey, algorithm);
        final byte[] dateRegionServiceKey = signMessage(s3AuthHeader.getService().getBytes(StandardCharsets.UTF_8), dateRegionKey, algorithm);
        final byte[] signingKey = signMessage("aws4_request".getBytes(StandardCharsets.UTF_8), dateRegionServiceKey, algorithm);

        final String date = s3AuthService.findHeaderValue(request, AmzHeaderNames.X_AMZ_DATE).orElseThrow(() -> new InvalidSecurityException("Absent date header"));
        final String canonicalRequest = createCanonicalRequest(request, s3AuthHeader, contentDigest);
        final String hashedCanonicalRequest = sha256().hashString(canonicalRequest, UTF_8).toString();
        final String signatureString = "AWS4-HMAC-SHA256\n" +
                date + "\n" +
                s3AuthHeader.getDate() + "/" + s3AuthHeader.getRegion() +
                "/s3/aws4_request\n" +
                hashedCanonicalRequest;
        final byte[] signature = signMessage(signatureString.getBytes(StandardCharsets.UTF_8), signingKey, algorithm);
        return (new Base16(true)).encodeToString(signature);
    }

    private byte[] signMessage(final byte[] data, final byte[] key, final String algorithm) throws InvalidKeyException, NoSuchAlgorithmException {
        final Mac mac = Mac.getInstance(algorithm);
        mac.init(new SecretKeySpec(key, algorithm));
        return mac.doFinal(data);
    }

    private String createCanonicalRequest(final HttpRequest request, final S3AuthHeader s3AuthHeader, final String contentDigest){
        return Joiner
                .on("\n")
                .join(
                        request.getMethod(),
                        request.getPath(),
                        buildCanonicalQueryString(request),
                        buildCanonicalHeaders(request, s3AuthHeader.getSignedHeaders()) + "\n",
                        Joiner.on(';').join(s3AuthHeader.getSignedHeaders()),
                        contentDigest
                );
    }

    private String buildCanonicalQueryString(final HttpRequest request){
        // The parameters are required to be sorted
        final List<String> parameters = request.getParams().keySet().stream().map(CharSequence::toString).collect(toCollection(ArrayList::new));
        Collections.sort(parameters);

        final List<String> queryParameters = new ArrayList<>();

        for (final String key : parameters) {
            if (key.equals("X-Amz-Signature") || key.equals("bucket")) {
                continue;
            }
            // re-encode keys and values in AWS normalized form
            request.parameter(key)
                    .ifPresent(s -> queryParameters.add(AWS_URL_PARAMETER_ESCAPER.escape(key) + "=" + AWS_URL_PARAMETER_ESCAPER.escape(s)));
        }
        return Joiner.on("&").join(queryParameters);
    }

    private String buildCanonicalHeaders(final HttpRequest request, final List<String> signedHeaders) {
        final List<String> headers = new ArrayList<>(signedHeaders.size());
        for (final String header : signedHeaders) {
            headers.add(header.toLowerCase());
        }

        Collections.sort(headers);

        final StringBuilder headersWithValues = new StringBuilder();

        boolean firstHeader = true;
        for (final String header : headers) {
            if (firstHeader) {
                firstHeader = false;
            } else {
                headersWithValues.append('\n');
            }
            headersWithValues.append(header);
            headersWithValues.append(':');

            String value = s3AuthService.findHeaderValue(request, header).orElse("");

            value = value.trim();
            if (!value.startsWith("\"")) {
                value = REPEATING_WHITESPACE.matcher(value).replaceAll(" ");
            }
            headersWithValues.append(value);
        }
        return headersWithValues.toString();
    }
}
