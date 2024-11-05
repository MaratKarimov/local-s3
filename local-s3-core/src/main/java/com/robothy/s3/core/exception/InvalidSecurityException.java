package com.robothy.s3.core.exception;

public class InvalidSecurityException extends LocalS3Exception {

  public InvalidSecurityException(final String message) {
    super(S3ErrorCode.InvalidSecurity, message);
  }
}
