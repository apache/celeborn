package org.apache.celeborn;

public class AWSCredentials {

  private final String bucketName;

  private final String s3AccessKey;

  private final String s3SecretKey;

  private final String s3EndpointRegion;

  private AWSCredentials(Builder builder) {
    this.bucketName = builder.bucketName;
    this.s3AccessKey = builder.s3AccessKey;
    this.s3SecretKey = builder.s3SecretKey;
    this.s3EndpointRegion = builder.s3EndpointRegion;
  }

  public String getBucketName() {
    return bucketName;
  }

  public String getS3AccessKey() {
    return s3AccessKey;
  }

  public String getS3SecretKey() {
    return s3SecretKey;
  }

  public String getS3EndpointRegion() {
    return s3EndpointRegion;
  }

  public static class Builder {

    private String bucketName;

    private String s3AccessKey;

    private String s3SecretKey;

    private String s3EndpointRegion;

    public AWSCredentials.Builder withBucketName(String bucketName) {
      this.bucketName = bucketName;
      return this;
    }

    public AWSCredentials.Builder withS3AccessKey(String s3AccessKey) {
      this.s3AccessKey = s3AccessKey;
      return this;
    }

    public AWSCredentials.Builder withS3SecretKey(String s3SecretKey) {
      this.s3SecretKey = s3SecretKey;
      return this;
    }

    public AWSCredentials.Builder withS3EndpointRegion(String s3EndpointRegion) {
      this.s3EndpointRegion = s3EndpointRegion;
      return this;
    }

    public AWSCredentials build() {
      return new AWSCredentials(this);
    }
  }
}
