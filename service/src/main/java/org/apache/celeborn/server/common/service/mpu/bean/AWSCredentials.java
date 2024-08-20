package org.apache.celeborn.server.common.service.mpu.bean;

public class AWSCredentials {

  private String bucketName;

  private String s3AccessKey;

  private String s3SecretKey;

  private String s3EndpointRegion;

  public AWSCredentials(
      String bucketName, String s3AccessKey, String s3SecretKey, String s3EndpointRegion) {
    this.bucketName = bucketName;
    this.s3AccessKey = s3AccessKey;
    this.s3SecretKey = s3SecretKey;
    this.s3EndpointRegion = s3EndpointRegion;
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

  public void setBucketName(String bucketName) {
    this.bucketName = bucketName;
  }

  public void setS3AccessKey(String s3AccessKey) {
    this.s3AccessKey = s3AccessKey;
  }

  public void setS3SecretKey(String s3SecretKey) {
    this.s3SecretKey = s3SecretKey;
  }

  public void setS3EndpointRegion(String s3EndpointRegion) {
    this.s3EndpointRegion = s3EndpointRegion;
  }

  @Override
  public String toString() {
    return "AWSCredentials{"
        + "bucketName='"
        + bucketName
        + '\''
        + ", s3AccessKey='"
        + s3AccessKey
        + '\''
        + ", s3SecretKey='"
        + s3SecretKey
        + '\''
        + ", s3EndpointRegion='"
        + s3EndpointRegion
        + '\''
        + '}';
  }
}
