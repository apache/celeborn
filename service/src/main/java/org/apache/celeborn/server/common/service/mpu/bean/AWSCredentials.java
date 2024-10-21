/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
