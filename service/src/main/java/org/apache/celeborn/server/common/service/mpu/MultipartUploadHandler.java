package org.apache.celeborn.server.common.service.mpu;

import java.io.InputStream;

public interface MultipartUploadHandler {

  void startUpload();

  void putPart(InputStream inputStream, Long lengthInBytes, Integer partNumber);

  void complete();

  void abort();
}
