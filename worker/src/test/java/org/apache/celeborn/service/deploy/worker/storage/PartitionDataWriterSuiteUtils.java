package org.apache.celeborn.service.deploy.worker.storage;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import scala.Tuple3;

import org.mockito.Mockito;

import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.meta.FileInfo;
import org.apache.celeborn.common.meta.NonMemoryFileInfo;

public class PartitionDataWriterSuiteUtils {
  public static File getTemporaryFile(File tempDir) throws IOException {
    String filename = UUID.randomUUID().toString();
    File temporaryFile = new File(tempDir, filename);
    temporaryFile.createNewFile();
    return temporaryFile;
  }

  public static Tuple3<StorageManager, CreateFileContext, FileInfo> prepareTestFileContext(
      File tempDir, UserIdentifier userIdentifier, Flusher flusher, boolean reduceMeta)
      throws IOException {
    File file = getTemporaryFile(tempDir);
    FileInfo fileInfo = new NonMemoryFileInfo(file, userIdentifier);
    if (!reduceMeta) {
      //      fileInfo.replaceFileMeta(new MapFileMeta());
    }

    StorageManager storageManager = Mockito.mock(StorageManager.class);
    CreateFileContext createFileContext = Mockito.mock(CreateFileContext.class);
    Mockito.doAnswer(
            invocation -> new CreateFileResult(file.getAbsolutePath(), null, flusher, fileInfo))
        .when(storageManager)
        .createFile(Mockito.any());
    return new Tuple3<>(storageManager, createFileContext, fileInfo);
  }
}
