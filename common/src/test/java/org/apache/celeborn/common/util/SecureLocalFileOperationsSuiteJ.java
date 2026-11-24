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

package org.apache.celeborn.common.util;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SecureDirectoryStream;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

public class SecureLocalFileOperationsSuiteJ {
  @Test
  public void rejectsAliasesInsideTheSameRoot() throws Exception {
    Path root = Files.createTempDirectory("secure-storage-");
    try {
      Path live = SecureLocalFileOperations.createFile(root, "live", 0, "data");
      Files.write(live, new byte[] {1, 2, 3});
      Files.createSymbolicLink(root.resolve("alias"), root.resolve("live"));
      assertThrows(
          IOException.class, () -> SecureLocalFileOperations.createFile(root, "alias", 1, "data"));
      assertThrows(
          IOException.class,
          () -> SecureLocalFileOperations.deleteDirectory(root, root.resolve("alias/0")));
      assertArrayEquals(new byte[] {1, 2, 3}, Files.readAllBytes(live));
      assertFalse(Files.exists(root.resolve("live/1")));
    } finally {
      JavaUtils.deleteRecursively(root.toFile());
    }
  }

  @Test
  public void protectsDataAndIndexChannelsAndIndividualDeletes() throws Exception {
    Path root = Files.createTempDirectory("secure-storage-");
    Path outside = Files.createTempDirectory("secure-storage-outside-");
    try {
      Path data = SecureLocalFileOperations.createFile(root, "app", 0, "data");
      Path canary = Files.write(outside.resolve("canary"), new byte[] {4, 5, 6});
      Files.createSymbolicLink(data.resolveSibling("data.index"), canary);
      assertThrows(
          IOException.class,
          () ->
              FileChannelUtils.createWritableFileChannel(
                  data.resolveSibling("data.index").toString()));
      Path old = root.resolve("old-app");
      Files.move(root.resolve("app"), old);
      Files.createDirectories(outside.resolve("0"));
      Files.createSymbolicLink(root.resolve("app"), outside);
      assertThrows(
          IOException.class, () -> FileChannelUtils.createWritableFileChannel(data.toString()));
      assertThrows(IOException.class, () -> SecureLocalFileOperations.deleteFile(data));
      assertArrayEquals(new byte[] {4, 5, 6}, Files.readAllBytes(canary));
      assertFalse(Files.exists(outside.resolve("0/data")));
    } finally {
      JavaUtils.deleteRecursively(root.toFile());
      JavaUtils.deleteRecursively(outside.toFile());
    }
  }

  @Test
  public void openChannelRemainsBoundWhenItsParentIsReplaced() throws Exception {
    Path root = Files.createTempDirectory("secure-storage-");
    Path outside = Files.createTempDirectory("secure-storage-outside-");
    try {
      Path data = SecureLocalFileOperations.createFile(root, "app", 0, "data");
      try (FileChannel channel = FileChannelUtils.createWritableFileChannel(data.toString())) {
        Files.move(root.resolve("app"), root.resolve("original"));
        Files.createSymbolicLink(root.resolve("app"), outside);
        channel.write(ByteBuffer.wrap(new byte[] {7, 8}));
      }
      assertArrayEquals(new byte[] {7, 8}, Files.readAllBytes(root.resolve("original/0/data")));
      assertFalse(Files.exists(outside.resolve("0")));
    } finally {
      JavaUtils.deleteRecursively(root.toFile());
      JavaUtils.deleteRecursively(outside.toFile());
    }
  }

  @Test
  public void recursiveDeleteUnlinksChildAliasesWithoutFollowingThem() throws Exception {
    Path root = Files.createTempDirectory("secure-storage-");
    Path outside = Files.createTempDirectory("secure-storage-outside-");
    try {
      Path data = SecureLocalFileOperations.createFile(root, "app", 0, "data");
      Path canary = Files.write(outside.resolve("canary"), new byte[] {1});
      Files.createSymbolicLink(data.resolveSibling("nested"), outside);
      SecureLocalFileOperations.deleteDirectory(root, root.resolve("app/0"));
      assertFalse(Files.exists(root.resolve("app/0")));
      assertArrayEquals(new byte[] {1}, Files.readAllBytes(canary));
    } finally {
      JavaUtils.deleteRecursively(root.toFile());
      JavaUtils.deleteRecursively(outside.toFile());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void recursiveDeleteContinuesAfterAChildDisappears() throws Exception {
    SecureDirectoryStream<Path> parent = mock(SecureDirectoryStream.class);
    SecureDirectoryStream<Path> child = mock(SecureDirectoryStream.class);
    Path name = Paths.get("shuffle");
    Path gone = Paths.get("gone");
    Path remaining = Paths.get("remaining");
    BasicFileAttributeView directoryView = mock(BasicFileAttributeView.class);
    BasicFileAttributes directoryAttributes = mock(BasicFileAttributes.class);
    Object identity = new Object();
    when(directoryAttributes.fileKey()).thenReturn(identity);
    when(directoryView.readAttributes()).thenReturn(directoryAttributes);
    when(parent.newDirectoryStream(name, LinkOption.NOFOLLOW_LINKS)).thenReturn(child);
    when(child.getFileAttributeView(BasicFileAttributeView.class)).thenReturn(directoryView);
    when(parent.getFileAttributeView(name, BasicFileAttributeView.class, LinkOption.NOFOLLOW_LINKS))
        .thenReturn(directoryView);
    when(child.iterator()).thenReturn(Arrays.asList(gone, remaining).iterator());
    BasicFileAttributeView goneView = mock(BasicFileAttributeView.class);
    when(child.getFileAttributeView(gone, BasicFileAttributeView.class, LinkOption.NOFOLLOW_LINKS))
        .thenReturn(goneView);
    when(goneView.readAttributes()).thenThrow(new NoSuchFileException(gone.toString()));
    BasicFileAttributeView remainingView = mock(BasicFileAttributeView.class);
    when(child.getFileAttributeView(
            remaining, BasicFileAttributeView.class, LinkOption.NOFOLLOW_LINKS))
        .thenReturn(remainingView);
    when(remainingView.readAttributes()).thenReturn(mock(BasicFileAttributes.class));

    SecureLocalFileOperations.deleteDirectory(parent, name);

    verify(child).deleteFile(remaining);
    verify(parent).deleteDirectory(name);
    verify(child).close();
  }

  @Test
  public void createdDirectoriesPreserveNormalMkdirPermissions() throws Exception {
    Path root = Files.createTempDirectory("secure-storage-permissions-");
    try {
      org.junit.Assume.assumeTrue(Files.getFileStore(root).supportsFileAttributeView("posix"));
      Path reference = Files.createDirectory(root.resolve("reference"));
      Path data = SecureLocalFileOperations.createFile(root, "app", 0, "data");
      assertEquals(
          Files.getPosixFilePermissions(reference),
          Files.getPosixFilePermissions(root.resolve("app")));
      assertEquals(
          Files.getPosixFilePermissions(reference),
          Files.getPosixFilePermissions(data.getParent()));
    } finally {
      JavaUtils.deleteRecursively(root.toFile());
    }
  }

  @Test
  public void concurrentCreationKeepsEveryFileInTheSameApplicationDirectory() throws Exception {
    Path root = Files.createTempDirectory("secure-concurrent-storage-");
    ExecutorService pool = Executors.newFixedThreadPool(8);
    try {
      List<Future<Path>> files = new ArrayList<>();
      for (int i = 0; i < 32; i++) {
        final String name = "data-" + i;
        files.add(pool.submit(() -> SecureLocalFileOperations.createFile(root, "app", 0, name)));
      }
      for (Future<Path> file : files)
        assertTrue(Files.isRegularFile(file.get(10, TimeUnit.SECONDS)));
    } finally {
      pool.shutdownNow();
      pool.awaitTermination(10, TimeUnit.SECONDS);
      JavaUtils.deleteRecursively(root.toFile());
    }
  }

  @Test
  public void propagatesLookupErrorsAndDoesNotTreatNonDirectoriesAsMissing() throws Exception {
    Path root = Files.createTempDirectory("secure-storage-");
    try {
      Files.createFile(root.resolve("app"));
      assertThrows(
          IOException.class, () -> SecureLocalFileOperations.createFile(root, "app", 0, "data"));
      assertThrows(
          IOException.class,
          () -> SecureLocalFileOperations.validateFile(root, root.resolve("app/0/data")));
      assertTrue(Files.isRegularFile(root.resolve("app")));
      Files.delete(root.resolve("app"));
      Files.createSymbolicLink(root.resolve("app"), root.resolve("missing-target"));
      assertThrows(
          IOException.class,
          () ->
              SecureLocalFileOperations.validatePathForDeletion(root, root.resolve("app/0/data")));
    } finally {
      JavaUtils.deleteRecursively(root.toFile());
    }
  }
}
