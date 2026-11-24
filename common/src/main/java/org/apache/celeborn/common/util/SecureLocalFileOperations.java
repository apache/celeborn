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

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.SecureDirectoryStream;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/** Local storage operations relative to open directories, without following child aliases. */
public final class SecureLocalFileOperations {
  // Only configured storage roots enter this map; reads and deletes never take these locks.
  private static final ConcurrentHashMap<Path, Object> DIRECTORY_LOCKS = new ConcurrentHashMap<>();

  public static final class InvalidStoragePathException extends IOException {
    public InvalidStoragePathException(String message) {
      super(message);
    }
  }

  private SecureLocalFileOperations() {}

  private static SecureDirectoryStream<Path> secure(DirectoryStream<Path> stream)
      throws IOException {
    if (!(stream instanceof SecureDirectoryStream)) {
      stream.close();
      throw new IOException("Local shuffle storage requires secure directory-stream support");
    }
    return (SecureDirectoryStream<Path>) stream;
  }

  private static void component(String name) throws IOException {
    if (name.isEmpty()
        || name.equals(".")
        || name.equals("..")
        || name.indexOf('/') >= 0
        || name.indexOf('\\') >= 0) {
      throw new IOException("Expected a single local storage path component");
    }
  }

  private static final class Directory implements AutoCloseable {
    private final Path rootPath;
    private final SecureDirectoryStream<Path> root;
    private final List<SecureDirectoryStream<Path>> opened = new ArrayList<>();
    private SecureDirectoryStream<Path> current;

    private Directory(Path workingRoot) throws IOException {
      // The configured root is trusted and may itself be a configured mount alias.
      // Application/shuffle components below it are always opened with NOFOLLOW_LINKS.
      rootPath = workingRoot.toRealPath();
      root = secure(Files.newDirectoryStream(rootPath));
      current = root;
      opened.add(root);
    }

    private void descend(String name, boolean create) throws IOException {
      component(name);
      if (create) {
        // Serialize directory creation only within one configured root. A stalled mount
        // must not block traversal or operations on another storage root.
        synchronized (DIRECTORY_LOCKS.computeIfAbsent(rootPath, ignored -> new Object())) {
          descendChild(name, true);
        }
      } else {
        descendChild(name, false);
      }
    }

    private void descendChild(String name, boolean create) throws IOException {
      Path child = rootPath.getFileSystem().getPath(name);
      SecureDirectoryStream<Path> next;
      try {
        BasicFileAttributes attributes =
            current
                .getFileAttributeView(
                    child, BasicFileAttributeView.class, LinkOption.NOFOLLOW_LINKS)
                .readAttributes();
        if (!attributes.isDirectory()) {
          throw new InvalidStoragePathException(
              "Application and shuffle directories must not be aliases");
        }
        next = current.newDirectoryStream(child, LinkOption.NOFOLLOW_LINKS);
      } catch (NoSuchFileException missing) {
        if (!create) {
          throw missing;
        }
        // SecureDirectoryStream has no mkdirat operation. Stage a new empty directory
        // directly in the trusted root, then move it relative to the pinned handles.
        // No application-controlled parent is used by the pathname-based mkdir.
        // Preserve the normal mkdir permissions rather than createTempDirectory's
        // owner-only default, which would prevent shared local readers from traversing it.
        Path staged =
            Files.createDirectory(rootPath.resolve(".celeborn-directory-" + UUID.randomUUID()));
        try {
          try {
            root.move(staged.getFileName(), current, child);
          } catch (FileAlreadyExistsException | DirectoryNotEmptyException concurrentCreation) {
            // Another writer created this application's directory first.
          }
        } finally {
          try {
            root.deleteDirectory(staged.getFileName());
          } catch (NoSuchFileException moved) {
            // The staged directory was moved successfully.
          }
        }
        next = current.newDirectoryStream(child, LinkOption.NOFOLLOW_LINKS);
      }
      opened.add(next);
      current = next;
    }

    @Override
    public void close() throws IOException {
      IOException failure = null;
      for (int i = opened.size() - 1; i >= 0; i--) {
        try {
          opened.get(i).close();
        } catch (IOException e) {
          if (failure == null) failure = e;
          else failure.addSuppressed(e);
        }
      }
      if (failure != null) throw failure;
    }
  }

  public static Path createFile(Path workingRoot, String appId, int shuffleId, String fileName)
      throws IOException {
    component(appId);
    component(fileName);
    try (Directory directory = new Directory(workingRoot)) {
      directory.descend(appId, true);
      directory.descend(Integer.toString(shuffleId), true);
      Set<OpenOption> options =
          new HashSet<>(
              Arrays.asList(
                  StandardOpenOption.CREATE_NEW,
                  StandardOpenOption.WRITE,
                  LinkOption.NOFOLLOW_LINKS));
      try (SeekableByteChannel ignored =
          directory.current.newByteChannel(
              workingRoot.getFileSystem().getPath(fileName), options)) {
        return directory
            .rootPath
            .resolve(appId)
            .resolve(Integer.toString(shuffleId))
            .resolve(fileName);
      }
    }
  }

  private static Path relative(Path root, Path file) throws IOException {
    Path absoluteRoot = root.toAbsolutePath().normalize();
    Path absolute = file.toAbsolutePath().normalize();
    if (!absolute.startsWith(absoluteRoot) || absolute.equals(absoluteRoot)) {
      throw new IOException("Shuffle file must remain within its configured storage root");
    }
    return absoluteRoot.relativize(absolute);
  }

  private static void parents(Directory directory, Path relative) throws IOException {
    for (int i = 0; i < relative.getNameCount() - 1; i++) {
      directory.descend(relative.getName(i).toString(), false);
    }
  }

  public static void validateFile(Path root, Path file) throws IOException {
    validateFile(root, file, false);
  }

  public static void validatePathForDeletion(Path root, Path file) throws IOException {
    validateFile(root, file, true);
  }

  private static void validateFile(Path root, Path file, boolean allowMissing) throws IOException {
    Path relative = relative(root, file);
    try (Directory directory = new Directory(root)) {
      parents(directory, relative);
      BasicFileAttributes attributes =
          directory
              .current
              .getFileAttributeView(
                  relative.getFileName(), BasicFileAttributeView.class, LinkOption.NOFOLLOW_LINKS)
              .readAttributes();
      if (!attributes.isRegularFile()) {
        throw new IOException("Shuffle path must identify a regular file");
      }
    } catch (NoSuchFileException missing) {
      if (!allowMissing) throw missing;
    }
  }

  public static FileChannel openFileChannel(Path file, OpenOption... requested) throws IOException {
    Path absolute = file.toAbsolutePath().normalize();
    try (Directory directory = new Directory(absolute.getRoot())) {
      parents(directory, absolute.getRoot().relativize(absolute));
      Set<OpenOption> options = new HashSet<>(Arrays.asList(requested));
      options.add(LinkOption.NOFOLLOW_LINKS);
      SeekableByteChannel channel =
          directory.current.newByteChannel(absolute.getFileName(), options);
      if (channel instanceof FileChannel) return (FileChannel) channel;
      channel.close();
      throw new IOException("Local shuffle storage requires file-channel support");
    }
  }

  public static void deleteFile(Path file) throws IOException {
    Path absolute = file.toAbsolutePath().normalize();
    try (Directory directory = new Directory(absolute.getRoot())) {
      parents(directory, absolute.getRoot().relativize(absolute));
      directory.current.deleteFile(absolute.getFileName());
    } catch (NoSuchFileException missing) {
      // Already removed.
    }
  }

  public static void deleteDirectory(Path workingRoot, Path target) throws IOException {
    Path relative = relative(workingRoot, target);
    try (Directory directory = new Directory(workingRoot)) {
      parents(directory, relative);
      deleteDirectory(directory.current, relative.getFileName());
    } catch (NoSuchFileException missing) {
      // Already removed, including a missing application parent.
    }
  }

  static void deleteDirectory(SecureDirectoryStream<Path> parent, Path name) throws IOException {
    try (SecureDirectoryStream<Path> child =
        parent.newDirectoryStream(name, LinkOption.NOFOLLOW_LINKS)) {
      Object identity =
          child.getFileAttributeView(BasicFileAttributeView.class).readAttributes().fileKey();
      for (Path entry : child) {
        Path entryName = entry.getFileName();
        try {
          BasicFileAttributes attributes =
              child
                  .getFileAttributeView(
                      entryName, BasicFileAttributeView.class, LinkOption.NOFOLLOW_LINKS)
                  .readAttributes();
          if (attributes.isDirectory()) deleteDirectory(child, entryName);
          else child.deleteFile(entryName);
        } catch (NoSuchFileException removed) {
          // A concurrent deletion of one child must not skip the remaining entries.
        }
      }
      BasicFileAttributes current =
          parent
              .getFileAttributeView(name, BasicFileAttributeView.class, LinkOption.NOFOLLOW_LINKS)
              .readAttributes();
      if (identity == null || !identity.equals(current.fileKey())) {
        throw new IOException("Shuffle directory changed while it was being deleted");
      }
      parent.deleteDirectory(name);
    }
  }
}
