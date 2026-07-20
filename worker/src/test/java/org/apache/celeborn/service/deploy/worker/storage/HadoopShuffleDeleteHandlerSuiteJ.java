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

package org.apache.celeborn.service.deploy.worker.storage;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HadoopShuffleDeleteHandlerSuiteJ {

  private File tempDir;
  private Configuration hadoopConf;
  private HadoopShuffleDeleteHandler handler;

  @Before
  public void setUp() throws IOException {
    tempDir = Files.createTempDirectory("celeborn-delete-test").toFile();
    hadoopConf = new Configuration();
    hadoopConf.set("fs.defaultFS", "file:///");
    // Use null shuffleServerId to trigger deleteAll (recursive full delete)
    handler = new HadoopShuffleDeleteHandler(hadoopConf, null);
  }

  @After
  public void tearDown() {
    deleteRecursively(tempDir);
  }

  @Test
  public void testDeleteSingleFileWithNullPrefix() throws Exception {
    Path testFile = new Path(tempDir.getAbsolutePath(), "test.file");
    LocalFileSystem fs = FileSystem.getLocal(hadoopConf);
    fs.create(testFile).close();
    assertTrue("File should exist before delete", fs.exists(testFile));

    handler.delete(new String[] {testFile.toString()}, "appId", "user");

    assertFalse("File should be deleted", fs.exists(testFile));
  }

  @Test
  public void testDeleteDirectoryRecursively() throws Exception {
    Path root = new Path(tempDir.getAbsolutePath(), "root");
    Path subDir = new Path(root, "sub1/sub2");
    LocalFileSystem fs = FileSystem.getLocal(hadoopConf);
    fs.mkdirs(subDir);
    fs.create(new Path(subDir, "data.file")).close();
    fs.create(new Path(root, "root.file")).close();
    assertTrue("Directory should exist before delete", fs.exists(root));

    handler.delete(new String[] {root.toString()}, "appId", "user");

    assertFalse("Root directory should be deleted", fs.exists(root));
  }

  @Test
  public void testDeleteNonExistentPath() throws Exception {
    Path nonExistent = new Path(tempDir.getAbsolutePath(), "does-not-exist");

    handler.delete(new String[] {nonExistent.toString()}, "appId", "user");

    // Should not throw exception, just log and skip
    assertFalse(FileSystem.getLocal(hadoopConf).exists(nonExistent));
  }

  @Test
  public void testDeleteWithPrefixFilter() throws Exception {
    // Test with a real shuffleServerId as prefix — only files starting with this
    // prefix should be deleted
    Path root = new Path(tempDir.getAbsolutePath(), "prefixed");
    LocalFileSystem fs = FileSystem.getLocal(hadoopConf);
    fs.mkdirs(root);
    // Create files with different prefixes
    fs.create(new Path(root, "worker-1-data")).close();
    fs.create(new Path(root, "worker-1-index")).close();
    fs.create(new Path(root, "worker-2-data")).close();
    fs.create(new Path(root, "other-file")).close();

    HadoopShuffleDeleteHandler prefixedHandler =
        new HadoopShuffleDeleteHandler(hadoopConf, "worker-1");

    prefixedHandler.delete(new String[] {root.toString()}, "appId", "user");

    // "worker-1" prefixed files should be deleted
    assertFalse("worker-1-data should be deleted", fs.exists(new Path(root, "worker-1-data")));
    assertFalse("worker-1-index should be deleted", fs.exists(new Path(root, "worker-1-index")));
    // "worker-2" and "other-file" should remain
    assertTrue("worker-2-data should remain", fs.exists(new Path(root, "worker-2-data")));
    assertTrue("other-file should remain", fs.exists(new Path(root, "other-file")));
  }

  private void deleteRecursively(File file) {
    if (file.isDirectory()) {
      File[] children = file.listFiles();
      if (children != null) {
        for (File child : children) {
          deleteRecursively(child);
        }
      }
    }
    file.delete();
  }
}
