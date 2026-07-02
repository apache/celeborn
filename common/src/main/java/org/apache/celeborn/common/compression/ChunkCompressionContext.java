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

package org.apache.celeborn.common.compression;

/**
 * Carries chunk-level compression settings from the client through to the worker's {@code
 * ChunkCompressedFileChannelWriter}. Using a context object instead of a bare boolean keeps the
 * call chain stable as new compression knobs are added.
 */
public final class ChunkCompressionContext {

  /** ZSTD default compression level (mirrors {@code Zstd.defaultCompressionLevel()}). */
  public static final int DEFAULT_COMPRESSION_LEVEL = 3;

  private static final ChunkCompressionContext DISABLED =
      new ChunkCompressionContext(false, DEFAULT_COMPRESSION_LEVEL);

  private final boolean enabled;
  private final int compressionLevel;

  public ChunkCompressionContext(boolean enabled, int compressionLevel) {
    this.enabled = enabled;
    this.compressionLevel = compressionLevel;
  }

  /** Returns a context with compression disabled and the default compression level. */
  public static ChunkCompressionContext disabled() {
    return DISABLED;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public int getCompressionLevel() {
    return compressionLevel;
  }
}
