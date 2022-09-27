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

package com.aliyun.emr.rss.common.util;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.protobuf.InvalidProtocolBufferException;

import com.aliyun.emr.rss.common.meta.DiskInfo;
import com.aliyun.emr.rss.common.meta.FileInfo;
import com.aliyun.emr.rss.common.protocol.*;

public class PbSerDeUtils {
  public static Set<String> fromPbSortedShuffleFileSet(byte[] data)
      throws InvalidProtocolBufferException {
    PbSortedShuffleFileSet pbSortedShuffleFileSet = PbSortedShuffleFileSet.parseFrom(data);
    Set<String> files = ConcurrentHashMap.newKeySet();
    files.addAll(pbSortedShuffleFileSet.getFilesList());
    return files;
  }

  public static byte[] toPbSortedShuffleFileSet(Set<String> files) {
    PbSortedShuffleFileSet.Builder builder = PbSortedShuffleFileSet.newBuilder();
    builder.addAllFiles(files);
    return builder.build().toByteArray();
  }

  public static ArrayList<Integer> fromPbStoreVersion(byte[] data)
      throws InvalidProtocolBufferException {
    PbStoreVersion pbStoreVersion = PbStoreVersion.parseFrom(data);
    ArrayList<Integer> versions = new ArrayList<>();
    versions.add(pbStoreVersion.getMajor());
    versions.add(pbStoreVersion.getMinor());
    return versions;
  }

  public static byte[] toPbStoreVersion(int major, int minor) {
    PbStoreVersion.Builder builder = PbStoreVersion.newBuilder();
    builder.setMajor(major);
    builder.setMinor(minor);
    return builder.build().toByteArray();
  }

  public static DiskInfo fromPbDiskInfo(PbDiskInfo pbDiskInfo) {
    // TODO disk status
    return new DiskInfo(
        pbDiskInfo.getMountPoint(),
        pbDiskInfo.getUsableSpace(),
        pbDiskInfo.getAvgFlushTime(),
        pbDiskInfo.getUsedSlots());
  }

  public static PbDiskInfo toPbDiskInfo(DiskInfo diskInfo) {
    // TODO disk status
    return PbDiskInfo.newBuilder()
        .setMountPoint(diskInfo.mountPoint())
        .setUsableSpace(diskInfo.actualUsableSpace())
        .setAvgFlushTime(diskInfo.avgFlushTime())
        .setUsedSlots(diskInfo.activeSlots())
        .build();
  }

  public static FileInfo fromPbFileInfo(PbFileInfo pbFileInfo)
      throws InvalidProtocolBufferException {
    return new FileInfo(
        pbFileInfo.getFilePath(), new ArrayList<>(pbFileInfo.getChunkOffsetsList()));
  }

  public static PbFileInfo toPbFileInfo(FileInfo fileInfo) {
    PbFileInfo.Builder builder = PbFileInfo.newBuilder();
    builder.setFilePath(fileInfo.getFilePath()).addAllChunkOffsets(fileInfo.getChunkOffsets());
    return builder.build();
  }

  public static ConcurrentHashMap<String, FileInfo> fromPbFileInfoMap(byte[] data)
      throws InvalidProtocolBufferException {
    PbFileInfoMap pbFileInfoMap = PbFileInfoMap.parseFrom(data);
    ConcurrentHashMap<String, FileInfo> fileInfoMap = new ConcurrentHashMap<>();
    for (Map.Entry<String, PbFileInfo> entry : pbFileInfoMap.getValuesMap().entrySet()) {
      fileInfoMap.put(entry.getKey(), fromPbFileInfo(entry.getValue()));
    }
    return fileInfoMap;
  }

  public static byte[] toPbFileInfoMap(ConcurrentHashMap<String, FileInfo> fileInfoMap) {
    PbFileInfoMap.Builder builder = PbFileInfoMap.newBuilder();
    ConcurrentHashMap<String, PbFileInfo> pbFileInfoMap = new ConcurrentHashMap<>();
    for (Map.Entry<String, FileInfo> entry : fileInfoMap.entrySet()) {
      pbFileInfoMap.put(entry.getKey(), toPbFileInfo(entry.getValue()));
    }
    builder.putAllValues(pbFileInfoMap);
    return builder.build().toByteArray();
  }
}
