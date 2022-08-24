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

package com.aliyun.emr.rss.common.utils;

import java.io.File;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.protobuf.InvalidProtocolBufferException;

import com.aliyun.emr.rss.common.network.server.FileInfo;
import com.aliyun.emr.rss.common.protocol.TransportMessages;

public class PBSerDeUtils {
  public static Set<String> fromPbSortedShuffleFileSet(byte[] data)
      throws InvalidProtocolBufferException {
    TransportMessages.PbSortedShuffleFileSet pbSortedShuffleFileSet =
        TransportMessages.PbSortedShuffleFileSet.parseFrom(data);
    Set<String> files = ConcurrentHashMap.newKeySet();
    files.addAll(pbSortedShuffleFileSet.getFilesList());
    return files;
  }

  public static byte[] toPbSortedShuffleFileSet(
      Set<String> files) {
    TransportMessages.PbSortedShuffleFileSet.Builder builder =
        TransportMessages.PbSortedShuffleFileSet.newBuilder();
    builder.addAllFiles(files);
    return builder.build().toByteArray();
  }

  public static ArrayList<Integer> fromPbStoreVersion(byte[] data)
      throws InvalidProtocolBufferException {
    TransportMessages.PbStoreVersion pbStoreVersion =
        TransportMessages.PbStoreVersion.parseFrom(data);
    ArrayList<Integer> versions = new ArrayList<>();
    versions.add(pbStoreVersion.getMajor());
    versions.add(pbStoreVersion.getMinor());
    return versions;
  }

  public static byte[] toPbStoreVersion(int major, int minor) {
    TransportMessages.PbStoreVersion.Builder builder =
        TransportMessages.PbStoreVersion.newBuilder();
    builder.setMajor(major);
    builder.setMinor(minor);
    return builder.build().toByteArray();
  }

  public static FileInfo fromPbFileInfo(TransportMessages.PbFileInfo pbFileInfo)
      throws InvalidProtocolBufferException {
    return new FileInfo(pbFileInfo.getFilePath(),
        new ArrayList<>(pbFileInfo.getChunkOffsetsList()));
  }

  public static TransportMessages.PbFileInfo toPbFileInfo(FileInfo fileInfo) {
    TransportMessages.PbFileInfo.Builder builder = TransportMessages.PbFileInfo.newBuilder();
    builder.setFilePath(fileInfo.filePath)
        .addAllChunkOffsets(fileInfo.getChunkOffsets());
    return builder.build();
  }

  public static ConcurrentHashMap<String, FileInfo> fromPbFileInfoMap(byte[] data)
      throws InvalidProtocolBufferException {
    TransportMessages.PBFileInfoMap pbFileInfoMap = TransportMessages.PBFileInfoMap.parseFrom(data);
    ConcurrentHashMap<String, FileInfo> fileInfoMap = new ConcurrentHashMap<>();
    for (Map.Entry<String, TransportMessages.PbFileInfo> entry :
        pbFileInfoMap.getValuesMap().entrySet()) {
      fileInfoMap.put(entry.getKey(), fromPbFileInfo(entry.getValue()));
    }
    return fileInfoMap;
  }

  public static byte[] toPbFileInfoMap(ConcurrentHashMap<String, FileInfo> fileInfoMap) {
    TransportMessages.PBFileInfoMap.Builder builder = TransportMessages.PBFileInfoMap.newBuilder();
    ConcurrentHashMap<String, TransportMessages.PbFileInfo> pbFileInfoMap =
        new ConcurrentHashMap<>();
    for (Map.Entry<String, FileInfo> entry : fileInfoMap.entrySet()) {
      pbFileInfoMap.put(entry.getKey(), toPbFileInfo(entry.getValue()));
    }
    builder.putAllValues(pbFileInfoMap);
    return builder.build().toByteArray();
  }
}
