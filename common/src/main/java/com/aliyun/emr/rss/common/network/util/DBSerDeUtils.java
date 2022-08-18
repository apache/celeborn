package com.aliyun.emr.rss.common.network.util;

import com.aliyun.emr.rss.common.protocol.TransportMessages;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class DBSerDeUtils {
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
}
