/// *
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
// package org.apache.celeborn.common.util;
//
// import java.util.*;
// import java.util.concurrent.ConcurrentHashMap;
//
// import com.google.protobuf.InvalidProtocolBufferException;
//
// import org.apache.celeborn.common.identity.UserIdentifier;
// import org.apache.celeborn.common.identity.UserIdentifier$;
// import org.apache.celeborn.common.meta.DiskInfo;
// import org.apache.celeborn.common.meta.FileInfo;
// import org.apache.celeborn.common.meta.WorkerInfo;
// import org.apache.celeborn.common.protocol.*;
// import org.apache.celeborn.common.protocol.PartitionLocation.Mode;
// import org.apache.celeborn.common.quota.ResourceConsumption;
//
// public class PbSerDeUtils {
//  public static Set<String> fromPbSortedShuffleFileSet(byte[] data)
//      throws InvalidProtocolBufferException {
//    PbSortedShuffleFileSet pbSortedShuffleFileSet = PbSortedShuffleFileSet.parseFrom(data);
//    Set<String> files = ConcurrentHashMap.newKeySet();
//    files.addAll(pbSortedShuffleFileSet.getFilesList());
//    return files;
//  }
//
//  public static byte[] toPbSortedShuffleFileSet(Set<String> files) {
//    PbSortedShuffleFileSet.Builder builder = PbSortedShuffleFileSet.newBuilder();
//    builder.addAllFiles(files);
//    return builder.build().toByteArray();
//  }
//
//  public static ArrayList<Integer> fromPbStoreVersion(byte[] data)
//      throws InvalidProtocolBufferException {
//    PbStoreVersion pbStoreVersion = PbStoreVersion.parseFrom(data);
//    ArrayList<Integer> versions = new ArrayList<>();
//    versions.add(pbStoreVersion.getMajor());
//    versions.add(pbStoreVersion.getMinor());
//    return versions;
//  }
//
//  public static byte[] toPbStoreVersion(int major, int minor) {
//    PbStoreVersion.Builder builder = PbStoreVersion.newBuilder();
//    builder.setMajor(major);
//    builder.setMinor(minor);
//    return builder.build().toByteArray();
//  }
//
//  public static DiskInfo fromPbDiskInfo(PbDiskInfo pbDiskInfo) {
//    return new DiskInfo(
//            pbDiskInfo.getMountPoint(),
//            pbDiskInfo.getUsableSpace(),
//            pbDiskInfo.getAvgFlushTime(),
//            pbDiskInfo.getUsedSlots())
//        .setStatus(Utils.toDiskStatus(pbDiskInfo.getStatus()));
//  }
//
//  public static PbDiskInfo toPbDiskInfo(DiskInfo diskInfo) {
//    return PbDiskInfo.newBuilder()
//        .setMountPoint(diskInfo.mountPoint())
//        .setUsableSpace(diskInfo.actualUsableSpace())
//        .setAvgFlushTime(diskInfo.avgFlushTime())
//        .setUsedSlots(diskInfo.activeSlots())
//        .setStatus(diskInfo.status().getValue())
//        .build();
//  }
//
//  public static FileInfo fromPbFileInfo(PbFileInfo pbFileInfo)
//      throws InvalidProtocolBufferException {
//    PbUserIdentifier pbUserIdentifier = pbFileInfo.getUserIdentifier();
//    UserIdentifier userIdentifier =
//        new UserIdentifier(pbUserIdentifier.getTenantId(), pbUserIdentifier.getName());
//    return new FileInfo(
//        pbFileInfo.getFilePath(),
//        new ArrayList<>(pbFileInfo.getChunkOffsetsList()),
//        userIdentifier);
//  }
//
//  public static FileInfo fromPbFileInfo(PbFileInfo pbFileInfo, UserIdentifier userIdentifier) {
//    return new FileInfo(
//        pbFileInfo.getFilePath(),
//        new ArrayList<>(pbFileInfo.getChunkOffsetsList()),
//        userIdentifier);
//  }
//
//  public static PbFileInfo toPbFileInfo(FileInfo fileInfo) {
//    PbFileInfo.Builder builder = PbFileInfo.newBuilder();
//    builder.setFilePath(fileInfo.getFilePath()).addAllChunkOffsets(fileInfo.getChunkOffsets());
//    return builder.build();
//  }
//
//  public static ConcurrentHashMap<String, FileInfo> fromPbFileInfoMap(
//      byte[] data, ConcurrentHashMap<String, UserIdentifier> cache)
//      throws InvalidProtocolBufferException {
//    PbFileInfoMap pbFileInfoMap = PbFileInfoMap.parseFrom(data);
//    ConcurrentHashMap<String, FileInfo> fileInfoMap = new ConcurrentHashMap<>();
//    for (Map.Entry<String, PbFileInfo> entry : pbFileInfoMap.getValuesMap().entrySet()) {
//      FileInfo fileInfo;
//      PbUserIdentifier pbUserIdentifier = entry.getValue().getUserIdentifier();
//      String userIdentifierKey = pbUserIdentifier.getTenantId() + "-" +
// pbUserIdentifier.getName();
//      if (!cache.containsKey(userIdentifierKey)) {
//        fileInfo = fromPbFileInfo(entry.getValue());
//        cache.put(userIdentifierKey, fileInfo.getUserIdentifier());
//      } else {
//        fileInfo = fromPbFileInfo(entry.getValue(), cache.get(userIdentifierKey));
//      }
//      fileInfoMap.put(entry.getKey(), fileInfo);
//    }
//    return fileInfoMap;
//  }
//
//  public static byte[] toPbFileInfoMap(ConcurrentHashMap<String, FileInfo> fileInfoMap) {
//    PbFileInfoMap.Builder builder = PbFileInfoMap.newBuilder();
//    ConcurrentHashMap<String, PbFileInfo> pbFileInfoMap = new ConcurrentHashMap<>();
//    for (Map.Entry<String, FileInfo> entry : fileInfoMap.entrySet()) {
//      pbFileInfoMap.put(entry.getKey(), toPbFileInfo(entry.getValue()));
//    }
//    builder.putAllValues(pbFileInfoMap);
//    return builder.build().toByteArray();
//  }
//
//  public static ResourceConsumption fromPbResourceConsumption(
//      PbResourceConsumption pbResourceConsumption) {
//    return new ResourceConsumption(
//        pbResourceConsumption.getDiskBytesWritten(),
//        pbResourceConsumption.getDiskFileCount(),
//        pbResourceConsumption.getHdfsBytesWritten(),
//        pbResourceConsumption.getHdfsFileCount());
//  }
//
//  public static PbResourceConsumption toPbResourceConsumption(
//      ResourceConsumption resourceConsumption) {
//    return PbResourceConsumption.newBuilder()
//        .setDiskBytesWritten(resourceConsumption.diskBytesWritten())
//        .setDiskFileCount(resourceConsumption.diskFileCount())
//        .setHdfsBytesWritten(resourceConsumption.hdfsBytesWritten())
//        .setHdfsFileCount(resourceConsumption.hdfsFileCount())
//        .build();
//  }
//
//  public static Map<UserIdentifier, ResourceConsumption> fromPbUserResourceConsumption(
//      Map<String, PbResourceConsumption> pbUserResourceConsumption) {
//    Map<UserIdentifier, ResourceConsumption> map = new ConcurrentHashMap<>();
//    pbUserResourceConsumption.forEach(
//        (k, v) -> map.put(UserIdentifier$.MODULE$.apply(k), fromPbResourceConsumption(v)));
//    return map;
//  }
//
//  public static Map<String, PbResourceConsumption> toPbUserResourceConsumption(
//      Map<UserIdentifier, ResourceConsumption> userResourceConsumption) {
//    Map<String, PbResourceConsumption> map = new ConcurrentHashMap<>();
//    userResourceConsumption.forEach((k, v) -> map.put(k.toString(), toPbResourceConsumption(v)));
//    return map;
//  }
//
//  public static WorkerInfo fromPbWorkerInfo(PbWorkerInfo pbWorkerInfo) {
//    Map<String, DiskInfo> disks = new ConcurrentHashMap<>();
//    if (pbWorkerInfo.getDisksCount() > 0) {
//      for (PbDiskInfo pbDiskInfo : pbWorkerInfo.getDisksList()) {
//        disks.put(pbDiskInfo.getMountPoint(), fromPbDiskInfo(pbDiskInfo));
//      }
//    }
//    Map<UserIdentifier, ResourceConsumption> userResourceConsumption =
//        PbSerDeUtils.fromPbUserResourceConsumption(pbWorkerInfo.getUserResourceConsumptionMap());
//
//    return new WorkerInfo(
//        pbWorkerInfo.getHost(),
//        pbWorkerInfo.getRpcPort(),
//        pbWorkerInfo.getPushPort(),
//        pbWorkerInfo.getFetchPort(),
//        pbWorkerInfo.getReplicatePort(),
//        disks,
//        userResourceConsumption,
//        null);
//  }
//
//  public static PbWorkerInfo toPbWorkerInfo(WorkerInfo workerInfo) {
//    Iterable<DiskInfo> diskInfos = workerInfo.diskInfos().values();
//    List<PbDiskInfo> pbDiskInfos = new ArrayList<>();
//    diskInfos.forEach(k -> pbDiskInfos.add(PbSerDeUtils.toPbDiskInfo(k)));
//    return PbWorkerInfo.newBuilder()
//        .setHost(workerInfo.host())
//        .setRpcPort(workerInfo.rpcPort())
//        .setFetchPort(workerInfo.fetchPort())
//        .setPushPort(workerInfo.pushPort())
//        .setReplicatePort(workerInfo.replicatePort())
//        .addAllDisks(pbDiskInfos)
//        .putAllUserResourceConsumption(
//            PbSerDeUtils.toPbUserResourceConsumption(workerInfo.userResourceConsumption()))
//        .build();
//  }
//
//  public static PartitionLocation fromPbPartitionLocation(PbPartitionLocation pbLoc) {
//    Mode mode = Mode.MASTER;
//    if (pbLoc.getMode() == PbPartitionLocation.Mode.Slave) {
//      mode = Mode.SLAVE;
//    }
//
//    PartitionLocation partitionLocation =
//        new PartitionLocation(
//            pbLoc.getId(),
//            pbLoc.getEpoch(),
//            pbLoc.getHost(),
//            pbLoc.getRpcPort(),
//            pbLoc.getPushPort(),
//            pbLoc.getFetchPort(),
//            pbLoc.getReplicatePort(),
//            mode,
//            null,
//            StorageInfo.fromPb(pbLoc.getStorageInfo()),
//            Utils.byteStringToRoaringBitmap(pbLoc.getMapIdBitmap()));
//    if (pbLoc.hasPeer()) {
//      PbPartitionLocation peerPb = pbLoc.getPeer();
//      Mode peerMode = Mode.MASTER;
//      if (peerPb.getMode() == PbPartitionLocation.Mode.Slave) {
//        peerMode = Mode.SLAVE;
//      }
//      PartitionLocation peerLocation =
//          new PartitionLocation(
//              peerPb.getId(),
//              peerPb.getEpoch(),
//              peerPb.getHost(),
//              peerPb.getRpcPort(),
//              peerPb.getPushPort(),
//              peerPb.getFetchPort(),
//              peerPb.getReplicatePort(),
//              peerMode,
//              partitionLocation,
//              StorageInfo.fromPb(peerPb.getStorageInfo()),
//              Utils.byteStringToRoaringBitmap(peerPb.getMapIdBitmap()));
//      partitionLocation.setPeer(peerLocation);
//    }
//
//    return partitionLocation;
//  }
//
//  public static PbPartitionLocation toPbPartitionLocation(PartitionLocation location) {
//    PbPartitionLocation.Builder builder = PbPartitionLocation.newBuilder();
//    if (location.getMode() == Mode.MASTER) {
//      builder.setMode(PbPartitionLocation.Mode.Master);
//    } else {
//      builder.setMode(PbPartitionLocation.Mode.Slave);
//    }
//    builder.setHost(location.getHost());
//    builder.setEpoch(location.getEpoch());
//    builder.setId(location.getId());
//    builder.setRpcPort(location.getRpcPort());
//    builder.setPushPort(location.getPushPort());
//    builder.setFetchPort(location.getFetchPort());
//    builder.setReplicatePort(location.getReplicatePort());
//    builder.setStorageInfo(StorageInfo.toPb(location.getStorageInfo()));
//    builder.setMapIdBitmap(Utils.roaringBitmapToByteString(location.getMapIdBitMap()));
//
//    if (location.getPeer() != null) {
//      PbPartitionLocation.Builder peerBuilder = PbPartitionLocation.newBuilder();
//      if (location.getPeer().getMode() == Mode.MASTER) {
//        peerBuilder.setMode(PbPartitionLocation.Mode.Master);
//      } else {
//        peerBuilder.setMode(PbPartitionLocation.Mode.Slave);
//      }
//      peerBuilder.setHost(location.getPeer().getHost());
//      peerBuilder.setEpoch(location.getPeer().getEpoch());
//      peerBuilder.setId(location.getPeer().getId());
//      peerBuilder.setRpcPort(location.getPeer().getRpcPort());
//      peerBuilder.setPushPort(location.getPeer().getPushPort());
//      peerBuilder.setFetchPort(location.getPeer().getFetchPort());
//      peerBuilder.setReplicatePort(location.getPeer().getReplicatePort());
//      peerBuilder.setStorageInfo(StorageInfo.toPb(location.getPeer().getStorageInfo()));
//      peerBuilder.setMapIdBitmap(Utils.roaringBitmapToByteString(location.getMapIdBitMap()));
//      builder.setPeer(peerBuilder.build());
//    }
//
//    return builder.build();
//  }
// }
