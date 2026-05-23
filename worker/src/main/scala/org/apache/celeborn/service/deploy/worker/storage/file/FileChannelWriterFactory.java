package org.apache.celeborn.service.deploy.worker.storage.file;

import org.apache.celeborn.common.meta.DiskFileInfo;
import org.apache.celeborn.service.deploy.worker.storage.file.chunk.compressed.ChunkCompressedFileChannelWriter;

import java.io.IOException;

public class FileChannelWriterFactory {
    public static FileChannelWriter getFileChannelWriter(DiskFileInfo diskFileInfo, long chunkSize) throws IOException {
        if (diskFileInfo.isChunkCompressionEnabled()) {
            return new ChunkCompressedFileChannelWriter(diskFileInfo, chunkSize);
        } else {
            return new BypassFileChannelWriter(diskFileInfo);
        }
    }
}
