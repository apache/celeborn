package org.apache.celeborn.service.deploy.worker.storage.file;

import io.netty.buffer.CompositeByteBuf;
import org.apache.celeborn.common.meta.DiskFileInfo;
import org.apache.celeborn.common.util.FileChannelUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class BypassFileChannelWriter extends FileChannelWriter  {
    private final FileChannel channel;

    public BypassFileChannelWriter(DiskFileInfo diskFileInfo) throws IOException {
        channel = FileChannelUtils.createWritableFileChannel(diskFileInfo.getFilePath());
    }

    @Override
    public void write(CompositeByteBuf buffer, boolean gatherApiEnabled) throws IOException {
        ByteBuffer[] buffers = buffer.nioBuffers();
        if (gatherApiEnabled) {
            int readableBytes = buffer.readableBytes();
            long written = 0L;
            do {
                written = channel.write(buffers) + written;
            } while (written != readableBytes);
        } else {
            for (ByteBuffer byteBuffer : buffers) {
                while (byteBuffer.hasRemaining()) {
                    channel.write(byteBuffer);
                }
            }
        }
    }

    @Override
    public void close(boolean commitFilesFsync) {
        try {
            if (commitFilesFsync) {
                channel.force(false);
            }
        } catch (IOException e) {
            // log and ignore
        } finally {
            try {
                channel.close();
            } catch (IOException e) {
                // log and ignore
            }
        }
    }
}
