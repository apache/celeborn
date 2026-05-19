package org.apache.celeborn.service.deploy.worker.storage.file;

import io.netty.buffer.CompositeByteBuf;

import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class FileChannelWriter {
    public abstract void write(CompositeByteBuf buffer, boolean gatherApiEnabled) throws IOException;

    public abstract void close(boolean commitFilesFsync);
}
