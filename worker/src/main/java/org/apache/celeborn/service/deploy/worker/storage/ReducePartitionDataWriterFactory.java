package org.apache.celeborn.service.deploy.worker.storage;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.metrics.source.AbstractSource;
import org.apache.celeborn.service.deploy.worker.storage.invertedindexreduce.InvertedIndexReducePartitionDataWriter;

import java.io.IOException;

public class ReducePartitionDataWriterFactory {

    public static PartitionDataWriter createReducePartitionDataWriter(StorageManager storageManager,
                                                                      AbstractSource workerSource,
                                                                      CelebornConf conf,
                                                                      DeviceMonitor deviceMonitor,
                                                                      PartitionDataWriterContext writerContext) throws IOException {

        if (writerContext.isInvertedIndexReaderEnabled()) {
            return new InvertedIndexReducePartitionDataWriter(
                    storageManager,
                    workerSource,
                    conf,
                    deviceMonitor,
                    writerContext);
        } else {
            return new ReducePartitionDataWriter(
                    storageManager,
                    workerSource,
                    conf,
                    deviceMonitor,
                    writerContext);
        }
    }
}
