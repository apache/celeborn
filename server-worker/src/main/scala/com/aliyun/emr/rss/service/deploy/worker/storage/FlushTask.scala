package com.aliyun.emr.rss.service.deploy.worker.storage

import java.nio.channels.FileChannel

import FileWriter.FlushNotifier
import io.netty.buffer.{ByteBufUtil, CompositeByteBuf}
import org.apache.hadoop.fs.FSDataOutputStream

private[worker] abstract class FlushTask(
    val buffer: CompositeByteBuf,
    val notifier: FlushNotifier) {
  def flush(): Unit
}

private[worker] class LocalFlushTask(
    buffer: CompositeByteBuf,
    fileChannel: FileChannel,
    notifier: FileWriter.FlushNotifier) extends FlushTask(buffer, notifier) {
  override def flush(): Unit = {
    fileChannel.write(buffer.nioBuffers())
  }
}

private[worker] class HdfsFlushTask(
    buffer: CompositeByteBuf,
    fsStream: FSDataOutputStream,
    notifier: FileWriter.FlushNotifier) extends FlushTask(buffer, notifier) {
  override def flush(): Unit = {
    fsStream.write(ByteBufUtil.getBytes(buffer))
  }
}