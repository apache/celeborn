package org.apache.tez.runtime.library.common.shuffle.orderedgrouped;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.Inflater;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.events.CompositeRoutedDataMovementEvent;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.InputFailedEvent;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.shuffle.ShuffleEventHandler;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CelebornEventHandlerOrderedGrouped implements ShuffleEventHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(CelebornEventHandlerOrderedGrouped.class);

  private final InputContext inputContext;
  private final boolean compositeFetch;
  private final Inflater inflater;
  private int partitionId = -1;

  private final AtomicInteger nextToLogEventCount = new AtomicInteger(0);
  private final AtomicInteger numDmeEvents = new AtomicInteger(0);
  private final AtomicInteger numObsoletionEvents = new AtomicInteger(0);
  private final AtomicInteger numDmeEventsNoData = new AtomicInteger(0);

  public CelebornEventHandlerOrderedGrouped(InputContext inputContext, boolean compositeFetch) {
    this.inputContext = inputContext;
    this.compositeFetch = compositeFetch;
    this.inflater = TezCommonUtils.newInflater();
  }

  @Override
  public void handleEvents(List<Event> events) throws IOException {
    for (Event event : events) {
      handleEvent(event);
    }
  }

  private void handleEvent(Event event) throws IOException {
    if (event instanceof DataMovementEvent) {
      numDmeEvents.incrementAndGet();
      DataMovementEvent dmEvent = (DataMovementEvent) event;
      ShuffleUserPayloads.DataMovementEventPayloadProto shufflePayload;
      try {
        shufflePayload =
            ShuffleUserPayloads.DataMovementEventPayloadProto.parseFrom(
                ByteString.copyFrom(dmEvent.getUserPayload()));
      } catch (InvalidProtocolBufferException e) {
        throw new TezUncheckedException("Unable to parse DataMovementEvent payload", e);
      }
      BitSet emptyPartitionsBitSet = null;
      if (shufflePayload.hasEmptyPartitions()) {
        try {
          byte[] emptyPartitions =
              TezCommonUtils.decompressByteStringToByteArray(
                  shufflePayload.getEmptyPartitions(), inflater);
          emptyPartitionsBitSet = TezUtilsInternal.fromByteArray(emptyPartitions);
        } catch (IOException e) {
          throw new TezUncheckedException("Unable to set the empty partition to succeeded", e);
        }
      }
      processDataMovementEvent(dmEvent, shufflePayload, emptyPartitionsBitSet);
    } else if (event instanceof CompositeRoutedDataMovementEvent) {
      CompositeRoutedDataMovementEvent crdme = (CompositeRoutedDataMovementEvent) event;
      ShuffleUserPayloads.DataMovementEventPayloadProto shufflePayload;
      try {
        shufflePayload =
            ShuffleUserPayloads.DataMovementEventPayloadProto.parseFrom(
                ByteString.copyFrom(crdme.getUserPayload()));
      } catch (InvalidProtocolBufferException e) {
        throw new TezUncheckedException("Unable to parse DataMovementEvent payload", e);
      }
      BitSet emptyPartitionsBitSet = null;
      if (shufflePayload.hasEmptyPartitions()) {
        try {
          byte[] emptyPartitions =
              TezCommonUtils.decompressByteStringToByteArray(
                  shufflePayload.getEmptyPartitions(), inflater);
          emptyPartitionsBitSet = TezUtilsInternal.fromByteArray(emptyPartitions);
        } catch (IOException e) {
          throw new TezUncheckedException("Unable to set the empty partition to succeeded", e);
        }
      }
      if (compositeFetch) {
        numDmeEvents.addAndGet(crdme.getCount());
        processCompositeRoutedDataMovementEvent(crdme, shufflePayload, emptyPartitionsBitSet);
      } else {
        for (int offset = 0; offset < crdme.getCount(); offset++) {
          numDmeEvents.incrementAndGet();
          processDataMovementEvent(crdme.expand(offset), shufflePayload, emptyPartitionsBitSet);
        }
      }
      // scheduler.updateEventReceivedTime();
    } else if (event instanceof InputFailedEvent) {
      numObsoletionEvents.incrementAndGet();
      processTaskFailedEvent((InputFailedEvent) event);
    }
    if (numDmeEvents.get() + numObsoletionEvents.get() > nextToLogEventCount.get()) {
      logProgress(false);
      // Log every 50 events seen.
      nextToLogEventCount.addAndGet(50);
    }
  }

  private void processDataMovementEvent(
      DataMovementEvent dmEvent,
      ShuffleUserPayloads.DataMovementEventPayloadProto shufflePayload,
      BitSet emptyPartitionsBitSet)
      throws IOException {
    int partitionId = dmEvent.getSourceIndex();
    this.partitionId = partitionId;

    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "DME srcIdx: "
              + partitionId
              + ", targetIdx: "
              + dmEvent.getTargetIndex()
              + ", attemptNum: "
              + dmEvent.getVersion()
              + ", payload: "
              + ShuffleUtils.stringify(shufflePayload));
    }
  }

  private void processCompositeRoutedDataMovementEvent(
      CompositeRoutedDataMovementEvent crdmEvent,
      ShuffleUserPayloads.DataMovementEventPayloadProto shufflePayload,
      BitSet emptyPartitionsBitSet)
      throws IOException {
    int partitionId = crdmEvent.getSourceIndex();
    this.partitionId = partitionId;
  }

  private void processTaskFailedEvent(InputFailedEvent ifEvent) {
    InputAttemptIdentifier taIdentifier =
        new InputAttemptIdentifier(ifEvent.getTargetIndex(), ifEvent.getVersion());
    // scheduler.obsoleteInput(taIdentifier);
    LOG.debug("Obsoleting output of src-task: {}", taIdentifier);
  }

  public int getPartitionId() {
    while (true) {
      if (partitionId != -1) {
        return partitionId;
      } else {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  @Override
  public void logProgress(boolean b) {}
}
