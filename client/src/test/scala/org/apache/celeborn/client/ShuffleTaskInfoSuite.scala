package org.apache.celeborn.client

import org.scalatest.funsuite.AnyFunSuite

class ShuffleTaskInfoSuite extends AnyFunSuite {

  test("encode shuffle id & map attemptId") {
    val shuffleTaskInfo = new ShuffleTaskInfo
    val encodeShuffleId = shuffleTaskInfo.getShuffleId("shuffleId")
    assert(encodeShuffleId == 0)

    // another shuffle
    val encodeShuffleId1 = shuffleTaskInfo.getShuffleId("shuffleId1")
    assert(encodeShuffleId1 == 1)

    val encodeShuffleId0 = shuffleTaskInfo.getShuffleId("shuffleId")
    assert(encodeShuffleId0 == 0)

    val encodeAttemptId011 = shuffleTaskInfo.getAttemptId("shuffleId", 1, "attempt1")
    val encodeAttemptId112 = shuffleTaskInfo.getAttemptId("shuffleId1", 1, "attempt2")
    val encodeAttemptId021 = shuffleTaskInfo.getAttemptId("shuffleId", 2, "attempt1")
    val encodeAttemptId012 = shuffleTaskInfo.getAttemptId("shuffleId", 1, "attempt2")
    assert(encodeAttemptId011 == 0)
    assert(encodeAttemptId112 == 0)
    assert(encodeAttemptId021 == 0)
    assert(encodeAttemptId012 == 1)

    // remove shuffleId and reEncode
    shuffleTaskInfo.remove(encodeShuffleId)
    val encodeShuffleIdNew = shuffleTaskInfo.getShuffleId("shuffleId")
    assert(encodeShuffleIdNew == 2)
  }
}
