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

package org.apache.celeborn.service.deploy.memory

import java.util
import java.util.concurrent.{CompletableFuture, TimeoutException, TimeUnit}

import io.netty.buffer.ByteBuf
import org.mockito.ArgumentMatchers.anyInt
import org.mockito.Mockito.{mock, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.service.deploy.worker.memory.{MemoryManager, ReadBufferDispatcher, ReadBufferListener, ReadBufferRequest}

class ReadBufferDispactherSuite extends CelebornFunSuite {

  test("[CELEBORN-1580] Test ReadBufferDispacther notify exception to listener") {
    val mockedMemoryManager = mock(classOf[MemoryManager])
    when(mockedMemoryManager.readBufferAvailable(anyInt())).thenAnswer(
      new Answer[Boolean] {
        override def answer(invocation: InvocationOnMock): Boolean = {
          throw new RuntimeException("throw exception for test")
        }
      })

    val conf = new CelebornConf()
    val readBufferDispatcher = new ReadBufferDispatcher(mockedMemoryManager, conf, null)
    val requestFuture = new CompletableFuture[Void]()

    val request = new ReadBufferRequest(
      Integer.MAX_VALUE,
      Integer.MAX_VALUE,
      new ReadBufferListener {
        override def notifyBuffers(
            allocatedBuffers: util.List[ByteBuf],
            throwable: Throwable): Unit = {
          assert(throwable != null)
          assert(throwable.isInstanceOf[RuntimeException])
          assert(throwable.getMessage.equals("throw exception for test"))
          requestFuture.complete(null);
        }
      })

    readBufferDispatcher.addBufferRequest(request)
    requestFuture.get(5, TimeUnit.SECONDS)
  }

  test("Test check thread alive") {
    val mockedMemoryManager = mock(classOf[MemoryManager])
    val conf = new CelebornConf()
    conf.set(CelebornConf.WORKER_READBUFFER_CHECK_THREAD_INTERVAL.key, "100ms")
    val readBufferDispatcher = new ReadBufferDispatcher(mockedMemoryManager, conf, null)
    val threadId1 = readBufferDispatcher.dispatcherThread.get().getId
    readBufferDispatcher.stopFlag = true
    Thread.sleep(1500)
    readBufferDispatcher.stopFlag = false
    val threadId2 = readBufferDispatcher.dispatcherThread.get().getId
    assert(threadId1 != threadId2)
  }

  test("[CELEBORN-2192] ReadBufferDispatcher should add timeout constraints to fast fail in case of timeout") {
    val memoryManager = mock(classOf[MemoryManager])
    val readBufferDispatcher = new ReadBufferDispatcher(
      memoryManager,
      new CelebornConf().set(
        CelebornConf.WORKER_READBUFFER_PROCESS_TIMEOUT.key,
        CelebornConf.WORKER_READBUFFER_ALLOCATIONWAIT.defaultValueString),
      null)
    when(memoryManager.readBufferAvailable(anyInt())).thenAnswer(new Answer[Boolean] {
      override def answer(invocationOnMock: InvocationOnMock): Boolean = false
    })
    val completableFuture = new CompletableFuture[Void]()
    val readBufferRequest = new ReadBufferRequest(
      Integer.MAX_VALUE,
      Integer.MAX_VALUE,
      new ReadBufferListener {
        override def notifyBuffers(
            allocatedBuffers: util.List[ByteBuf],
            throwable: Throwable): Unit = {
          assert(throwable != null)
          assert(throwable.isInstanceOf[TimeoutException])
          completableFuture.complete(null);
        }
      })
    readBufferDispatcher.addBufferRequest(readBufferRequest)
    completableFuture.get(5, TimeUnit.SECONDS)
  }
}
