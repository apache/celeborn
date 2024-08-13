/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.runtime.library.input;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.util.Progress;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.library.common.sort.impl.TezRawKeyValueIterator;

@Private
public class CelebornOrderedGroupedInputLegacy extends CelebornOrderedGroupedKVInput {

  private final Progress progress = new Progress();

  public CelebornOrderedGroupedInputLegacy(InputContext inputContext, int numPhysicalInputs) {
    super(inputContext, numPhysicalInputs);
  }

  @Private
  public TezRawKeyValueIterator getIterator() throws IOException, InterruptedException, TezException {
    // wait for input so that iterator is available
    synchronized(this) {
    if (getNumPhysicalInputs() == 0) {
      return new TezRawKeyValueIterator() {
        @Override
        public DataInputBuffer getKey() throws IOException {
          throw new RuntimeException("No data available in Input");
        }

        @Override
        public DataInputBuffer getValue() throws IOException {
          throw new RuntimeException("No data available in Input");
        }

        @Override
        public boolean next() throws IOException {
          return false;
        }

        @Override
        public boolean hasNext() throws IOException {
          return false;
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public Progress getProgress() {
          progress.complete();
          return progress;
        }

        @Override
        public boolean isSameKey() throws IOException {
          throw new UnsupportedOperationException("isSameKey is not supported");
        }
      };
    }
    }

    waitForInputReady();
    synchronized(this) {
      return rawIter;
    }
  }
}
