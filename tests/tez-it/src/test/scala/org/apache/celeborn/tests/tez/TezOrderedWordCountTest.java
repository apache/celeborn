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

package org.apache.celeborn.tests.tez;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.tez.examples.OrderedWordCount;
import org.junit.jupiter.api.Test;

import java.util.*;

public class TezOrderedWordCountTest extends TezIntegrationTestBase {

  private String inputPath = "ordered_word_count_input";
  private String outputPath = "ordered_word_count_output";
  private List<String> wordTable =
      Lists.newArrayList(
          "apple", "banana", "fruit", "cherry", "Chinese", "America", "Japan", "tomato");

  @Test
  public void orderedWordCountTest() throws Exception {
    generateInputFile();
    run();
  }

  private void generateInputFile() throws Exception {
    // For ordered word count, the key of last ordered sorter is the summation of word, the value is
    // the word. So it means this key may not be unique. Because Sorter can only make sure key is
    // sorted, so the second column (word column) may be not sorted.
    // To keep pace with verifyResults, here make sure summation of word is unique number.
    FSDataOutputStream outputStream = fs.create(new Path(inputPath));
    Random random = new Random();
    Set<Integer> used = new HashSet();
    List<String> outputList = new ArrayList<>();
    int index = 0;
    while (index < wordTable.size()) {
      int summation = random.nextInt(50);
      if (used.contains(summation)) {
        continue;
      }
      used.add(summation);
      for (int i = 0; i < summation; i++) {
        outputList.add(wordTable.get(index));
      }
      index++;
    }
    Collections.shuffle(outputList);
    for (String word : outputList) {
      outputStream.writeBytes(word + "\n");
    }
    outputStream.close();
  }

  @Override
  public Tool getTestTool() {
    return new OrderedWordCount();
  }

  @Override
  public String[] getTestArgs(String uniqueOutputName) {
    return new String[] {inputPath, outputPath + "/" + uniqueOutputName, "2"};
  }

  @Override
  public String getOutputDir(String uniqueOutputName) {
    return outputPath + "/" + uniqueOutputName;
  }
}
