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

package org.apache.celeborn.tests.mr;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.junit.Ignore;
import org.junit.Test;

// This test case is ignored because of class dependencies conflicts.
@Ignore
public class WordCountTest extends MRTestBase {
  static String inputPath = "word_count_input";
  static List<String> wordTable =
      Lists.newArrayList(
          "apple", "banana", "fruit", "cherry", "Chinese", "America", "Japan", "tomato");

  static class TestCaseWordCount extends org.apache.hadoop.mapred.WordCount {
    String outputPath = "word_count_output/" + System.currentTimeMillis();

    @Override
    public int run(String[] args) throws Exception {
      JobConf conf = (JobConf) getConf();
      conf.setJobName("wordcount");

      // the keys are words (strings)
      conf.setOutputKeyClass(Text.class);
      // the values are counts (ints)
      conf.setOutputValueClass(IntWritable.class);

      conf.setMapperClass(MapClass.class);
      conf.setCombinerClass(Reduce.class);
      conf.setReducerClass(Reduce.class);
      conf.setNumMapTasks(1);
      conf.setNumReduceTasks(1);
      FileInputFormat.setInputPaths(conf, new Path(inputPath));
      FileOutputFormat.setOutputPath(conf, new Path(outputPath));
      Job job = new Job(conf);
      return job.waitForCompletion(true) ? 0 : 1;
    }
  }

  @Override
  public Tool getTestCaseClass() {
    return new TestCaseWordCount();
  }

  private void genTestData() throws IOException {
    FSDataOutputStream outputStream = fs.create(new Path(inputPath));
    Random random = new Random();
    for (int i = 0; i < 1000; i++) {
      String tmp = random.nextInt(wordTable.size()) + "\n";
      outputStream.writeBytes(tmp);
    }
    outputStream.flush();
    outputStream.close();
  }

  @Test
  public void testWordCount() throws Exception {
    genTestData();
    testMRApp();
  }
}
