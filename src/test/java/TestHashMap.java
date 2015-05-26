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
import org.apache.hadoop.io.LongWritable;
import org.junit.Test;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * Benchmark for {@link HashSet} key.
 * {@link Long} is faster than {@link String}.
 */
public class TestHashMap {

  public final int SET_SIZE = 10000000;
  public final int DATA_SIZE = 1000000;

  @Test
  public void testHashSetString() throws Exception {
    final Set<String> hashSet = new HashSet<>();
    final Random random = new Random(0xDEADBEEF);
    int matched = 0;
    LongWritable num = new LongWritable();

    long startTime = System.nanoTime();

    for (int i = 0; i < SET_SIZE; i++) {
      // input data is String
      String input = Long.toString(random.nextLong());
      // disable optimizer
      if (input.length() > 5) {
        hashSet.add(input);
      }
    }

    random.setSeed(0xDEADBEEF);

    for (int i = 0; i < DATA_SIZE; i++) {
      // query data is LongWritable
      num.set(random.nextLong());
      if (hashSet.contains(num.toString())) {
        matched++;
      }
    }

    long endTime = System.nanoTime();
    System.out.println("  HashSet<String>");
    System.out.println("  Elapsed time: " + (endTime - startTime) / 1000000 + " ms");
    System.out.println("  Matched " + matched + " times");
  }

  @Test
  public void testHashSetLong() throws Exception {
    final Set<Long> hashSet = new HashSet<>();
    final Random random = new Random(0xDEADBEEF);
    int matched = 0;
    LongWritable num = new LongWritable();

    long startTime = System.nanoTime();

    for (int i = 0; i < SET_SIZE; i++) {
      // input data is String
      String input = Long.toString(random.nextLong());
      // disable optimizer
      if (input.length() > 5) {
        hashSet.add(Long.parseLong(input));
      }
    }

    random.setSeed(0xDEADBEEF);

    for (int i = 0; i < DATA_SIZE; i++) {
      // query data is LongWritable
      num.set(random.nextLong());
      if (hashSet.contains(num.get())) {
        matched++;
      }
    }

    long endTime = System.nanoTime();
    System.out.println("  HashSet<Long>");
    System.out.println("  Elapsed time: " + (endTime - startTime) / 1000000f + " ms");
    System.out.println("  Matched " + matched + " times");
  }
}
