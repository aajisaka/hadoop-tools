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
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.util.Time;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

/**
 * Benchmark of {@link DFSUtilClient#bytes2String} and
 * {@link DFSUtilClient#string2Bytes}.
 */
public class TestDFSUtilClient {

  @Test
  public void testString2Bytes() {
    final long numLoop = 100000000;
    final String str = "testString2Bytes";
    byte[] b = null;
    long start = Time.monotonicNow();
    for (int i = 0; i < numLoop; i++) {
      b = DFSUtilClient.string2Bytes(str);
    }
    long end = Time.monotonicNow();
    System.out.println(new String(b, StandardCharsets.UTF_8));
    System.out.println("Elapsed time: " + (end - start));

    start = Time.monotonicNow();
    for (int i = 0; i < numLoop; i++) {
      b = str.getBytes(StandardCharsets.UTF_8);
    }
    end = Time.monotonicNow();
    System.out.println(new String(b, StandardCharsets.UTF_8));
    System.out.println("Elapsed time: " + (end - start));

    start = Time.monotonicNow();
    for (int i = 0; i < numLoop; i++) {
      b = DFSUtilClient.string2Bytes(str);
    }
    end = Time.monotonicNow();
    System.out.println(new String(b, StandardCharsets.UTF_8));
    System.out.println("Elapsed time: " + (end - start));

    start = Time.monotonicNow();
    for (int i = 0; i < numLoop; i++) {
      b = str.getBytes(StandardCharsets.UTF_8);
    }
    end = Time.monotonicNow();
    System.out.println(new String(b, StandardCharsets.UTF_8));
    System.out.println("Elapsed time: " + (end - start));
  }

  @Test
  public void testBytes2String() {
    final long numLoop = 100000000;
    final byte[] bytes = "testBytes2String".getBytes(StandardCharsets.UTF_8);
    String str = null;
    long start = Time.monotonicNow();
    for (int i = 0; i < numLoop; i++) {
      str = DFSUtilClient.bytes2String(bytes);
    }
    long end = Time.monotonicNow();
    System.out.println(str);
    System.out.println("Elapsed time: " + (end - start));

    start = Time.monotonicNow();
    for (int i = 0; i < numLoop; i++) {
      str = new String(bytes, StandardCharsets.UTF_8);
    }
    end = Time.monotonicNow();
    System.out.println(str);
    System.out.println("Elapsed time: " + (end - start));

    start = Time.monotonicNow();
    for (int i = 0; i < numLoop; i++) {
      str = DFSUtilClient.bytes2String(bytes);
    }
    end = Time.monotonicNow();
    System.out.println(str);
    System.out.println("Elapsed time: " + (end - start));

    start = Time.monotonicNow();
    for (int i = 0; i < numLoop; i++) {
      str = new String(bytes, StandardCharsets.UTF_8);
    }
    end = Time.monotonicNow();
    System.out.println(str);
    System.out.println("Elapsed time: " + (end - start));
  }
}
