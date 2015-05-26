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
import org.apache.hadoop.io.Text;
import org.junit.Test;

/**
 * Benchmark for {@link String} and {@link Text}.
 * The result shows we should avoid {@link String#substring(int, int)}
 * as possible in loop.
 */
public class TestString {

  @Test
  public void testStringSubstring() throws Exception {
    Text text = new Text("string");
    Text text1 = new Text();
    Text text2 = new Text();

    long start = System.nanoTime();
    for (int i = 0; i < 100000000; i++) {
      String str = text.toString();
      String str1 = str.substring(0, 2);
      String str2 = str.substring(3, str.length());
      text1.set(str1);
      text2.set(str2);
    }
    long end = System.nanoTime();
    System.out.println("TextStringSubString");
    System.out.println("text1: " + text1.toString());
    System.out.println("text2: " + text2.toString());
    System.out.println("Elapsed Time: " + 
        (end - start) / 1000000000f + " seconds.");
  }

  @Test
  public void testTextSubstring() throws Exception {
    Text text = new Text("string");
    Text text1 = new Text();
    Text text2 = new Text();

    long start = System.nanoTime();
    for (int i = 0; i < 100000000; i++) {
      text1.set(text.getBytes(), 0, 2);
      text2.set(text.getBytes(), 3, text.getLength() - 3);
    }
    long end = System.nanoTime();
    System.out.println("TestTextSubString");
    System.out.println("text1: " + text1.toString());
    System.out.println("text2: " + text2.toString());
    System.out.println("Elapsed Time: " +
        (end - start) / 1000000000f + " seconds.");
  }
}
