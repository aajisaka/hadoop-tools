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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInotifyEventInputStream;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.hadoop.hdfs.inotify.MissingEventsException;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

import java.io.IOException;

/**
 * Sample class for using HDFS INotify feature.
 */
public class INotifyUtil {

  /**
   * Poll events and output the details.
   * Ctrl + C to stop polling.
   * @param args the parameter is not used.
   * @throws IOException if configuration error or I/O error happens.
   */
  public static void main(String args[]) throws IOException {
    Configuration conf = new Configuration();
    DFSClient client = new DFSClient(NameNode.getAddress(conf), conf);
    DFSInotifyEventInputStream iStream = client.getInotifyEventStream();
    while (true) {
      try {
        EventBatch eventBatch = iStream.take();
        for (Event event : eventBatch.getEvents()) {
          System.out.println(event.toString());
        }
      } catch (InterruptedException e) {
        System.out.println("Interrupted. Exiting...");
        return;
      } catch (MissingEventsException e) {
        e.printStackTrace();
        return;
      }
    }
  }
}