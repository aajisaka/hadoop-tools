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
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInotifyEventInputStream;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.hadoop.hdfs.inotify.MissingEventsException;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

import java.io.IOException;
import java.util.List;

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
        outputEvents(eventBatch);
      } catch (InterruptedException e) {
        System.out.println("Interrupted. Exiting...");
        return;
      } catch (MissingEventsException e) {
        e.printStackTrace();
        return;
      }
    }
  }

  /**
   * Display the details of the events to {@link System#out}.
   * @param eventBatch events (should be non-null)
   * @throws NullPointerException if the events is null.
   */
  private static void outputEvents(EventBatch eventBatch) {
    for (Event event : eventBatch.getEvents()) {
      Event.EventType type = event.getEventType();
      switch (type) {
        case CREATE:
          Event.CreateEvent create = (Event.CreateEvent) event;
          String path = create.getPath();
          String group = create.getGroupName();
          String owner = create.getOwnerName();
          long timestamp = create.getCtime();
          System.out.println("CREATE: " + path + " created by " +
              group + ":" + owner + " at " + timestamp + ".");
          break;
        case CLOSE:
          Event.CloseEvent close = (Event.CloseEvent) event;
          path = close.getPath();
          timestamp = close.getTimestamp();
          long size = close.getFileSize();
          System.out.println("CLOSE: " + path + " closed at " +
              timestamp + ". Size: " + size + ".");
          break;
        case APPEND:
          Event.AppendEvent append = (Event.AppendEvent) event;
          path = append.getPath();
          System.out.println("APPEND: " + path + " was opened for append.");
          break;
        case RENAME:
          Event.RenameEvent rename = (Event.RenameEvent) event;
          String srcPath = rename.getSrcPath();
          String dstPath = rename.getDstPath();
          timestamp = rename.getTimestamp();
          System.out.println("RENAME: " + srcPath + " was renamed to " +
              dstPath + " at " + timestamp + ".");
          break;
        case METADATA:
          Event.MetadataUpdateEvent metadata =
              (Event.MetadataUpdateEvent) event;
          outputMetadata(metadata);
          break;
        case UNLINK:
          Event.UnlinkEvent unlink = (Event.UnlinkEvent) event;
          path = unlink.getPath();
          timestamp = unlink.getTimestamp();
          System.out.println("UNLINK: " + path + " was removed at " +
              timestamp + ".");
          break;
      }
    }
  }

  private static void outputMetadata (Event.MetadataUpdateEvent metadata) {
    Event.MetadataUpdateEvent.MetadataType metadataType =
        metadata.getMetadataType();
    String path = metadata.getPath();
    System.out.print("METADATA: " + metadataType + " of " + path +
        " was updated. ");
    switch (metadataType) {
      case TIMES:
        long aTime = metadata.getAtime();
        long mTime = metadata.getMtime();
        System.out.println("atime: " + aTime + " mtime: " + mTime + ".");
        break;
      case REPLICATION:
        int replication = metadata.getReplication();
        System.out.println("replication: " + replication + ".");
        break;
      case OWNER:
        String group = metadata.getGroupName();
        String owner = metadata.getOwnerName();
        System.out.println("group: " + group + "owner: " + owner + ".");
        break;
      case PERMS:
        FsPermission permission = metadata.getPerms();
        System.out.println("permission: " + permission + ".");
        break;
      case ACLS:
        List<AclEntry> aclList = metadata.getAcls();
        System.out.println("ACLs: " + aclList + ".");
        break;
      case XATTRS:
        List<XAttr> xattrList = metadata.getxAttrs();
        System.out.println("XATTRs: " + xattrList);
        break;
    }
  }
}