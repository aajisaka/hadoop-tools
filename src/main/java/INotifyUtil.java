import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInotifyEventInputStream;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.hadoop.hdfs.inotify.MissingEventsException;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

import java.io.IOException;

public class INotifyUtil {

  public static void main(String args[])
      throws IOException, InterruptedException {
    Configuration conf = new Configuration();
    DFSClient client = new DFSClient(NameNode.getAddress(conf), conf);
    DFSInotifyEventInputStream iStream = client.getInotifyEventStream();
    EventBatch eventBatch = null;
    try {
      eventBatch = iStream.take();
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
            System.out.println("CLOSE: " + path + " closed at " +
                timestamp + ".");
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
            Event.MetadataUpdateEvent.MetadataType metadataType =
                metadata.getMetadataType();
            path = metadata.getPath();
            System.out.println("METADATA: " + metadataType + " of " + path +
                " was updated.");
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
    } catch (MissingEventsException e) {
      e.printStackTrace();
    }
  }
}