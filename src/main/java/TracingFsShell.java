/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.tracing.SpanReceiverHost;
import org.apache.hadoop.util.ToolRunner;
import org.apache.htrace.Sampler;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;

/**
 * FsShell with tracing by HTrace.
 * This class is for Hadoop 2.7, not for trunk.
 */
public class TracingFsShell {
  public static void main(String argv[]) throws Exception {
    Configuration conf = new Configuration();
    conf.setQuietMode(false);
    FsShell shell = new FsShell();
    shell.setConf(conf);
    SpanReceiverHost.getInstance(conf);
    int res;
    try (TraceScope ts = Trace.startSpan("FsShell", Sampler.ALWAYS)){
      res = ToolRunner.run(shell, argv);
    }
    System.exit(res);
  }
}
