/*
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
package org.apache.beam.runners.flink.translation.functions;

import org.apache.beam.sdk.util.WindowedValue;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class FlinkExecutableStageFunction<InputT, OutputT> extends
    RichMapPartitionFunction<WindowedValue<InputT>, WindowedValue<OutputT>> {

  public FlinkExecutableStageFunction() {
    // TODO: Set executable stage subgraph, possibly in serialized form.
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    // TODO: Deserialize executable stage and possibly translate into process bundle descriptor.
    // TODO: Obtain SDK harness client from harness manager.
  }

  @Override
  public void mapPartition(Iterable<WindowedValue<InputT>> input,
      Collector<WindowedValue<OutputT>> collector) throws Exception {
    // TODO: Create an active bundle.
    for (WindowedValue<InputT> value : input) {
      // TODO: Write value to grpc fn data receiver.
    }
    // TODO: Read all data from fn data receiver and collect in output.
  }

  @Override
  public void close() throws Exception {
    // TODO
  }
}
