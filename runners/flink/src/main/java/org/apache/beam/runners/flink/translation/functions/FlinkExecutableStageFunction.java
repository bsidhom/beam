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

import static org.apache.flink.util.Preconditions.checkState;

import com.google.common.collect.ImmutableMap;
import java.util.stream.Stream;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.ProvisionApi;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.flink.execution.EnvironmentSession;
import org.apache.beam.runners.flink.execution.SdkHarnessManager;
import org.apache.beam.runners.flink.execution.SingletonSdkHarnessManager;
import org.apache.beam.runners.fnexecution.artifact.ArtifactSource;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.util.DoFnAndMainOutput;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

public class FlinkExecutableStageFunction<InputT, OutputT> extends
    RichMapPartitionFunction<WindowedValue<InputT>, WindowedValue<OutputT>> {

  private final RunnerApi.PTransform transform;
  private final RunnerApi.Components components;

  private SdkHarnessClient client;
  private ProcessBundleDescriptors.SimpleProcessBundleDescriptor processBundleDescriptor;

  public FlinkExecutableStageFunction(RunnerApi.PTransform transform,
      RunnerApi.Components components) {
    this.transform = transform;
    this.components = components;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    ExecutableStage stage = ExecutableStage.fromPTransform(transform, components);
    SdkHarnessManager manager = SingletonSdkHarnessManager.getInstance();
    ProvisionApi.ProvisionInfo provisionInfo = ProvisionApi.ProvisionInfo.newBuilder()
        // TODO: Set this from job metadata.
        .setJobId("job-id")
        .setWorkerId(getRuntimeContext().getTaskNameWithSubtasks())
        .build();
    RunnerApi.Environment environment = RunnerApi.Environment.newBuilder()
        // TODO: Set this from transform metadata.
        .setUrl("beam-java")
        .build();
    // TODO: Get distributed cache-based artifact source.
    ArtifactSource artifactSource = new ArtifactSource() {
      @Override
      public ArtifactApi.Manifest getManifest() {
        throw new UnsupportedOperationException();
      }

      @Override
      public Stream<ArtifactApi.ArtifactChunk> getArtifact(String name) {
        throw new UnsupportedOperationException();
      }
    };
    EnvironmentSession session = manager.getSession(provisionInfo, environment, artifactSource);
    // TODO: Get ApiServiceDescriptor for data endpoint from environment session.
    Endpoints.ApiServiceDescriptor dataEndpoint = null;
    client = session.getClient();
    processBundleDescriptor =
        ProcessBundleDescriptors.fromExecutableStage("1", stage, components, null);
    // TODO: Deserialize executable stage and possibly translate into process bundle descriptor.
  }

  @Override
  public void mapPartition(Iterable<WindowedValue<InputT>> input,
      Collector<WindowedValue<OutputT>> collector) throws Exception {
    checkState(client != null, "SDK client not prepared");
    checkState(processBundleDescriptor != null,
        "ProcessBundleDescriptor not prepared");
    // TODO: Wire through proper coder.
    Coder<WindowedValue<InputT>> coder = null;
    // TODO: Set target.
    BeamFnApi.Target target = BeamFnApi.Target.newBuilder()
        .setName("output")
        .build();
    SdkHarnessClient.RemoteInputDestination<WindowedValue<InputT>> destination =
        (SdkHarnessClient.RemoteInputDestination<WindowedValue<InputT>>) processBundleDescriptor.getRemoteInputDestination();
        //SdkHarnessClient.RemoteInputDestination.of(coder, target);
    SdkHarnessClient.BundleProcessor<InputT> processor = client.getProcessor(
        processBundleDescriptor.getProcessBundleDescriptor(), destination);
    // TODO: Get/create output receivers.
    SdkHarnessClient.RemoteOutputReceiver<WindowedValue<OutputT>> mainOutputReceiver =
        new SdkHarnessClient.RemoteOutputReceiver<WindowedValue<OutputT>>() {
          @Override
          public Coder<WindowedValue<OutputT>> getCoder() {
            // TODO: Get output coder.
            return null;
          }

          @Override
          public FnDataReceiver<WindowedValue<OutputT>> getReceiver() {
            return new FnDataReceiver<WindowedValue<OutputT>>() {
              @Override
              public void accept(WindowedValue<OutputT> input) throws Exception {
                collector.collect(input);
              }
            };
          }
        };
    SdkHarnessClient.ActiveBundle<InputT> bundle = processor.newBundle(
        ImmutableMap.of(target, mainOutputReceiver));
    try (CloseableFnDataReceiver<WindowedValue<InputT>> inputReceiver = bundle.getInputReceiver()) {
      for (WindowedValue<InputT> value : input) {
        inputReceiver.accept(value);
      }
    }
    bundle.getBundleResponse().toCompletableFuture().get();
  }

  @Override
  public void close() throws Exception {
    client.close();
    client = null;
  }
}
