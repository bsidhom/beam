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
import com.google.common.collect.Iterables;
import com.google.protobuf.Struct;
import io.netty.util.internal.ThreadLocalRandom;
import java.math.BigInteger;
import java.util.Map;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.ProvisionApi;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.flink.execution.CachedArtifactSource;
import org.apache.beam.runners.flink.execution.EnvironmentSession;
import org.apache.beam.runners.flink.execution.SdkHarnessManager;
import org.apache.beam.runners.flink.execution.SingletonSdkHarnessManager;
import org.apache.beam.runners.fnexecution.artifact.ArtifactSource;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.util.MoreFutures;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** ExecutableStage operator. */
public class FlinkExecutableStageFunction<InputT> extends
    RichMapPartitionFunction<WindowedValue<InputT>, RawUnionValue> {

  private static final Logger LOG =
      LoggerFactory.getLogger(FlinkExecutableStageFunction.class);

  private final RunnerApi.ExecutableStagePayload payload;
  private final RunnerApi.Components components;
  private final Map<String, Integer> outputMap;

  private transient EnvironmentSession session;
  private transient SdkHarnessClient client;
  private transient ProcessBundleDescriptors.SimpleProcessBundleDescriptor processBundleDescriptor;

  public FlinkExecutableStageFunction(RunnerApi.ExecutableStagePayload payload,
      RunnerApi.Components components,
      Map<String, Integer> outputMap) {
    this.payload = payload;
    this.components = components;
    this.outputMap = outputMap;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    ExecutableStage stage = ExecutableStage.fromPayload(payload, components);
    SdkHarnessManager manager = SingletonSdkHarnessManager.getInstance();
    Struct options = PipelineOptionsTranslation.toProto(PipelineOptionsFactory.create());
    ProvisionApi.ProvisionInfo provisionInfo = ProvisionApi.ProvisionInfo.newBuilder()
        // TODO: Set this from job metadata.
        .setJobId("job-id")
        .setWorkerId(getRuntimeContext().getTaskNameWithSubtasks())
        .setPipelineOptions(options)
        .build();
    RunnerApi.Environment environment = RunnerApi.Environment.newBuilder()
        // TODO: Set this from transform metadata.
        .setUrl("gcr.io/google.com/hadoop-cloud-dev/beam/python")
        .build();
    ArtifactSource artifactSource =
        CachedArtifactSource.createDefault(getRuntimeContext().getDistributedCache());
    session = manager.getSession(provisionInfo, environment, artifactSource);
    Endpoints.ApiServiceDescriptor dataEndpoint = session.getDataServiceDescriptor();
    client = session.getClient();
    LOG.debug("Data endpoint: {}", dataEndpoint.getUrl());
    String id = new BigInteger(32, ThreadLocalRandom.current()).toString(36);
    processBundleDescriptor =
        ProcessBundleDescriptors.fromExecutableStage(id, stage, components, dataEndpoint);
    LOG.debug("Process bundle descriptor: {}", processBundleDescriptor);
  }

  @Override
  public void mapPartition(Iterable<WindowedValue<InputT>> input,
      Collector<RawUnionValue> collector) throws Exception {
    checkState(client != null, "SDK client not prepared");
    checkState(processBundleDescriptor != null,
        "ProcessBundleDescriptor not prepared");
    // NOTE: A double-cast is necessary below in order to hide pseudo-covariance from the compiler.
    @SuppressWarnings("unchecked")
    SdkHarnessClient.RemoteInputDestination<WindowedValue<InputT>> destination =
        (SdkHarnessClient.RemoteInputDestination<WindowedValue<InputT>>)
        (SdkHarnessClient.RemoteInputDestination<?>)
            processBundleDescriptor.getRemoteInputDestination();

    SdkHarnessClient.BundleProcessor<InputT> processor = client.getProcessor(
        processBundleDescriptor.getProcessBundleDescriptor(), destination);
    processor.getRegistrationFuture().toCompletableFuture().get();
    Map<BeamFnApi.Target, Coder<WindowedValue<?>>> outputCoders =
        processBundleDescriptor.getOutputTargetCoders();
    ImmutableMap.Builder<BeamFnApi.Target,
        SdkHarnessClient.RemoteOutputReceiver<?>> receiverBuilder = ImmutableMap.builder();
    final Object collectorLock = new Object();
    for (Map.Entry<BeamFnApi.Target, Coder<WindowedValue<?>>> entry : outputCoders.entrySet()) {
      BeamFnApi.Target target = entry.getKey();
      String pcollectionId = processBundleDescriptor.getProcessBundleDescriptor()
          .getTransformsOrThrow(target.getPrimitiveTransformReference())
          .getInputsOrThrow(target.getName());
      int unionTag = outputMap.get(pcollectionId);
      Coder<WindowedValue<?>> coder = entry.getValue();
      LOG.warn("CODER FOR {}: {}", target, coder);
      SdkHarnessClient.RemoteOutputReceiver<WindowedValue<?>> receiver = new SdkHarnessClient.RemoteOutputReceiver<WindowedValue<?>>() {
        @Override
        public Coder<WindowedValue<?>> getCoder() {
          return coder;
        }

        @Override
        public FnDataReceiver<WindowedValue<?>> getReceiver() {
          return new FnDataReceiver<WindowedValue<?>>() {
            @Override
            public void accept(WindowedValue<?> input) throws Exception {
              LOG.debug("Receiving value: {}", input);
              // TODO: Can this be called by multiple threads? Are calls guaranteed to at least be
              // serial? If not, these calls may need to be synchronized.
              // TODO: If this needs to be synchronized, consider requiring immutable maps.
              synchronized (collectorLock) {
                // TODO: Get the correct output union tag from the corresponding output tag.
                // Plumb through output tags from process bundle descriptor.
                // NOTE: The output map is guaranteed to be non-empty at this point, so we can
                // always grab index 0.
                // TODO: Plumb through TupleTag <-> Target mappings to get correct union tag here.
                // For now, assume only one union tag.
                collector.collect(new RawUnionValue(unionTag, input));
              }
            }
          };
        }
      };
      receiverBuilder.put(target, receiver);
    }
    Map<BeamFnApi.Target,
        SdkHarnessClient.RemoteOutputReceiver<?>> receiverMap =
        receiverBuilder.build();

    SdkHarnessClient.ActiveBundle<InputT> bundle = processor.newBundle(receiverMap);
    try (CloseableFnDataReceiver<WindowedValue<InputT>> inputReceiver = bundle.getInputReceiver()) {
      for (WindowedValue<InputT> value : input) {
        LOG.debug("Sending value: {}", value);
        inputReceiver.accept(value);
      }
    }

    // Await all outputs and active bundle completion. This is necessary because the Flink collector
    // must not be accessed outside of mapPartition.
    bundle.getOutputClients().values().forEach(client -> {
      try {
        client.awaitCompletion();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    MoreFutures.get(bundle.getBundleResponse());
  }

  @Override
  public void close() throws Exception {
    // TODO: In what order should the client and session be closed? Should the session eventually
    // own the client itself?
    client.close();
    client = null;
    session.close();
  }
}
