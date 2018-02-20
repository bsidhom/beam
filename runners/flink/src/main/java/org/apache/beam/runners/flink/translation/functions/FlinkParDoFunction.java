/*
 * Copyright (C) 2017 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.beam.runners.flink.translation.functions;

import com.google.common.collect.ImmutableMap;
import java.util.stream.Stream;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.flink.execution.EnvironmentSession;
import org.apache.beam.runners.flink.execution.SdkHarnessManager;
import org.apache.beam.runners.fnexecution.artifact.ArtifactSource;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * Portable ParDo function.
 * @param <InputT> Input element type
 * @param <OutputT> Output element type
 */
public class FlinkParDoFunction<InputT, OutputT> extends RichMapPartitionFunction<
    WindowedValue<InputT>, WindowedValue<OutputT>> {

  private final String jobId;
  private final RunnerApi.Environment environment;

  private EnvironmentSession session;
  private SdkHarnessClient client;
  private SdkHarnessClient.BundleProcessor<InputT> processor;

  public FlinkParDoFunction(String jobId, RunnerApi.Environment environment) {
    this.jobId = jobId;
    this.environment = environment;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    SdkHarnessManager harnessManager = new SdkHarnessManager() {
      @Override
      public EnvironmentSession getSession(String jobId, RunnerApi.Environment environment, ArtifactSource artifactSource) {
        // TODO
        return null;
      }
    };
    ArtifactSource artifactSource = new ArtifactSource() {
      @Override
      public ArtifactApi.Manifest getManifest() {
        // TODO
        return null;
      }

      @Override
      public Stream<ArtifactApi.ArtifactChunk> getArtifact(String name) {
        // TODO
        return null;
      }
    };
    session = harnessManager.getSession(jobId, environment, artifactSource);
    client = session.getClient();
    // TODO
    BeamFnApi.ProcessBundleDescriptor pbd = BeamFnApi.ProcessBundleDescriptor.newBuilder()
        .setId("pado-id-here")
        .build();
    SdkHarnessClient.RemoteInputDestination<WindowedValue<InputT>> remote = null;
    processor = client.getProcessor(pbd, remote);
  }

  @Override
  public void mapPartition(Iterable<WindowedValue<InputT>> iterable,
      Collector<WindowedValue<OutputT>> collector) {
    SdkHarnessClient.ActiveBundle<InputT> bundle = processor.newBundle(ImmutableMap.of());
    try (CloseableFnDataReceiver<WindowedValue<InputT>> inputReceiver = bundle.getInputReceiver()) {
      for (WindowedValue<InputT> input : iterable) {
        System.out.println("Input value: " + input);
        inputReceiver.accept(input);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws Exception {
    client.close();
    session.close();
  }
}
