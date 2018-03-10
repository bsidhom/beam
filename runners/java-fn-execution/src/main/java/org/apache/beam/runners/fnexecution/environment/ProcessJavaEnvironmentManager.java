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
package org.apache.beam.runners.fnexecution.environment;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import org.apache.beam.fn.harness.FnHarness;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;
import org.apache.beam.runners.fnexecution.control.FnApiControlClient;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClientControlService;
import org.apache.beam.runners.fnexecution.data.GrpcDataService;
import org.apache.beam.runners.fnexecution.logging.GrpcLoggingService;
import org.apache.beam.runners.fnexecution.provisioning.StaticGrpcProvisionService;
import org.apache.beam.sdk.fn.channel.ManagedChannelFactory;
import org.apache.beam.sdk.fn.stream.StreamObserverFactory;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

/** Environment manager for in-process Java environments. */
public class ProcessJavaEnvironmentManager implements EnvironmentManager {

  private static final String JAVA_ENV_URL = "gcr.io/google.com/hadoop-cloud-dev/beam/java";

  public static ProcessJavaEnvironmentManager forServices(
      GrpcFnServer<SdkHarnessClientControlService> controlServiceServer,
      GrpcFnServer<GrpcLoggingService> loggingServiceServer,
      GrpcFnServer<ArtifactRetrievalService> retrievalServiceServer,
      GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer,
      GrpcDataService dataService) {
    return new ProcessJavaEnvironmentManager(controlServiceServer, loggingServiceServer,
        retrievalServiceServer, provisioningServiceServer, dataService);
  }

  private final GrpcFnServer<SdkHarnessClientControlService> controlServiceServer;
  private final GrpcFnServer<GrpcLoggingService> loggingServiceServer;
  private final GrpcFnServer<ArtifactRetrievalService> retrievalServiceServer;
  private final GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer;
  private final GrpcDataService dataService;

  private ProcessJavaEnvironmentManager(
      GrpcFnServer<SdkHarnessClientControlService> controlServiceServer,
      GrpcFnServer<GrpcLoggingService> loggingServiceServer,
      GrpcFnServer<ArtifactRetrievalService> retrievalServiceServer,
      GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer,
      GrpcDataService dataService) {
    this.controlServiceServer = controlServiceServer;
    this.dataService = dataService;
    this.loggingServiceServer = loggingServiceServer;
    this.retrievalServiceServer = retrievalServiceServer;
    this.provisioningServiceServer = provisioningServiceServer;
  }

  @Override
  public RemoteEnvironment getEnvironment(Environment environment) throws Exception {
    checkArgument(JAVA_ENV_URL.equals(environment.getUrl()),
        String.format("%s only supports the Java environment",
            ProcessJavaEnvironmentManager.class.getName()));
    SynchronousQueue<FnApiControlClient> clientPool = new SynchronousQueue<>();
    ExecutorService executor = Executors.newCachedThreadPool(
        new ThreadFactoryBuilder().setDaemon(true).build());
    Future<?> ignored = executor.submit(() -> {
      FnHarness.main(
          PipelineOptionsFactory.create(),
          loggingServiceServer.getApiServiceDescriptor(),
          controlServiceServer.getApiServiceDescriptor(),
          new ManagedChannelFactory() {
            @Override
            public ManagedChannel forDescriptor(
                Endpoints.ApiServiceDescriptor apiServiceDescriptor) {
              return InProcessChannelBuilder.forName(apiServiceDescriptor.getUrl()).build();
            }
          },
          StreamObserverFactory.direct());
      return null;
    });
    System.out.println(ignored);
    SdkHarnessClient client = SdkHarnessClient.usingFnApiClient(clientPool.take(),
        dataService);
    return new Handle(environment, client);
  }

  private static class Handle implements RemoteEnvironment {

    private final Environment environment;
    private final SdkHarnessClient client;

    Handle(Environment environment, SdkHarnessClient client) {
      this.environment = environment;
      this.client = client;
    }

    @Override
    public Environment getEnvironment() {
      return environment;
    }

    @Override
    public SdkHarnessClient getClient() {
      return client;
    }

    @Override
    public void close() throws Exception {
      client.close();
      // TODO: Propagate shutdown to servers
    }
  }
}
