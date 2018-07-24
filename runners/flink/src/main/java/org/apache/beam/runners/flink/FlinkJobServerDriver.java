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
package org.apache.beam.runners.flink;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.ServerFactory;
import org.apache.beam.runners.fnexecution.artifact.BeamFileSystemArtifactStagingService;
import org.apache.beam.runners.fnexecution.jobsubmission.InMemoryJobService;
import org.apache.beam.runners.fnexecution.jobsubmission.JobInvoker;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.kohsuke.args4j.CmdLineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Driver program that starts a job server. */
public class FlinkJobServerDriver implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkJobServerDriver.class);

  private final ListeningExecutorService executor;
  private final FlinkDriverOptions runnerOptions;
  private final ServerFactory serverFactory;

  public static void main(String[] args) throws IOException {
    FlinkDriverOptions runnerOptions =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(FlinkDriverOptions.class);
    FileSystems.setDefaultPipelineOptions(runnerOptions);
    FlinkJobServerDriver driver = fromConfig(runnerOptions);
    driver.run();
  }

  private static void printUsage(CmdLineParser parser) {
    System.err.println(
        String.format("Usage: java %s arguments...", FlinkJobServerDriver.class.getSimpleName()));
    parser.printUsage(System.err);
    System.err.println();
  }

  public static FlinkJobServerDriver fromConfig(FlinkDriverOptions runnerOptions) {
    ThreadFactory threadFactory =
        new ThreadFactoryBuilder().setNameFormat("flink-runner-job-server").setDaemon(true).build();
    ListeningExecutorService executor =
        MoreExecutors.listeningDecorator(Executors.newCachedThreadPool(threadFactory));
    ServerFactory serverFactory = ServerFactory.createDefault();
    return create(runnerOptions, executor, serverFactory);
  }

  public static FlinkJobServerDriver create(
      FlinkDriverOptions runnerOptions,
      ListeningExecutorService executor,
      ServerFactory serverFactory) {
    return new FlinkJobServerDriver(runnerOptions, executor, serverFactory);
  }

  private FlinkJobServerDriver(
      FlinkDriverOptions runnerOptions,
      ListeningExecutorService executor,
      ServerFactory serverFactory) {
    this.runnerOptions = runnerOptions;
    this.executor = executor;
    this.serverFactory = serverFactory;
  }

  @Override
  public void run() {
    try {
      GrpcFnServer<InMemoryJobService> server = createJobServer();
      server.getServer().awaitTermination();
    } catch (InterruptedException e) {
      LOG.warn("Job server interrupted", e);
    } catch (Exception e) {
      LOG.warn("Exception during job server creation", e);
    }
  }

  private GrpcFnServer<InMemoryJobService> createJobServer() throws IOException {
    InMemoryJobService service = createJobService();
    Endpoints.ApiServiceDescriptor descriptor =
        Endpoints.ApiServiceDescriptor.newBuilder().setUrl(runnerOptions.getJobHost()).build();
    return GrpcFnServer.create(service, descriptor, serverFactory);
  }

  private InMemoryJobService createJobService() throws IOException {
    GrpcFnServer<BeamFileSystemArtifactStagingService> artifactStagingService =
        createArtifactStagingService();
    JobInvoker invoker = createJobInvoker();
    return InMemoryJobService.create(
        artifactStagingService.getApiServiceDescriptor(),
        (String session) -> {
          try {
            return BeamFileSystemArtifactStagingService.generateStagingSessionToken(
                session, runnerOptions.getArtifactsDir());
          } catch (Exception exn) {
            throw new RuntimeException(exn);
          }
        },
        invoker);
  }

  private GrpcFnServer<BeamFileSystemArtifactStagingService> createArtifactStagingService()
      throws IOException {
    BeamFileSystemArtifactStagingService service = new BeamFileSystemArtifactStagingService();
    return GrpcFnServer.allocatePortAndCreateFor(service, serverFactory);
  }

  private JobInvoker createJobInvoker() throws IOException {
    return FlinkJobInvoker.create(executor, runnerOptions.getFlinkMasterUrl());
  }
}
