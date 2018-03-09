package org.apache.beam.runners.flink.execution;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import org.apache.beam.model.fnexecution.v1.ProvisionApi.ProvisionInfo;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.ServerFactory;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;
import org.apache.beam.runners.fnexecution.artifact.ArtifactSource;
import org.apache.beam.runners.fnexecution.artifact.GrpcArtifactProxyService;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClientControlService;
import org.apache.beam.runners.fnexecution.data.GrpcDataService;
import org.apache.beam.runners.fnexecution.environment.DockerWrapper;
import org.apache.beam.runners.fnexecution.environment.EnvironmentManager;
import org.apache.beam.runners.fnexecution.environment.SingletonDockerEnvironmentManager;
import org.apache.beam.runners.fnexecution.logging.GrpcLoggingService;
import org.apache.beam.runners.fnexecution.logging.LogWriter;
import org.apache.beam.runners.fnexecution.logging.Slf4jLogWriter;
import org.apache.beam.runners.fnexecution.provisioning.StaticGrpcProvisionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for resources that are managed by {@link JobResourceManager}.
 */
public class JobResourceFactory {
  private static final Logger LOG = LoggerFactory.getLogger(JobResourceFactory.class);

  public static JobResourceFactory create(ServerFactory serverFactory, ExecutorService executor) {
    return new JobResourceFactory(serverFactory, executor);
  }

  private final ServerFactory serverFactory;
  private final ExecutorService executor;

  private JobResourceFactory(ServerFactory serverFactory, ExecutorService executor) {
    this.serverFactory = serverFactory;
    this.executor = executor;
  }

  /** Create a new logging service. */
  private GrpcFnServer<GrpcLoggingService> loggingService() throws IOException {
    LogWriter logWriter = Slf4jLogWriter.getDefault();
    GrpcLoggingService loggingService = GrpcLoggingService.forWriter(logWriter);
    GrpcFnServer<GrpcLoggingService> server =
        GrpcFnServer.allocatePortAndCreateFor(loggingService, serverFactory);
    LOG.info("Created logging server at {}.", server.getApiServiceDescriptor());
    return server;
  }

  /** Create a new artifact retrieval service. */
  private GrpcFnServer<ArtifactRetrievalService> artifactRetrievalService(
      ArtifactSource artifactSource) throws IOException {
    ArtifactRetrievalService retrievalService = GrpcArtifactProxyService.fromSource(artifactSource);
    GrpcFnServer<ArtifactRetrievalService> server =
        GrpcFnServer.allocatePortAndCreateFor(retrievalService, serverFactory);
    LOG.info("Created artifact retrieval service at {}.", server.getApiServiceDescriptor());
    return server;
  }

  /** Create a new provisioning service. */
  private GrpcFnServer<StaticGrpcProvisionService> provisionService(ProvisionInfo jobInfo)
      throws IOException {
    StaticGrpcProvisionService provisioningService = StaticGrpcProvisionService.create(jobInfo);
    GrpcFnServer<StaticGrpcProvisionService> server =
        GrpcFnServer.allocatePortAndCreateFor(provisioningService, serverFactory);
    LOG.info("Created provisioning service at {}.", server.getApiServiceDescriptor());
    return server;
  }

  /** Create a new control service. */
  private GrpcFnServer<SdkHarnessClientControlService> controlService(GrpcDataService dataService)
      throws IOException {
    SdkHarnessClientControlService controlService =
        SdkHarnessClientControlService.create(() -> dataService);
    GrpcFnServer<SdkHarnessClientControlService> server =
        GrpcFnServer.allocatePortAndCreateFor(controlService, serverFactory);
    LOG.info("Created control service at {}.", server.getApiServiceDescriptor());
    return server;
  }

  /** Create a new data service. */
  public GrpcFnServer<GrpcDataService> dataService() throws IOException {
    GrpcDataService dataService = GrpcDataService.create(executor);
    GrpcFnServer<GrpcDataService> server =
        GrpcFnServer.allocatePortAndCreateFor(dataService, serverFactory);
    LOG.info("Created data service at {}.", server.getApiServiceDescriptor());
    return server;
  }

  /** Create a new container manager from artifact source and jobInfo. */
  EnvironmentManager containerManager(
      ArtifactSource artifactSource, ProvisionInfo jobInfo, GrpcDataService dataService)
      throws IOException {
    return SingletonDockerEnvironmentManager.forServices(
        // TODO: Replace hardcoded values with configurable ones
        DockerWrapper.forCommand("docker", Duration.ofSeconds(30)),
        controlService(dataService),
        loggingService(),
        artifactRetrievalService(artifactSource),
        provisionService(jobInfo)
    );
  }

}
