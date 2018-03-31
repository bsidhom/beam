package org.apache.beam.runners.flink.execution;

import org.apache.beam.model.fnexecution.v1.ProvisionApi;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.artifact.ArtifactSource;
import org.apache.beam.runners.fnexecution.control.ControlClientPool;
import org.apache.beam.runners.fnexecution.control.FnApiControlClientPoolService;
import org.apache.beam.runners.fnexecution.environment.EnvironmentManager;
import org.apache.beam.runners.fnexecution.environment.RemoteEnvironment;
import org.apache.beam.runners.fnexecution.state.GrpcStateService;

/**
 * A class that manages the long-lived resources of an individual job.
 *
 * <p>Only one harness environment is currently supported per job.
 */
public class JobResourceManager implements AutoCloseable {

  /** Create a new JobResourceManager. */
  public static JobResourceManager create(
      ControlClientPool controlClientPool,
      ProvisionApi.ProvisionInfo jobInfo,
      ArtifactSource artifactSource,
      JobResourceFactory jobResourceFactory) throws Exception {
    GrpcFnServer<FnApiControlClientPoolService> controlServer =
        jobResourceFactory.controlService(controlClientPool.getSink());
    EnvironmentManager environmentManager = jobResourceFactory.containerManager(
        artifactSource, jobInfo, controlServer, controlClientPool.getSource());
    GrpcFnServer<GrpcStateService> stateService = jobResourceFactory.stateService();
    return new JobResourceManager(artifactSource, jobResourceFactory, environmentManager,
        stateService, controlServer);
  }

  // job resources
  private final ArtifactSource artifactSource;
  private final JobResourceFactory jobResourceFactory;

  // environment resources (will eventually need to support multiple environments)
  private final EnvironmentManager environmentManager;
  private final GrpcFnServer<GrpcStateService> stateService;
  private final GrpcFnServer<FnApiControlClientPoolService> controlServer;

  private JobResourceManager (
      ArtifactSource artifactSource,
      JobResourceFactory jobResourceFactory,
      EnvironmentManager environmentManager,
      GrpcFnServer<GrpcStateService> stateService,
      GrpcFnServer<FnApiControlClientPoolService> controlServer) {
    this.artifactSource = artifactSource;
    this.jobResourceFactory = jobResourceFactory;
    this.environmentManager = environmentManager;
    this.stateService = stateService;
    this.controlServer = controlServer;
  }

  /** Get a new environment session using the manager's resources. */
  public EnvironmentSession getSession(RunnerApi.Environment environment) throws Exception {
    // TODO: Decide who is responsible for service lifecycles.
    RemoteEnvironment remoteEnvironment = environmentManager.getEnvironment(environment);
    return JobResourceEnvironmentSession.create(
        jobResourceFactory::dataService,
        remoteEnvironment.getEnvironment(),
        remoteEnvironment.getInstructionRequestHandler(),
        artifactSource,
        stateService.getService(),
        stateService.getApiServiceDescriptor());
  }

  @Override
  public void close() throws Exception {
    if (controlServer != null) {
      controlServer.close();
    }
    if (stateService != null) {
      stateService.close();
    }
  }

}
