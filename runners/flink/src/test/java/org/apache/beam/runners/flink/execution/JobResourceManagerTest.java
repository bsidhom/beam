package org.apache.beam.runners.flink.execution;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

import org.apache.beam.fn.harness.fn.ThrowingConsumer;
import org.apache.beam.model.fnexecution.v1.ProvisionApi;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.artifact.ArtifactSource;
import org.apache.beam.runners.fnexecution.control.ControlClientPool;
import org.apache.beam.runners.fnexecution.control.FnApiControlClientPoolService;
import org.apache.beam.runners.fnexecution.control.InstructionRequestHandler;
import org.apache.beam.runners.fnexecution.data.GrpcDataService;
import org.apache.beam.runners.fnexecution.environment.EnvironmentManager;
import org.apache.beam.runners.fnexecution.environment.RemoteEnvironment;
import org.apache.beam.runners.fnexecution.state.GrpcStateService;
import org.apache.beam.sdk.util.ThrowingSupplier;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Tests for {@link JobResourceManager}.
 */
public class JobResourceManagerTest {

  private ProvisionApi.ProvisionInfo jobInfo = ProvisionApi.ProvisionInfo.newBuilder().build();
  private RunnerApi.Environment environment = RunnerApi.Environment.newBuilder().build();
  private @Mock ArtifactSource artifactSource;
  private @Mock JobResourceFactory jobResourceFactory;
  private @Mock EnvironmentManager containerManager;
  private @Mock RemoteEnvironment remoteEnvironment;
  private @Mock GrpcFnServer<FnApiControlClientPoolService> controlServer;
  private @Mock GrpcFnServer<GrpcDataService> dataServer;
  private @Mock GrpcFnServer<GrpcStateService> stateServer;
  private @Mock GrpcDataService dataService;
  private @Mock InstructionRequestHandler requestHandler;
  private @Mock ControlClientPool clientPool;

  private ThrowingSupplier<InstructionRequestHandler> requestHandlerSupplier = () -> requestHandler;
  private ThrowingConsumer<InstructionRequestHandler> requestHandlerConsumer = (client) -> {};

  private JobResourceManager manager;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    when(clientPool.getSink()).thenReturn(requestHandlerConsumer);
    when(clientPool.getSource()).thenReturn(requestHandlerSupplier);
    when(jobResourceFactory.dataService()).thenReturn(dataServer);
    when(jobResourceFactory.stateService()).thenReturn(stateServer);
    when(jobResourceFactory.controlService(requestHandlerConsumer)).thenReturn(controlServer);
    when(dataServer.getService()).thenReturn(dataService);
    when(jobResourceFactory.containerManager(
        artifactSource,
        jobInfo,
        controlServer,
        requestHandlerSupplier))
            .thenReturn(containerManager);
    when(containerManager.getEnvironment(environment)).thenReturn(remoteEnvironment);

    manager = JobResourceManager.create(clientPool, jobInfo, artifactSource,
        jobResourceFactory);
  }

  @Test
  public void testGetEnvironmentSucceeds() throws Exception {
    EnvironmentSession session = manager.getSession(environment);
    assertNotNull(session);
  }

}
