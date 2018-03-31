package org.apache.beam.runners.flink.execution;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutorService;
import org.apache.beam.model.fnexecution.v1.ProvisionApi.ProvisionInfo;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.runners.fnexecution.ServerFactory;
import org.apache.beam.runners.fnexecution.artifact.ArtifactSource;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Tests the functionality of {@link SingletonSdkHarnessManager}.
 */
public class SingletonSdkHarnessManagerTest {

  @Mock ServerFactory serverFactory;
  @Mock ExecutorService executorService;
  @Mock JobResourceManagerFactory jobResourceManagerFactory;
  ProvisionInfo jobInfo = ProvisionInfo.newBuilder().build();
  Environment environment = Environment.newBuilder().build();
  @Mock ArtifactSource artifactSource;
  @Mock JobResourceManager jobResourceManager;
  @Mock EnvironmentSession environmentSession;

  SingletonSdkHarnessManager manager;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    manager =
        new SingletonSdkHarnessManager(serverFactory, executorService, jobResourceManagerFactory);
    when(
        jobResourceManagerFactory.create(
            jobInfo,
            artifactSource,
            serverFactory,
            executorService
        )
    ).thenReturn(jobResourceManager);

    when(jobResourceManager.getSession(environment)).thenReturn(environmentSession);
  }

  @Test
  public void testGetSessionCallsJobResourceManager() throws Exception {
    EnvironmentSession session = manager.getSession(jobInfo, environment, artifactSource);
    verify(jobResourceManager, times(1)).getSession(environment);
    assertEquals(session, environmentSession);
  }

  @Test
  public void testGetSessionCreatesJobResourceManagerOnlyOnFirstRun() throws Exception {
    // get session and check that create was called once
    EnvironmentSession session = manager.getSession(jobInfo, environment, artifactSource);
    verify(jobResourceManagerFactory, times(1)).create(
        jobInfo,
        artifactSource,
        serverFactory,
        executorService);
    // get session again and check that create was still only called once
    session = manager.getSession(jobInfo, environment, artifactSource);
    verify(jobResourceManagerFactory, times(1)).create(
        jobInfo,
        artifactSource,
        serverFactory,
        executorService);
  }
}
