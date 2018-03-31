package org.apache.beam.runners.flink.execution;

import java.util.concurrent.ExecutorService;
import org.apache.beam.model.fnexecution.v1.ProvisionApi;
import org.apache.beam.runners.fnexecution.ServerFactory;
import org.apache.beam.runners.fnexecution.artifact.ArtifactSource;
import org.apache.beam.runners.fnexecution.control.QueueControlClientPool;

/**
 * A Factory which creates {@link JobResourceManager JobResourceManagers}.
 */
public class JobResourceManagerFactory {

  public static JobResourceManagerFactory create() {
    return new JobResourceManagerFactory();
  }

  private JobResourceManagerFactory() {}

  public JobResourceManager create(
      ProvisionApi.ProvisionInfo jobInfo,
      ArtifactSource artifactSource,
      ServerFactory serverFactory,
      ExecutorService executor) throws Exception {
    JobResourceFactory jobResourceFactory = JobResourceFactory.create(serverFactory, executor);
    return JobResourceManager
        .create(QueueControlClientPool.createSynchronous(), jobInfo, artifactSource,
            jobResourceFactory);
  }
}
