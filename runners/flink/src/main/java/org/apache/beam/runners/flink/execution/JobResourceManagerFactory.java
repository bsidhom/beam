package org.apache.beam.runners.flink.execution;

import java.util.concurrent.ExecutorService;
import org.apache.beam.model.fnexecution.v1.ProvisionApi;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.ServerFactory;
import org.apache.beam.runners.fnexecution.artifact.ArtifactSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Factory which creates {@link JobResourceManager JobResourceManagers}.
 */
public class JobResourceManagerFactory {
  private static final Logger LOG = LoggerFactory.getLogger(JobResourceManagerFactory.class);

  public static JobResourceManagerFactory create() {
    return new JobResourceManagerFactory();
  }

  private JobResourceManagerFactory() {
  }

  public JobResourceManager create(
      ProvisionApi.ProvisionInfo jobInfo,
      RunnerApi.Environment environment,
      ArtifactSource artifactSource,
      ServerFactory serverFactory,
      ExecutorService executor) {
    JobResourceFactory jobResourceFactory = JobResourceFactory.create(serverFactory, executor);
    LOG.info("Creating job resource manager for Job {}.", jobInfo.getJobName());
    return JobResourceManager
        .create(jobInfo, environment, artifactSource, jobResourceFactory);
  }
}
