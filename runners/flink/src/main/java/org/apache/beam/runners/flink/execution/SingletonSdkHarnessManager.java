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

package org.apache.beam.runners.flink.execution;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import org.apache.beam.model.fnexecution.v1.ProvisionApi;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.ServerFactory;
import org.apache.beam.runners.fnexecution.artifact.ArtifactSource;

/**
 * A long-lived class to manage SDK harness container instances on behalf of
 * shorter-lived Flink operators.  It is responsible for initializing and storing
 * job resources for retrieval.
 */
public class SingletonSdkHarnessManager implements  SdkHarnessManager {
  private static class LazyHolder {
    static final SingletonSdkHarnessManager INSTANCE = SingletonSdkHarnessManager.create();
  }

  private static final String DOCKER_FOR_MAC_HOST = "docker.for.mac.host.internal";

  /** Get the singleton instance of the harness manager. */
  public static SingletonSdkHarnessManager getInstance() {
    return LazyHolder.INSTANCE;
  }

  private final ServerFactory serverFactory;
  private final ExecutorService executorService;
  private final JobResourceManagerFactory jobResourceManagerFactory;

  private final Object jobResourceManagerLock = new Object();
  @GuardedBy("jobResourceManagerLock")
  @Nullable
  private JobResourceManager jobResourceManager;

  private static SingletonSdkHarnessManager create() {
    ThreadFactory threadFactory =
        new ThreadFactoryBuilder().setThreadFactory(MoreExecutors.platformThreadFactory())
            .setNameFormat("flink-runner-sdk-harness")
            .setDaemon(true)
            .build();
    ServerFactory serverFactory = getServerFactory();
    return new SingletonSdkHarnessManager(
        serverFactory,
        Executors.newCachedThreadPool(threadFactory),
        JobResourceManagerFactory.create()
    );
  }

  @VisibleForTesting
  SingletonSdkHarnessManager(
      ServerFactory serverFactory,
      ExecutorService executorService,
      JobResourceManagerFactory jobResourceManagerFactory) {
    this.serverFactory = serverFactory;
    this.executorService = executorService;
    this.jobResourceManagerFactory = jobResourceManagerFactory;
  }

  @Override
  public EnvironmentSession getSession(
      ProvisionApi.ProvisionInfo jobInfo,
      RunnerApi.Environment environment,
      ArtifactSource artifactSource) throws Exception {

    synchronized (jobResourceManagerLock) {
      if (jobResourceManager == null) {
        jobResourceManager =
            jobResourceManagerFactory.create(
                jobInfo,
                artifactSource,
                serverFactory,
                executorService);
      }
    }

    // TODO: Don't spin up a docker container for every stage but
    // reuse one container per TaskManager/JVM.
    // TODO: Ensure JobResourceManager is thread-safe.
    return jobResourceManager.getSession(environment);
  }

  private static ServerFactory getServerFactory() {
    switch (getPlatform()) {
      case LINUX:
        return ServerFactory.createDefault();
      case MAC:
        return ServerFactory.createWithUrlFactory((host, port) ->
          HostAndPort.fromParts(DOCKER_FOR_MAC_HOST, port).toString());
      default:
        throw new RuntimeException("Platform not supported");
    }
  }

  private enum Platform {
    MAC,
    LINUX,
    OTHER,
  }

  private static Platform getPlatform() {
    String osName = System.getProperty("os.name").toLowerCase();
    // TODO: Make this more robust?
    if (osName.startsWith("mac")) {
      return Platform.MAC;
    } else if (osName.startsWith("linux")) {
      return Platform.LINUX;
    }
    return Platform.OTHER;
  }

}
