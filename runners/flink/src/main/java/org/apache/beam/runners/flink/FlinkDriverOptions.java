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

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;

/** Runner options for {@link FlinkJobServerDriver}. */
public interface FlinkDriverOptions extends PipelineOptions {
  @Description("The job server host string.")
  @Required
  String getJobHost();

  void setJobHost(String jobHost);

  @Description("The location to store staged artifact files.")
  @Required
  String getArtifactsDir();

  void setArtifactsDir(String artifactsDir);

  @Description("Flink master url to submit job. Use '[auto]' for the Flink defaults.")
  @Default.String("[auto]")
  String getFlinkMasterUrl();

  void setFlinkMasterUrl(String flinkMasterUrl);
}
