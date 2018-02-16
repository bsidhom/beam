/*
 * Copyright (C) 2017 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.beam.runners.flink.translation.functions;

import org.apache.beam.sdk.util.WindowedValue;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.util.Collector;

/**
 * Portable ParDo function.
 * @param <InputT> Input element type
 * @param <OutputT> Output element type
 */
public class FlinkParDoFunction<InputT, OutputT> extends RichMapPartitionFunction<
    WindowedValue<InputT>, WindowedValue<OutputT>> {

  public FlinkParDoFunction() {
  }

  @Override
  public void mapPartition(Iterable<WindowedValue<InputT>> iterable,
      Collector<WindowedValue<OutputT>> collector) {
    for (WindowedValue<InputT> element : iterable) {
      System.out.println("Input value: " + element);
    }
  }
}
