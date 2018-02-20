package org.apache.beam.runners.flink;

import com.google.common.collect.Iterables;
import java.util.Map;
import org.apache.beam.runners.core.construction.PTransformReplacements;
import org.apache.beam.runners.core.construction.ReplacementOutputs;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Flink override for a ParDo with a single input and a single output.
 */
public class SingleInputOutputParDo<InputT , OutputT> extends PTransform<
    PCollection<? extends InputT>, PCollection<OutputT>> {

  public static final String SINGLE_INPUT_OUTPUT_PARDO_URN =
      "beam:transform:flink:single_input_output_pardo:v1";

  private final Coder<OutputT> outputCoder;
  private final PTransform<?, ?> originalTransform;

  private SingleInputOutputParDo(Coder<OutputT> outputCoder, PTransform<?, ?> originalTransform) {
    this.outputCoder = outputCoder;
    this.originalTransform = originalTransform;
  }

  public PTransform<?, ?> getOriginalTransform() {
    return originalTransform;
  }

  @Override
  public PCollection<OutputT> expand(PCollection<? extends InputT> input) {
    return PCollection.createPrimitiveOutputInternal(input.getPipeline(),
        input.getWindowingStrategy(), input.isBounded(),
        outputCoder);
  }

  /** Override factory. */
  public static class Factory<InputT, OutputT> implements PTransformOverrideFactory<
      PCollection<? extends InputT>, PCollection<OutputT>,
      PTransform<PCollection<? extends InputT>, PCollection<OutputT>>> {

    @Override
    public PTransformReplacement<PCollection<? extends InputT>, PCollection<OutputT>>
        getReplacementTransform(
            AppliedPTransform<
                PCollection<? extends InputT>, PCollection<OutputT>,
                PTransform<PCollection<? extends InputT>, PCollection<OutputT>>> transform) {
      @SuppressWarnings("unchecked")
      PCollection<? extends InputT> collection =
          (PCollection<? extends InputT>) Iterables.getOnlyElement(transform.getInputs().values());
      SingleInputOutputParDo parDo = new SingleInputOutputParDo<>(
          PTransformReplacements.getSingletonMainOutput(transform).getCoder(),
          transform.getTransform());
      return PTransformReplacement.of(collection, parDo);
    }

    @Override
    public Map<PValue, ReplacementOutput> mapOutputs(Map<TupleTag<?>, PValue> outputs,
        PCollection<OutputT> newOutput) {
      return ReplacementOutputs.singleton(outputs, newOutput);
    }
  }
}
