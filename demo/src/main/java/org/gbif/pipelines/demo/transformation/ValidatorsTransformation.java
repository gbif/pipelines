package org.gbif.pipelines.demo.transformation;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;

/**
 * Abstract class for validators, validator should have the same type for input and output
 */
public abstract class ValidatorsTransformation<T> extends PTransform<PCollection<T>, PCollectionTuple> {}
