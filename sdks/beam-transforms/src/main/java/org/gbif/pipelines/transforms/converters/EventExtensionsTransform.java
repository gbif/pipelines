package org.gbif.pipelines.transforms.converters;

import static org.gbif.pipelines.core.utils.ModelUtils.hasExtension;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;

/**
 * Some datasets do some hacks to link extensions to an occurrence extension by using the
 * occurrenceID.
 *
 * <p>In these cases, we have to remove the records that have an occurrenceID to unlink them from
 * the event core. At the moment this is done for the eMoF and DNA extensions.
 */
@Slf4j
public class EventExtensionsTransform extends DoFn<ExtendedRecord, ExtendedRecord> {

  public static SingleOutput<ExtendedRecord, ExtendedRecord> create() {
    return ParDo.of(new EventExtensionsTransform());
  }

  @ProcessElement
  public void processElement(@Element ExtendedRecord er, OutputReceiver<ExtendedRecord> out) {
    convert(er, out::output);
  }

  public void convert(ExtendedRecord er, Consumer<ExtendedRecord> resultConsumer) {

    Arrays.asList(Extension.EXTENDED_MEASUREMENT_OR_FACT, Extension.DNA_DERIVED_DATA)
        .forEach(
            extension -> {
              if (hasExtension(er, extension.getRowType())) {
                List<Map<String, String>> parsedExtension =
                    er.getExtensions().get(extension.getRowType()).stream()
                        .filter(v -> !v.containsKey(DwcTerm.occurrenceID.qualifiedName()))
                        .collect(Collectors.toList());

                if (parsedExtension.isEmpty()) {
                  er.getExtensions().remove(extension.getRowType());
                } else {
                  er.getExtensions().put(extension.getRowType(), parsedExtension);
                }
              }
            });

    resultConsumer.accept(er);
  }
}
