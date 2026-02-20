package org.gbif.pipelines.transforms.converters;

import java.util.HashMap;
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

  private static final List<String> EXTENSIONS_TO_PARSE =
      List.of(
          Extension.EXTENDED_MEASUREMENT_OR_FACT.getRowType(),
          Extension.DNA_DERIVED_DATA.getRowType());

  @ProcessElement
  public void processElement(@Element ExtendedRecord er, OutputReceiver<ExtendedRecord> out) {
    convert(er, out::output);
  }

  public void convert(ExtendedRecord er, Consumer<ExtendedRecord> resultConsumer) {

    ExtendedRecord.Builder builder =
        ExtendedRecord.newBuilder()
            .setId(er.getId())
            .setCoreRowType(er.getCoreRowType())
            .setCoreTerms(er.getCoreTerms());

    Map<String, List<Map<String, String>>> extensions = new HashMap<>();
    er.getExtensions()
        .forEach(
            (extension, values) -> {
              if (EXTENSIONS_TO_PARSE.contains(extension)) {
                List<Map<String, String>> parsedExtension =
                    er.getExtensions().get(extension).stream()
                        .filter(v -> !v.containsKey(DwcTerm.occurrenceID.qualifiedName()))
                        .collect(Collectors.toList());

                if (!parsedExtension.isEmpty()) {
                  extensions.put(extension, parsedExtension);
                }
              } else {
                extensions.put(extension, values);
              }
            });

    builder.setExtensions(extensions);

    resultConsumer.accept(builder.build());
  }
}
