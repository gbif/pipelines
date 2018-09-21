package org.gbif.pipelines.core.converters;

import org.gbif.dwc.record.Record;
import org.gbif.dwc.record.StarRecord;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Converters from *.class to {@link ExtendedRecord} */
public class ExtendedRecordConverter {

    //Function that removes all the empty elements of a record
    private static final Function<Record, Map<String, String>> REMOVE_EMPTY_CONTENT =
            record ->
                    record.terms()
                            .stream()
                            .filter(term -> term.qualifiedName() != null && record.value(term) != null)
                            .collect(Collectors.toMap(Term::qualifiedName, record::value, (a, b) -> b));

    private ExtendedRecordConverter() {}

    /** Converts {@link StarRecord} to {@link ExtendedRecord} */
    public static ExtendedRecord from(StarRecord record) {
        Record core = record.core();
        ExtendedRecord.Builder builder = ExtendedRecord.newBuilder().setId(core.id());
        Optional.ofNullable(core.rowType()).ifPresent(x -> builder.setCoreRowType(x.qualifiedName()));
        builder.setCoreTerms(REMOVE_EMPTY_CONTENT.apply(core));
        builder.setExtensions(record.extensions().entrySet().stream().collect(Collectors.toMap(
                entry -> entry.getKey().qualifiedName(),
                entry -> entry.getValue().stream().map(REMOVE_EMPTY_CONTENT).collect(Collectors.toList()))));
        return builder.build();
    }
}