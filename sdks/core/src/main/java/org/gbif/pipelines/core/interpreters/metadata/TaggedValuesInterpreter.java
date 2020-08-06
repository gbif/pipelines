package org.gbif.pipelines.core.interpreters.metadata;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MachineTag;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.TaggedValueRecord;

/** Interprets MachineTags that are later stored or interpreted as {@link TaggedValueRecord}. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TaggedValuesInterpreter {

  public static final String PROCESSING_NAMESPACE = "processing.gbif.org";
  public static final String INSTITUTION_TAG_NAME = "institutionCode";
  public static final String COLLECTION_TAG_NAME = "collectionCode";
  public static final String COLLECTION_TO_INSTITUTION_TAG_NAME = "collectionToInstitutionCode";
  public static final String INSTITUTION_TO_COLLECTION_TAG_NAME = "institutionToCollectionCode";

  private static final Set<String> GRSCICOLL_TAG_NAME =
      new HashSet<>(
          Arrays.asList(
              INSTITUTION_TAG_NAME,
              COLLECTION_TAG_NAME,
              COLLECTION_TO_INSTITUTION_TAG_NAME,
              INSTITUTION_TO_COLLECTION_TAG_NAME));

  /** Interpreter of ExtenderRecord and TaggedValueRecord using the MachineTags of MetdataRecord. */
  public static BiConsumer<ExtendedRecord, TaggedValueRecord> interpret(MetadataRecord mr) {
    return (er, tvr) -> processCollectionsData(mr, er, tvr);
  }

  /** Utility method to process machine tags. */
  private static void processMachineTags(
      MetadataRecord metadataRecord,
      String namespace,
      Set<String> tagNames,
      Consumer<MachineTag> consumer) {
    if (Objects.nonNull(metadataRecord.getMachineTags())) {
      metadataRecord.getMachineTags().stream()
          .filter(
              machineTag ->
                  namespace.equals(machineTag.getNamespace())
                      && tagNames.contains(machineTag.getName()))
          .forEach(consumer);
    }
  }

  /** Process collections/grSciColl data. */
  private static void processCollectionsData(
      MetadataRecord mr, ExtendedRecord er, TaggedValueRecord tvr) {
    processMachineTags(
        mr,
        PROCESSING_NAMESPACE,
        GRSCICOLL_TAG_NAME,
        mt -> {
          String[] val = mt.getValue().split(":");
          if (val.length > 1) {
            String grSciCollKey = val[0];
            String code = mt.getValue().substring(val[0].length() + 1);
            if (INSTITUTION_TAG_NAME.equals(mt.getName())) {
              setValueIfTermEquals(
                  er,
                  tvr,
                  DwcTerm.institutionCode,
                  GbifInternalTerm.institutionKey,
                  code,
                  grSciCollKey);
            } else if (COLLECTION_TAG_NAME.equals(mt.getName())) {
              setValueIfTermEquals(
                  er,
                  tvr,
                  DwcTerm.collectionCode,
                  GbifInternalTerm.collectionKey,
                  code,
                  grSciCollKey);
            } else if (COLLECTION_TO_INSTITUTION_TAG_NAME.equals(mt.getName())) {
              setValueIfTermEquals(
                  er,
                  tvr,
                  DwcTerm.collectionCode,
                  GbifInternalTerm.institutionKey,
                  code,
                  grSciCollKey);
            } else if (INSTITUTION_TO_COLLECTION_TAG_NAME.equals(mt.getName())) {
              setValueIfTermEquals(
                  er,
                  tvr,
                  DwcTerm.institutionCode,
                  GbifInternalTerm.collectionKey,
                  code,
                  grSciCollKey);
            }
          }
        });
  }

  /** Sets the tvr.toTerm to valueToAssign if er.fromTerm == valueToCompare. */
  private static void setValueIfTermEquals(
      ExtendedRecord er,
      TaggedValueRecord tvr,
      Term fromTerm,
      Term toTerm,
      String valueToCompare,
      String valueToAssign) {
    Optional.ofNullable(er.getCoreTerms().get(fromTerm.qualifiedName()))
        .ifPresent(
            termValue -> {
              if (valueToCompare.equals(termValue)) {
                tvr.getTaggedValues().put(toTerm.qualifiedName(), valueToAssign);
              }
            });
  }
}
