package org.gbif.pipelines.core.predicates;

import java.util.function.Predicate;
import java.util.stream.Stream;

import org.gbif.api.vocabulary.Extension;
import org.gbif.pipelines.io.avro.ExtendedRecord;

public class ExtensionPredicates {

  private ExtensionPredicates() {}

  public static Predicate<ExtendedRecord> multimediaPr() {
    return er -> Stream.of(Extension.MULTIMEDIA, Extension.AUDUBON, Extension.IMAGE)
        .sequential()
        .anyMatch(ex -> er.getExtensions().containsKey(ex.getRowType()));
  }

}
