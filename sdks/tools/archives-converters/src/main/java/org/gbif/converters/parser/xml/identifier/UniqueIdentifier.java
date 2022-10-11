package org.gbif.converters.parser.xml.identifier;

import java.util.UUID;

/** This interface is meant to be used for classes that can uniquely identify occurrence records. */
public interface UniqueIdentifier {

  /**
   * Every unique identifier must be scoped within the dataset key.
   *
   * @return the UUID of the dataset for the identified occurrence
   */
  UUID getDatasetKey();

  /**
   * A string that uniquely identifies the occurrence (e.g. a concatenation of it's unique fields)
   * that could be used as a key for maps or databases.
   *
   * @return a unique String representing the unique identifier
   */
  String getUniqueString();

  /**
   * A string that uniquely identifies the occurrence within a dataset (e.g. a concatenation of it's
   * unique fields) but does not incorporate the datasetKey.
   *
   * @return a unique String representing the unique identifier within the dataset (but does not
   *     include the datasetKey)
   */
  String getUnscopedUniqueString();
}
