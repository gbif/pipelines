package org.gbif.pipelines.core.interpreters.specific;

/**
 * Which identifier, if any, is forced to be used and looked up first when assigning GBIF ids,
 * regardless of the result of the identifier validation (the tripletValid and occurrenceIdValid
 * flags). Set per dataset with machine tags, see IdentifiersPipeline.
 *
 * This mainly of use with BioCASe datasets.
 */
public enum IdPriority {

  /** Use the validation flags, the occurrenceID is looked up before the triplet. */
  DEFAULT,

  /** The occurrenceID is used regardless of its flag, and looked up before the triplet. */
  OCCURRENCE_ID,

  /** The triplet is used regardless of its flag, and looked up before the occurrenceID. */
  TRIPLET;

  /**
   * @return whether the triplets are used, true if forced
   */
  public boolean useTriplet(boolean isTripletValid) {
    return isTripletValid || this == TRIPLET;
  }

  /**
   * @return whether the occurrenceIDs are used, true if forced
   */
  public boolean useOccurrenceId(boolean isOccurrenceIdValid) {
    return isOccurrenceIdValid || this == OCCURRENCE_ID;
  }

  /**
   * @return whether the triplet is looked up before the occurrenceID
   */
  public boolean isTripletFirst() {
    return this == TRIPLET;
  }

  /**
   * @return the priority, {@link #DEFAULT} if null
   */
  public static IdPriority orDefault(IdPriority idPriority) {
    return idPriority == null ? DEFAULT : idPriority;
  }
}
