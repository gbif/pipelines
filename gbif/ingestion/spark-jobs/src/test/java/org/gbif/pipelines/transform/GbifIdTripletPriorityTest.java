package org.gbif.pipelines.transform;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Identifier.GBIF_ID_ABSENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.gbif.api.model.registry.MachineTag;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.interpreters.specific.GbifIdInterpreter;
import org.gbif.pipelines.core.interpreters.specific.IdPriority;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;
import org.gbif.pipelines.spark.HbaseServer;
import org.gbif.pipelines.spark.IdentifiersPipeline;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/** Lookup of existing GBIF ids when the triplet takes priority over the occurrenceID. */
public class GbifIdTripletPriorityTest {

  @ClassRule public static final HbaseServer HBASE_SERVER = new HbaseServer();

  private static final String DATASET = "3f1d6b0c-2a4e-4f5b-9c8d-7e6f5a4b3c01";
  private static final String TRIPLET = "IC|CC|CN1|null";

  private static HBaseLockingKeyService keygen;
  private static long occKey;
  private static long tripletKey;

  @BeforeClass
  public static void setUp() {
    keygen = new HBaseLockingKeyService(HbaseServer.CFG, HBASE_SERVER.getConnection(), DATASET);
    // the occurrenceID and the triplet of the same record point to different GBIF ids
    occKey = keygen.generateKey(Collections.singleton("occA")).getKey();
    tripletKey = keygen.generateKey(Collections.singleton(TRIPLET)).getKey();
  }

  @Test
  public void occurrenceIdTakesPriorityByDefault() {
    IdentifierRecord ir = interpret(record("occA", "CN1"), true, false);
    assertEquals(String.valueOf(occKey), ir.getInternalId());
  }

  @Test
  public void tripletTakesPriorityWithTag() {
    IdentifierRecord ir = interpret(record("occA", "CN1"), true, true);
    assertEquals(String.valueOf(tripletKey), ir.getInternalId());
    // the occurrenceID keeps its own GBIF id
    assertEquals(occKey, keygen.findKey(Collections.singleton("occA")).get().getKey());
  }

  @Test
  public void tripletPriorityDoesNotFillOccurrenceId() {
    IdentifierRecord ir = interpret(record("occNew", "CN1"), true, true);
    assertEquals(String.valueOf(tripletKey), ir.getInternalId());
    assertFalse(keygen.findKey(Collections.singleton("occNew")).isPresent());
  }

  @Test
  public void defaultFillsOccurrenceIdFromTriplet() {
    IdentifierRecord ir = interpret(record("occFilled", "CN1"), true, false);
    assertEquals(String.valueOf(tripletKey), ir.getInternalId());
    assertEquals(tripletKey, keygen.findKey(Collections.singleton("occFilled")).get().getKey());
  }

  @Test
  public void unknownTripletFallsBackToOccurrenceId() {
    IdentifierRecord ir = interpret(record("occA", "CN-unknown"), true, true);
    assertEquals(String.valueOf(occKey), ir.getInternalId());
  }

  @Test
  public void missingTripletFallsBackToOccurrenceId() {
    IdentifierRecord ir = interpret(record("occA", null), true, true);
    assertEquals(String.valueOf(occKey), ir.getInternalId());
  }

  @Test
  public void nothingFoundIsAbsent() {
    IdentifierRecord ir = interpret(record("occ-unknown", "CN-unknown"), true, true);
    assertNull(ir.getInternalId());
    assertTrue(ir.getIssues().getIssueList().contains(GBIF_ID_ABSENT));
  }

  @Test
  public void forcedOccurrenceIdIsUsedWhenNotValid() {
    // the occurrenceIdValid flag is false, the tag forces it on, the triplet is not used
    IdentifierRecord ir = interpret(record("occA", "CN1"), IdPriority.OCCURRENCE_ID, false, false);
    assertEquals(String.valueOf(occKey), ir.getInternalId());
  }

  @Test
  public void forcedOccurrenceIdTakesPriorityOverValidTriplet() {
    IdentifierRecord ir = interpret(record("occA", "CN1"), IdPriority.OCCURRENCE_ID, true, false);
    assertEquals(String.valueOf(occKey), ir.getInternalId());
  }

  @Test
  public void forcedTripletIsUsedWhenNotValid() {
    // the tripletValid flag is false, the tag forces it on and gives it priority
    IdentifierRecord ir = interpret(record("occA", "CN1"), IdPriority.TRIPLET, false, true);
    assertEquals(String.valueOf(tripletKey), ir.getInternalId());
  }

  @Test
  public void forcedTripletIsUsedWhenOccurrenceIdNotValid() {
    IdentifierRecord ir = interpret(record("occA", "CN1"), IdPriority.TRIPLET, false, false);
    assertEquals(String.valueOf(tripletKey), ir.getInternalId());
  }

  @Test
  public void defaultUsesFlags() {
    assertFalse(IdPriority.DEFAULT.useTriplet(false));
    assertTrue(IdPriority.DEFAULT.useTriplet(true));
    assertFalse(IdPriority.DEFAULT.useOccurrenceId(false));
    assertFalse(IdPriority.TRIPLET.useOccurrenceId(false));
    assertFalse(IdPriority.OCCURRENCE_ID.useTriplet(false));
    assertTrue(IdPriority.TRIPLET.useTriplet(false));
    assertTrue(IdPriority.OCCURRENCE_ID.useOccurrenceId(false));
    assertEquals(IdPriority.DEFAULT, IdPriority.orDefault(null));
  }

  @Test
  public void idPriorityFromTags() {
    assertEquals(
        IdPriority.TRIPLET,
        IdentifiersPipeline.getIdPriority(
            DATASET, Arrays.asList(tag("id_force_triplet", " TRUE "))));
    assertEquals(
        IdPriority.OCCURRENCE_ID,
        IdentifiersPipeline.getIdPriority(
            DATASET, Arrays.asList(tag("other", "true"), tag("id_force_occurrence_id", "true"))));
    assertEquals(
        IdPriority.TRIPLET,
        IdentifiersPipeline.getIdPriority(
            DATASET,
            Arrays.asList(
                tag("id_force_triplet", "true"), tag("id_force_occurrence_id", "false"))));
    assertEquals(
        IdPriority.DEFAULT,
        IdentifiersPipeline.getIdPriority(DATASET, Arrays.asList(tag("id_force_triplet", null))));
    assertEquals(
        IdPriority.DEFAULT,
        IdentifiersPipeline.getIdPriority(DATASET, Arrays.asList(tag("other", "true"))));
    assertEquals(IdPriority.DEFAULT, IdentifiersPipeline.getIdPriority(DATASET, null));
  }

  @Test(expected = IllegalArgumentException.class)
  public void bothTagsIsBadConfiguration() {
    IdentifiersPipeline.getIdPriority(
        DATASET,
        Arrays.asList(tag("id_force_triplet", "true"), tag("id_force_occurrence_id", "true")));
  }

  /** Interprets with the raw validation flags, the interpreter applying the forced id. */
  private static IdentifierRecord interpret(
      ExtendedRecord er, IdPriority idPriority, boolean tripletValid, boolean occurrenceIdValid) {
    IdentifierRecord ir = IdentifierRecord.newBuilder().setId(er.getId()).build();
    GbifIdInterpreter.interpretGbifId(keygen, tripletValid, occurrenceIdValid, false, idPriority)
        .accept(er, ir);
    return ir;
  }

  private static IdentifierRecord interpret(
      ExtendedRecord er, boolean tripletValid, boolean tripletPriority) {
    IdentifierRecord ir = IdentifierRecord.newBuilder().setId(er.getId()).build();
    GbifIdInterpreter.interpretGbifId(
            keygen,
            tripletValid,
            true,
            false,
            tripletPriority ? IdPriority.TRIPLET : IdPriority.DEFAULT)
        .accept(er, ir);
    return ir;
  }

  private static ExtendedRecord record(String occurrenceId, String catalogNumber) {
    Map<String, String> terms = new HashMap<>();
    terms.put(DwcTerm.occurrenceID.qualifiedName(), occurrenceId);
    terms.put(DwcTerm.institutionCode.qualifiedName(), "IC");
    terms.put(DwcTerm.collectionCode.qualifiedName(), "CC");
    if (catalogNumber != null) {
      terms.put(DwcTerm.catalogNumber.qualifiedName(), catalogNumber);
    }
    return ExtendedRecord.newBuilder().setId("1").setCoreTerms(terms).build();
  }

  private static MachineTag tag(String name, String value) {
    MachineTag mt = new MachineTag();
    mt.setNamespace("pipelines.gbif.org");
    mt.setName(name);
    mt.setValue(value);
    return mt;
  }
}
