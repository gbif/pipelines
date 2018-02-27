package org.gbif.pipelines.transform.function;

import org.gbif.dwca.avro.Event;
import org.gbif.dwca.avro.ExtendedOccurrence;
import org.gbif.dwca.avro.Location;
import org.gbif.pipelines.transform.ExtendedOccurrenceTransform;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;

/**
 * Convert's Beam's represented Joined PCollection to an Interpreted Occurrence
 */
public class InterpretedOccurrenceTransform extends DoFn<KV<String, CoGbkResult>, ExtendedOccurrence> {

  private final ExtendedOccurrenceTransform extendedOccurrenceTransform;

  public InterpretedOccurrenceTransform(ExtendedOccurrenceTransform extendedOccurrenceTransform) {
    this.extendedOccurrenceTransform = extendedOccurrenceTransform;
  }

  @ProcessElement
  public void processElement(ProcessContext ctx) {
    KV<String, CoGbkResult> result = ctx.element();
    //get temporal and spatial info from the joined beam collection with tags

    Event event = result.getValue().getOnly(extendedOccurrenceTransform.getTemporalTag());
    Location location = result.getValue().getOnly(extendedOccurrenceTransform.getSpatialTag());

    //create final interpreted record with values from the interpreted category
    ExtendedOccurrence occurrence = ExtendedOccurrence.newBuilder()
      .setOccurrenceID(result.getKey())
      .setBasisOfRecord(event.getBasisOfRecord())
      .setDay(event.getDay())
      .setMonth(event.getMonth())
      .setYear(event.getYear())
      .setEventDate(event.getEventDate())
      .setDecimalLatitude(location.getDecimalLatitude())
      .setDecimalLongitude(location.getDecimalLongitude())
      .setCountry(location.getCountry())
      .setCountryCode(location.getCountryCode())
      .setContinent(location.getContinent())
      .build();

    ctx.output(occurrence);
  }
}
