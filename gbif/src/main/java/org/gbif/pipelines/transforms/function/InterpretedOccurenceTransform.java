package org.gbif.pipelines.transforms.function;

import org.gbif.dwca.avro.Event;
import org.gbif.dwca.avro.ExtendedOccurence;
import org.gbif.dwca.avro.Location;
import org.gbif.pipelines.transforms.ExtendedOccurenceTransform;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;

/**
 * Convert's Beam's represented Joined PCollection to an Interpreted Occurrence
 */
public class InterpretedOccurenceTransform extends DoFn<KV<String, CoGbkResult>, ExtendedOccurence> {

  private final ExtendedOccurenceTransform extendedOccurenceTransform;

  public InterpretedOccurenceTransform(ExtendedOccurenceTransform extendedOccurenceTransform) {
    this.extendedOccurenceTransform = extendedOccurenceTransform;
  }

  @ProcessElement
  public void processElement(ProcessContext ctx) {
    KV<String, CoGbkResult> result = ctx.element();
    //get temporal and spatial info from the joined beam collection with tags

    Event evt = result.getValue().getOnly(extendedOccurenceTransform.getTemporalTag());
    Location loc = result.getValue().getOnly(extendedOccurenceTransform.getSpatialTag());

    //create final interpreted record with values from the interpreted category
    ExtendedOccurence occurence = ExtendedOccurence.newBuilder()
      .setOccurrenceID(result.getKey())
      .setBasisOfRecord(evt.getBasisOfRecord())
      .setDay(evt.getDay())
      .setMonth(evt.getMonth())
      .setYear(evt.getYear())
      .setEventDate(evt.getEventDate())
      .setDecimalLatitude(loc.getDecimalLatitude())
      .setDecimalLongitude(loc.getDecimalLongitude())
      .setCountry(loc.getCountry())
      .setCountryCode(loc.getCountryCode())
      .setContinent(loc.getContinent())
      .build();

    ctx.output(occurence);
  }
}
