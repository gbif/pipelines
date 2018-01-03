package org.gbif.pipelines.core.functions.ws.gbif.species;

import java.io.IOException;
import org.gbif.pipelines.core.functions.SerializableFunction;
import org.gbif.pipelines.io.avro.TypedOccurrence;
import retrofit2.Call;
import retrofit2.Response;

/**
 * DEMO IMPLEMENTATION ONLY! Much can be done to improve this... A function to set the nub fields of the interpreted
 * record.
 */
public class NubSpeciesMatchFunction implements SerializableFunction<TypedOccurrence, TypedOccurrence> {

  @Override
  public TypedOccurrence apply(TypedOccurrence s) {

    Call<SpeciesMatchResponseModel> call = SpeciesMatchServiceRest.SINGLE
        .getService()
        .match(s); // i.e. not strict

    Response<SpeciesMatchResponseModel> r;
    try {
      r = call.execute();
    } catch (IOException e) {
      throw new RuntimeException("Unable to run species/match lookup", e);
    }

    if (!r.isSuccessful()) {
      return s;
    }

    SpeciesMatchResponseModel model = r.body();
    if (model != null) {
      TypedOccurrenceMapper.mapFromSpeciesMatch(s, model);
    }

    return s;
  }

}
