package au.org.ala.pipelines.interpreters;

import static au.org.ala.pipelines.transforms.SeedbankTransform.SEED_BANK_ROW_TYPE;

import au.org.ala.term.SeedbankTerm;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import lombok.Builder;
import org.gbif.common.parsers.core.OccurrenceParseResult;
import org.gbif.common.parsers.date.DateComponentOrdering;
import org.gbif.common.parsers.date.MultiinputTemporalParser;
import org.gbif.common.parsers.date.TemporalAccessorUtils;
import org.gbif.pipelines.core.functions.SerializableFunction;
import org.gbif.pipelines.core.interpreters.ExtensionInterpretation;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.SeedbankRecord;

public class SeedbankInterpreter {

  private final MultiinputTemporalParser temporalParser;
  private final SerializableFunction<String, String> preprocessDateFn;

  public final ExtensionInterpretation.TargetHandler<SeedbankRecord> HANDLER =
      ExtensionInterpretation.extension(SEED_BANK_ROW_TYPE)
          .to(SeedbankRecord::new)
          .map(
              SeedbankTerm.adjustedGerminationPercentage,
              SeedbankInterpreter::setAdjustedGermination)
          .map(SeedbankTerm.darkHours, SeedbankInterpreter::setDarkHours)
          .map(SeedbankTerm.dayTemperatureInCelsius, SeedbankInterpreter::setDayTemp)
          .map(
              SeedbankTerm.nightTemperatureInCelsius,
              SeedbankInterpreter::setNightTemperatureInCelsius)
          .map(SeedbankTerm.lightHours, SeedbankInterpreter::setLightHours)
          .map(SeedbankTerm.numberFull, SeedbankInterpreter::setNumberFull)
          .map(SeedbankTerm.numberGerminated, SeedbankInterpreter::setNumberGerminated)
          .map(SeedbankTerm.numberPlantsSampled, SeedbankInterpreter::setNumberPlantsSampled)
          .map(SeedbankTerm.quantityCount, SeedbankInterpreter::setQuantityCount)
          .map(SeedbankTerm.quantityInGrams, SeedbankInterpreter::setQuantityInGrams)
          .map(SeedbankTerm.testLengthInDays, SeedbankInterpreter::setTestLengthInDays)
          .map(SeedbankTerm.thousandSeedWeight, SeedbankInterpreter::setThousandSeedWeight)
          .map(SeedbankTerm.seedPerGram, SeedbankInterpreter::setSeedPerGram)
          .map(SeedbankTerm.purityPercentage, SeedbankInterpreter::setPurityPercentage)
          .map(SeedbankTerm.viabilityPercentage, SeedbankInterpreter::setViabilityPercentage)
          .map(
              SeedbankTerm.storageRelativeHumidityPercentage,
              SeedbankInterpreter::setStorageRelativeHumidityPercentage)
          .map(SeedbankTerm.storageTemperatureInCelsius, SeedbankInterpreter::setStorageTemp)
          .map(SeedbankTerm.germinationRateInDays, SeedbankInterpreter::setGerminateRate)
          .map(SeedbankTerm.numberEmpty, SeedbankInterpreter::setNumberEmpty)
          .map(SeedbankTerm.numberTested, SeedbankInterpreter::setNumberTested)
          .map(SeedbankTerm.dateInStorage, this::interpretDateInStorage)
          .map(SeedbankTerm.dateCollected, this::interpretDateCollected)
          .map(SeedbankTerm.numberNotViable, SeedbankInterpreter::setNumberNotViable)
          .map(SeedbankTerm.testDateStarted, this::interpretTestDateStarted);

  @Builder(buildMethodName = "create")
  private SeedbankInterpreter(
      List<DateComponentOrdering> orderings,
      SerializableFunction<String, String> preprocessDateFn) {
    this.temporalParser = MultiinputTemporalParser.create(orderings);
    this.preprocessDateFn = preprocessDateFn;
  }

  public void interpret(ExtendedRecord er, SeedbankRecord sr) {
    Objects.requireNonNull(er);
    Objects.requireNonNull(sr);
    ExtensionInterpretation.Result<SeedbankRecord> result = HANDLER.convert(er);
  }

  public static void setQuantityInGrams(SeedbankRecord sr, String value) {
    try {
      sr.setQuantityInGrams(Double.parseDouble(value));
    } catch (Exception e) {
      // do nothing
    }
  }

  public static void setQuantityCount(SeedbankRecord sr, String value) {
    try {
      sr.setQuantityCount(Double.parseDouble(value));
    } catch (Exception e) {
      // do nothing
    }
  }

  public static void setAdjustedGermination(SeedbankRecord sr, String value) {
    try {
      sr.setAdjustedGerminationPercentage(validOrNullPercentage(Double.parseDouble(value)));
    } catch (Exception e) {
      // do nothing
    }
  }

  public static void setDarkHours(SeedbankRecord sr, String value) {
    try {
      sr.setDarkHours(Double.parseDouble(value));
    } catch (Exception e) {
      // do nothing
    }
  }

  public static void setDayTemp(SeedbankRecord sr, String value) {
    try {
      sr.setDayTemperatureInCelsius(Double.parseDouble(value));
    } catch (Exception e) {
      // do nothing
    }
  }

  public static void setLightHours(SeedbankRecord sr, String value) {
    try {
      sr.setLightHours(Double.parseDouble(value));
    } catch (Exception e) {
      // do nothing
    }
  }

  public static void setNightTemperatureInCelsius(SeedbankRecord sr, String value) {
    try {
      sr.setNightTemperatureInCelsius(Double.parseDouble(value));
    } catch (Exception e) {
      // do nothing
    }
  }

  public static void setNumberFull(SeedbankRecord sr, String value) {
    try {
      sr.setNumberFull(Double.parseDouble(value));
    } catch (Exception e) {
      // do nothing
    }
  }

  public static void setNumberGerminated(SeedbankRecord sr, String value) {
    try {
      sr.setNumberGerminated(Double.parseDouble(value));
    } catch (Exception e) {
      // do nothing
    }
  }

  public static void setNumberPlantsSampled(SeedbankRecord sr, String value) {
    try {
      sr.setNumberPlantsSampled(Double.parseDouble(value));
    } catch (Exception e) {
      // do nothing
    }
  }

  public static void setTestLengthInDays(SeedbankRecord sr, String value) {
    try {
      sr.setTestLengthInDays(Double.parseDouble(value));
    } catch (Exception e) {
      // do nothing
    }
  }

  public static void setThousandSeedWeight(SeedbankRecord sr, String value) {
    try {
      sr.setThousandSeedWeight(Double.parseDouble(value));
    } catch (Exception e) {
      // do nothing
    }
  }

  public static void setSeedPerGram(SeedbankRecord sr, String value) {
    try {
      sr.setSeedPerGram(Double.parseDouble(value));
    } catch (Exception e) {
      // do nothing
    }
  }

  public static void setPurityPercentage(SeedbankRecord sr, String value) {
    try {
      sr.setPurityPercentage(validOrNullPercentage(Double.parseDouble(value)));
    } catch (Exception e) {
      // do nothing
    }
  }

  public static void setViabilityPercentage(SeedbankRecord sr, String value) {
    try {
      sr.setViabilityPercentage(validOrNullPercentage(Double.parseDouble(value)));
    } catch (Exception e) {
      // do nothing
    }
  }

  public static void setStorageRelativeHumidityPercentage(SeedbankRecord sr, String value) {
    try {
      sr.setStorageRelativeHumidityPercentage(validOrNullPercentage(Double.parseDouble(value)));
    } catch (Exception e) {
      // do nothing
    }
  }

  public static void setStorageTemp(SeedbankRecord sr, String value) {
    try {
      sr.setStorageTemperatureInCelsius(Double.parseDouble(value));
    } catch (Exception e) {
      // do nothing
    }
  }

  public static void setGerminateRate(SeedbankRecord sr, String value) {
    try {
      sr.setGerminationRateInDays(Double.parseDouble(value));
    } catch (Exception e) {
      // do nothing
    }
  }

  public static void setNumberEmpty(SeedbankRecord sr, String value) {
    try {
      sr.setNumberEmpty(Double.parseDouble(value));
    } catch (Exception e) {
      // do nothing
    }
  }

  public static void setNumberTested(SeedbankRecord sr, String value) {
    try {
      sr.setNumberTested(Double.parseDouble(value));
    } catch (Exception e) {
      // do nothing
    }
  }

  public static void setNumberNotViable(SeedbankRecord sr, String value) {
    try {
      sr.setNumberNotViable(Long.parseLong(value));
    } catch (Exception e) {
      // do nothing
    }
  }

  public void interpretDateCollected(SeedbankRecord sr, String dateCollected) {
    if (dateCollected != null) {
      String normalised =
          Optional.ofNullable(preprocessDateFn)
              .map(x -> x.apply(dateCollected))
              .orElse(dateCollected);
      OccurrenceParseResult<TemporalAccessor> parsed = temporalParser.parseRecordedDate(normalised);
      if (parsed.isSuccessful()) {
        Optional.ofNullable(parsed.getPayload())
            .map(ta -> TemporalAccessorUtils.toDate(ta).getTime())
            .ifPresent(sr::setDateCollected);
      }
    }
  }

  public void interpretDateInStorage(SeedbankRecord sr, String dateInStorage) {
    if (dateInStorage != null) {
      String normalised =
          Optional.ofNullable(preprocessDateFn)
              .map(x -> x.apply(dateInStorage))
              .orElse(dateInStorage);
      OccurrenceParseResult<TemporalAccessor> parsed = temporalParser.parseRecordedDate(normalised);
      if (parsed.isSuccessful()) {
        Optional.ofNullable(parsed.getPayload())
            .map(ta -> TemporalAccessorUtils.toDate(ta).getTime())
            .ifPresent(sr::setDateInStorage);
      }
    }
  }

  public void interpretTestDateStarted(SeedbankRecord sr, String testDateStarted) {
    if (testDateStarted != null) {
      String normalizedDate =
          Optional.ofNullable(preprocessDateFn)
              .map(x -> x.apply(testDateStarted))
              .orElse(testDateStarted);
      OccurrenceParseResult<TemporalAccessor> parsed =
          temporalParser.parseRecordedDate(normalizedDate);
      if (parsed.isSuccessful()) {
        Optional.ofNullable(parsed.getPayload())
            .map(ta -> TemporalAccessorUtils.toDate(ta).getTime())
            .ifPresent(sr::setTestDateStarted);
      }
    }
  }

  private static Double validOrNullPercentage(Double percentage) {
    if (percentage > 100 || percentage < 0) return null;
    return percentage;
  }
}
