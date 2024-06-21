package uk.org.nbn.pipelines.interpreters;

import static org.gbif.pipelines.core.utils.ModelUtils.addIssue;

import au.org.ala.pipelines.vocabulary.Vocab;
import com.google.common.base.Strings;
import java.util.Optional;
import java.util.function.BiConsumer;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.License;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import uk.org.nbn.pipelines.vocabulary.NBNOccurrenceIssue;

/**
 * Extensions to {@link org.gbif.pipelines.core.interpreters.core.BasicInterpreter} to support NBN
 * living atlas.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class NBNBasicInterpreter {

  public static void interpretBasisOfRecord(ExtendedRecord er, BasicRecord br) {
    if (BasisOfRecord.OCCURRENCE.name().equals(br.getBasisOfRecord())) {
      br.setBasisOfRecord(BasisOfRecord.HUMAN_OBSERVATION.name());
    }
  }

  public static BiConsumer<ExtendedRecord, BasicRecord> interpretIdentificationVerificationStatus(
      Vocab identificationVerificationStatusVocab) {
    return (er, br) -> {
      String value =
          er.getCoreTerms().get(DwcTerm.identificationVerificationStatus.qualifiedName());
      if (!Strings.isNullOrEmpty(value)) {
        Optional<String> identificationVerificationStatus =
            identificationVerificationStatusVocab.matchTerm(value);
        if (identificationVerificationStatus.isPresent()) {
          br.setIdentificationVerificationStatus(identificationVerificationStatus.get());
        } else {
          br.setIdentificationVerificationStatus(
              identificationVerificationStatusVocab.matchTerm("Unconfirmed").orElse("Unconfirmed"));
          addIssue(br, NBNOccurrenceIssue.UNRECOGNISED_IDENTIFICATIONVERIFICATIONSTATUS.name());
        }
      } else {
        addIssue(br, NBNOccurrenceIssue.MISSING_IDENTIFICATIONVERIFICATIONSTATUS.name());
      }
    };
  }

  public static BiConsumer<ExtendedRecord, BasicRecord> interpretLicense(Vocab licenseVocab) {
    return (er, br) -> {
      // there is no DwcTerm.license, there is only DcTerm.license http://purl.org/dc/terms/license
      String value = er.getCoreTerms().get("http://rs.tdwg.org/dwc/terms/license");
      String license;
      if (!Strings.isNullOrEmpty(value)) {
        license =
            licenseVocab
                .matchTerm(er.getCoreTerms().get("http://rs.tdwg.org/dwc/terms/license"))
                .orElse(License.UNSPECIFIED.name());
      } else {
        license = License.UNSPECIFIED.name();
      }
      br.setLicense(license);
    };
  }

  //  public static void interpretLicense(ExtendedRecord er, BasicRecord br) {
  //    LicenseParser parser = LicenseParser.getInstance();
  //    Optional<String> value = extractOptValue(er, DcTerm.license);
  //    if (value.isPresent()) {
  //      br.setLicense(parser.matchLicense(value.get()));
  //    } else {
  //      br.setLicense(License.UNSPECIFIED.name());
  //    }
  //  }
}
