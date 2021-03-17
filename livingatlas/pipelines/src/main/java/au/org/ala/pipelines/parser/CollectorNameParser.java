package au.org.ala.pipelines.parser;

import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class CollectorNameParser {
  private static final String NAME_LETTERS = "A-ZÃÃ‹Ã–ÃœÃ„Ã‰ÃˆÄŒÃÃ€Ã†Å’\\p{Lu}";
  private static final String NA = "[nN]/[aA]|\\([\\x00-\\x7F\\s]*?\\)";
  private static final String TITLES =
      "Dr|DR|dr|\\(Professor\\)|Mr|MR|mr|Mrs|mrs|MRS|Ms|ms|MS|Lieutenant";
  private static final String ET_AL = "[eE][tT][. ] ?[aA][Ll][. ]?";
  private static final String INITIALS_REG_EX = "((?:[A-Z][-. ]? ?){0,4})";
  private static final String ORGANISATION_WORDS =
      "collection|Entomology|University|Oceanographic|Indonesia|Division|American|Photographic|SERVICE|Section|Arachnology|Northern|Institute|Ichthyology|AUSTRALIA|Malacology|Institution|Department|Survey|DFO|Society|FNS-\\(SA\\)|Association|Government|COMMISSION|Department|Conservation|Expedition|NPWS-\\(SA\\)|Study Group|DIVISION|Melbourne|ATLAS|summer parties|Macquarie Island|NSW|Australian|Museum|Herpetology|ORNITHOLOGICAL|ASSOCIATION|SURVEY|Fisheries|Queensland|Griffith Npws|NCS-\\(SA\\)|UNIVERSITY|SCIENTIFIC|Ornithologists|Bird Observation|CMAR|Kangaroo Management Program";
  private static final String[] SURNAME_PREFIXES =
      new String[] {
        "ben", "da", "Da", "Dal", "de", "De", "del", "Del", "den", "der", "Di", "du", "e", "la",
        "La", "Le", "Mc", "San", "St", "Ste", "van", "Van", "Vander", "vel", "von", "Von"
      };
  private static final String SURNAME_PREFIX_REGEX =
      "((?:(?:" + String.join("|", SURNAME_PREFIXES) + ")(?:[. ]|$)){0,2})";
  private static final String INITIALS_SURNAME_PATTERN =
      "(?:(?:"
          + TITLES
          + ")(?:[. ]|$))?"
          + INITIALS_REG_EX
          + "[. ]([\\p{Lu}\\p{Ll}'-]*) ?(?:(?:"
          + TITLES
          + ")(?:[. ]|$)?)?(?:"
          + ET_AL
          + ")?";
  private static final String SURNAME_FIRSTNAE_PATTERN =
      "\"?([\\p{Lu}'-]*) ((?:[A-Z][-. ] ?){0,4}) ?([\\p{Lu}\\p{Ll}']*)(?: " + NA + ")?\"?";
  private static final String SURNAME_PUNC_FIRSTNAME_PATTERN =
      "\"?"
          + SURNAME_PREFIX_REGEX
          + "([\\p{Lu}\\p{Ll}'-]*) ?[,] ?(?:(?:"
          + TITLES
          + ")(?:[. ]|$))? ?((?:[A-Z][-. ] ?){0,4}) ?"
          + SURNAME_PREFIX_REGEX
          + "([\\p{Lu}\\p{Ll}']*)? ?([\\p{Lu}\\p{Ll}']{3,})? ?((?:[A-Z][. ]? ?){0,4})"
          + SURNAME_PREFIX_REGEX
          + "(?: "
          + NA
          + ")?\"?";
  private static final String SINGLE_NAME_PATTERN =
      "(?:(?:" + TITLES + ")(?:[. ]|$))?([\\p{Lu}\\p{Ll}']*)";
  private static final String ORGANISATION_PATTERN =
      "((?:.*?)?(?:" + ORGANISATION_WORDS + ")(?:.*)?)";
  private static final String AND = "AND|and|And|&";
  private static final String COLLECTOR_DELIM = ";|\"\"|\\|| - ";
  private static final String COMMA_LIST = ",|&";
  private static final String AND_NAME_LIST_PATTERN =
      "((?:[A-Z][. ] ?){0,3})(["
          + NAME_LETTERS
          + "][\\p{Ll}-']*)? ?(["
          + NAME_LETTERS
          + "][\\p{Ll}\\p{Lu}'-]*)? ?"
          + "(?:"
          + AND
          + ") ?((?:[A-Z][. ] ?){0,3})(["
          + NAME_LETTERS
          + "][\\p{Ll}'-]*)? ?(["
          + NAME_LETTERS
          + "][\\p{Ll}\\p{Lu}'-]*)?";
  private static final String FIRSTNAME_SURNAME_PATTERN =
      "(["
          + NAME_LETTERS
          + "][\\p{Ll}']*) ((?:[A-Z][. ] ?){0,4}) ?([\\p{Lu}\\p{Ll}'-]*)? ?(?:"
          + NA
          + ")?";
  private static final String[] UNKNOWN =
      new String[] {
        "\"?ANON  N/A\"?",
        "\"NOT ENTERED[ ]*-[ ]*SEE ORIGINAL DATA[ ]*-[ ]*\"",
        "\\[unknown\\]",
        "Anon.",
        "No data",
        "Unknown",
        "Anonymous",
        "\\?"
      };
  private static final String UNKNOWN_PATTERN = String.join("|", UNKNOWN);
  private static final String EMAIL_PATTERN =
      "(?:(?:\\r\\n)?[ \\t])*(?:(?:(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\"(?:[^\\\"\\r\\\\]|\\\\.|(?:(?:\\r\\n)?[ \\t]))*\"(?:(?:\\r\\n)?[ \\t])*)(?:\\.(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\"(?:[^\\\"\\r\\\\]|\\\\.|(?:(?:\\r\\n)?[ \\t]))*\"(?:(?:\\r\\n)?[ \\t])*))*@(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*)(?:\\.(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*))*|(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\"(?:[^\\\"\\r\\\\]|\\\\.|(?:(?:\\r\\n)?[ \\t]))*\"(?:(?:\\r\\n)?[ \\t])*)*\\<(?:(?:\\r\\n)?[ \\t])*(?:@(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*)(?:\\.(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*))*(?:,@(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*)(?:\\.(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*))*)*:(?:(?:\\r\\n)?[ \\t])*)?(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\"(?:[^\\\"\\r\\\\]|\\\\.|(?:(?:\\r\\n)?[ \\t]))*\"(?:(?:\\r\\n)?[ \\t])*)(?:\\.(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\"(?:[^\\\"\\r\\\\]|\\\\.|(?:(?:\\r\\n)?[ \\t]))*\"(?:(?:\\r\\n)?[ \\t])*))*@(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*)(?:\\.(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*))*\\>(?:(?:\\r\\n)?[ \\t])*)|(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\"(?:[^\\\"\\r\\\\]|\\\\.|(?:(?:\\r\\n)?[ \\t]))*\"(?:(?:\\r\\n)?[ \\t])*)*:(?:(?:\\r\\n)?[ \\t])*(?:(?:(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\"(?:[^\\\"\\r\\\\]|\\\\.|(?:(?:\\r\\n)?[ \\t]))*\"(?:(?:\\r\\n)?[ \\t])*)(?:\\.(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\"(?:[^\\\"\\r\\\\]|\\\\.|(?:(?:\\r\\n)?[ \\t]))*\"(?:(?:\\r\\n)?[ \\t])*))*@(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*)(?:\\.(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*))*|(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\"(?:[^\\\"\\r\\\\]|\\\\.|(?:(?:\\r\\n)?[ \\t]))*\"(?:(?:\\r\\n)?[ \\t])*)*\\<(?:(?:\\r\\n)?[ \\t])*(?:@(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*)(?:\\.(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*))*(?:,@(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*)(?:\\.(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*))*)*:(?:(?:\\r\\n)?[ \\t])*)?(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\"(?:[^\\\"\\r\\\\]|\\\\.|(?:(?:\\r\\n)?[ \\t]))*\"(?:(?:\\r\\n)?[ \\t])*)(?:\\.(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\"(?:[^\\\"\\r\\\\]|\\\\.|(?:(?:\\r\\n)?[ \\t]))*\"(?:(?:\\r\\n)?[ \\t])*))*@(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*)(?:\\.(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*))*\\>(?:(?:\\r\\n)?[ \\t])*)(?:,\\s*(?:(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\"(?:[^\\\"\\r\\\\]|\\\\.|(?:(?:\\r\\n)?[ \\t]))*\"(?:(?:\\r\\n)?[ \\t])*)(?:\\.(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\"(?:[^\\\"\\r\\\\]|\\\\.|(?:(?:\\r\\n)?[ \\t]))*\"(?:(?:\\r\\n)?[ \\t])*))*@(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*)(?:\\.(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*))*|(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\"(?:[^\\\"\\r\\\\]|\\\\.|(?:(?:\\r\\n)?[ \\t]))*\"(?:(?:\\r\\n)?[ \\t])*)*\\<(?:(?:\\r\\n)?[ \\t])*(?:@(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*)(?:\\.(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*))*(?:,@(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*)(?:\\.(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*))*)*:(?:(?:\\r\\n)?[ \\t])*)?(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\"(?:[^\\\"\\r\\\\]|\\\\.|(?:(?:\\r\\n)?[ \\t]))*\"(?:(?:\\r\\n)?[ \\t])*)(?:\\.(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\"(?:[^\\\"\\r\\\\]|\\\\.|(?:(?:\\r\\n)?[ \\t]))*\"(?:(?:\\r\\n)?[ \\t])*))*@(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*)(?:\\.(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*))*\\>(?:(?:\\r\\n)?[ \\t])*))*)?;\\s*)";

  private static final Pattern INIT_SURNAME_P = Pattern.compile(INITIALS_SURNAME_PATTERN);
  private static final Pattern ORG_P = Pattern.compile(ORGANISATION_PATTERN);
  private static final Pattern SINGLE_NAME_P = Pattern.compile(SINGLE_NAME_PATTERN);
  private static final Pattern SURNAME_FIRSTNAME_P = Pattern.compile(SURNAME_FIRSTNAE_PATTERN);
  private static final Pattern SURNAME_PUNC_FIRSTNAME_P =
      Pattern.compile(SURNAME_PUNC_FIRSTNAME_PATTERN);
  private static final Pattern FIRSTNAME_SURNAME_PATTERN_P =
      Pattern.compile(FIRSTNAME_SURNAME_PATTERN);
  private static final Pattern AND_NAME_LIST_PATTERN_P = Pattern.compile(AND_NAME_LIST_PATTERN);

  private static final Pattern CONTAINS_NUMBER = Pattern.compile("[0-9]{1,}");

  public static String[] parseList(String source) {

    // if it contains numbers, its is likely to be an ID  - avoid parsing
    if (CONTAINS_NUMBER.matcher(source).find()) {
      return new String[] {source};
    }

    // pattern 1
    if (source.matches(AND_NAME_LIST_PATTERN)) {
      // initials1, firstName, secondName, initials2, thirdName, forthName
      Matcher m = AND_NAME_LIST_PATTERN_P.matcher(source);
      if (m.find() && m.groupCount() == 6) {
        String initials1 = m.group(1);
        String firstName = m.group(2);
        String secondName = m.group(3);
        String initials2 = m.group(4);
        String thirdName = m.group(5);
        String forthName = m.group(6);
        if (Strings.isNullOrEmpty(secondName)) {
          if (Strings.isNullOrEmpty(forthName) && Strings.isNullOrEmpty(initials1)) {
            // we have 2 surnames
            String name1 = generateName(null, firstName, initials1, null, null);
            String name2 = generateName(null, thirdName, initials2, null, null);
            return new String[] {name1, name2};
          } else {
            // we have 2 people who share the same surname
            if (!Strings.isNullOrEmpty(initials1)
                && !Strings.isNullOrEmpty(firstName)
                && !Strings.isNullOrEmpty(thirdName)) {
              String name1 = generateName(null, firstName, initials1, null, null);
              String name2 = generateName(null, thirdName, initials2, null, null);
              return new String[] {name1, name2};
            } else if (!Strings.isNullOrEmpty(initials1) && !Strings.isNullOrEmpty(initials2)) {
              String name1 = generateName(null, thirdName, initials1, null, null);
              String name2 = generateName(null, thirdName, initials2, null, null);
              return new String[] {name1, name2};
            } else {
              String name1 = generateName(firstName, forthName, initials1, null, null);
              String name2 = generateName(thirdName, forthName, initials2, null, null);
              return new String[] {name1, name2};
            }
          }
        } else {
          if (Strings.isNullOrEmpty(forthName)) {
            // first person has 2 names second has a surname
            String name1 = generateName(firstName, firstName, initials1, null, null);
            String name2 = generateName(null, thirdName, initials2, null, null);
            return new String[] {name1, name2};
          } else {
            String name1 = generateName(firstName, secondName, initials1, null, null);
            String name2 = generateName(thirdName, forthName, initials2, null, null);
            return new String[] {name1, name2};
          }
        }
      }
    } else if (source.matches(UNKNOWN_PATTERN)) {
      return new String[] {"UNKNOWN OR ANONYMOUS"};
    } else {
      String[] list = source.split(COLLECTOR_DELIM);
      List<String> outputs = new ArrayList<>();
      if (list.length > 1) {
        for (String s : list) {
          String name = parse(s.trim());
          if (!Strings.isNullOrEmpty(name)) {
            outputs.add(name);
          }
        }
        return outputs.toArray(new String[0]);
      } else {
        // Always treated as a single name first
        String res = parse(source);
        if (!Strings.isNullOrEmpty(res)) {
          return new String[] {res};
        } else {
          // check to see if it contains a comma delimited list - this needs to be done outside the
          // other items due to mixed meaning of comma
          String[] names = source.split(COMMA_LIST);
          if (names.length > 1) {
            for (String s : names) {
              String name = parse(s.trim());
              if (!Strings.isNullOrEmpty(name)) {
                outputs.add(name);
              }
            }
          }
        }
      }
      return outputs.toArray(new String[0]);
    }
    return null;
  }

  public static String parse(String source) {

    // if it contains numbers, its is likely to be an ID  - avoid parsing
    if (CONTAINS_NUMBER.matcher(source).find()) {
      return source;
    }

    if (source.matches(UNKNOWN_PATTERN)) {
      log.debug(source + " UNKNOWN PATTERN");
      return "UNKNOWN OR ANONYMOUS";
    }
    if (source.matches(ORGANISATION_PATTERN)) {
      log.debug(source + ": ORGANISATION_PATTERN");
      Matcher m = ORG_P.matcher(source);
      if (m.find() && m.groupCount() == 1) {
        return m.group(1);
      }
    }

    if (source.matches(EMAIL_PATTERN)) {
      log.debug(source + ": EMAIL_PATTERN");
      return source;
    }

    if (source.matches(SINGLE_NAME_PATTERN)) {
      log.debug(source + ": SINGLE_NAME_PATTERN");
      Matcher m = SINGLE_NAME_P.matcher(source);
      if (m.find() && m.groupCount() == 1) {
        String surname = m.group(1);
        return generateName(null, surname, null, null, null);
      }
    }

    // Dr NL Kirby
    if (source.matches(INITIALS_SURNAME_PATTERN)) {
      log.debug(source + ": INITIALS_SURNAME_PATTERN");
      Matcher m = INIT_SURNAME_P.matcher(source);
      if (m.find() && m.groupCount() == 2) {
        String inits = m.group(1);
        String surname = m.group(2);
        return generateName(null, surname, inits, null, null);
      }
    }
    // Simon Starr
    if (source.matches(FIRSTNAME_SURNAME_PATTERN)) {
      log.debug(source + ": FIRSTNAME_SURNAME_PATTERN");
      Matcher m = FIRSTNAME_SURNAME_PATTERN_P.matcher(source);
      if (m.find() && m.groupCount() == 3) {
        String first = m.group(1);
        String inits = m.group(2);
        String surname = m.group(3);
        return generateName(first, surname, inits, null, null);
      }
    }

    if (source.matches(SURNAME_FIRSTNAE_PATTERN)) {
      log.debug(source + ": SURNAME_FIRSTNAE_PATTERN");
      Matcher m = SURNAME_FIRSTNAME_P.matcher(source);
      if (m.find() && m.groupCount() == 3) {
        String surname = m.group(1);
        String inits = m.group(2);
        String first = m.group(3);
        return generateName(first, surname, inits, null, null);
      }
    }

    if (source.matches(SURNAME_PUNC_FIRSTNAME_PATTERN)) {
      log.debug(source + ": SURNAME_PUNC_FIRSTNAME_PATTERN");
      Matcher m = SURNAME_PUNC_FIRSTNAME_P.matcher(source);
      if (m.find() && m.groupCount() == 8) {
        String prefix = m.group(1);
        String surname = m.group(2);
        String initials = m.group(3);
        String prefix2 = m.group(4);
        String firstname = m.group(5);
        String middlename = m.group(6);
        String initials2 = m.group(7);
        String prefix3 = m.group(8);

        String finalPrefix;
        if (!Strings.isNullOrEmpty(prefix3)) {
          finalPrefix = prefix3;
        } else if (!Strings.isNullOrEmpty(prefix2)) {
          finalPrefix = prefix2;
        } else {
          finalPrefix = prefix;
        }

        return generateName(
            firstname,
            surname,
            Strings.isNullOrEmpty(initials) ? initials2 : initials,
            middlename,
            finalPrefix);
      }
    }

    return null;
  }

  public static String generateName(
      String firstName, String surname, String initials, String middlename, String surnamePrefix) {
    StringBuilder name = new StringBuilder();
    if (!Strings.isNullOrEmpty(surnamePrefix)) {
      name.append(surnamePrefix.trim()).append(" ");
    }
    if (!Strings.isNullOrEmpty(surname)) {
      name.append(
          org.apache.commons.lang3.text.WordUtils.capitalize(surname.toLowerCase(), '-', '\''));
    }
    if (!Strings.isNullOrEmpty(initials)) {
      name.append(", ");
      // R.J-P. will be converted to R.J.-.P.
      String newinit = initials.trim().replaceAll("[^\\p{Lu}\\p{Ll}-]", "");
      char[] inits = newinit.toCharArray();
      for (char init : inits) {
        name.append(init).append(".");
      }
      // R.J.-.P. will be converted R.J-P.
      name = new StringBuilder(name.toString().replaceAll("\\.-\\.", "-"));
    }

    if (!Strings.isNullOrEmpty(firstName)) {
      if (!Strings.isNullOrEmpty(initials)) {
        name.append(" ")
            .append(org.apache.commons.lang3.StringUtils.capitalize(firstName.toLowerCase()));
      } else {
        name.append(", ")
            .append(org.apache.commons.lang3.StringUtils.capitalize(firstName.toLowerCase()));
      }

      if (!Strings.isNullOrEmpty(middlename)) {
        name.append(" ").append(org.apache.commons.lang3.text.WordUtils.capitalize(middlename));
      }
    }
    return name.toString().trim();
  }
}
