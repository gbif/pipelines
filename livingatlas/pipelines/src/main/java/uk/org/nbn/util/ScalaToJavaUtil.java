package uk.org.nbn.util;

import scala.Option;
public class ScalaToJavaUtil {
    public static String scalaOptionToString(Option<String> option) {
        if (option.isDefined()) {
            return option.get();
        } else {
            return null;
        }
    }

}
