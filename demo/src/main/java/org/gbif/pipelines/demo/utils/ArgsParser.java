package org.gbif.pipelines.demo.utils;

/**
 * Parser for command line arguments.
 */
public final class ArgsParser {

  /**
   * Gets a argument in short form (starts with only one dash).
   *
   * @param args     from command line
   * @param argName  name of the argument to look for
   * @param required true if the argument is required, false otherwise.
   *
   * @return the value of the argument
   */
  public static String getArgShortForm(String[] args, String argName, boolean required) {
    for (String arg : args) {
      if (arg.startsWith("-" + argName + "=")) {
        return arg.split("=")[1];
      }
    }

    if (required) {
      throw new IllegalArgumentException(argName + " argument is required. Usage -" + argName + "=<value>");
    }

    return null;
  }

}
