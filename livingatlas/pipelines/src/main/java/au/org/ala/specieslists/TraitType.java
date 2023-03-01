package au.org.ala.specieslists;

/** Used for AusTraits trait types */
public enum TraitType {
  FIRE_RESPONSE("fire_response"),
  POST_FIRE_RECRUITMENT("post_fire_recruitment");

  public final String label;

  private TraitType(String label) {
    this.label = label;
  }

  public static TraitType valueOfLabel(String label) {
    for (TraitType e : values()) {
      if (e.label.equals(label)) {
        return e;
      }
    }
    return null;
  }
}
