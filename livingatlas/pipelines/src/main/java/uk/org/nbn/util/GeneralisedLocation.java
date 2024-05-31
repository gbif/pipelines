/***************************************************************************
 * Copyright (C) 2010 Atlas of Living Australia
 * All Rights Reserved.
 *
 * The contents of this file are subject to the Mozilla Public
 * License Version 1.1 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at http://www.mozilla.org/MPL/
 *
 * Software distributed under the License is distributed on an "AS
 * IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * rights and limitations under the License.
 ***************************************************************************/
package uk.org.nbn.util;


import org.apache.commons.lang.StringUtils;

import java.math.BigDecimal;


public class GeneralisedLocation {
    private final String originalLatitude;
    private final String originalLongitude;
    private String generalisedLatitude;
    private String generalisedLongitude;
    private int generalisationInMetres;

    public GeneralisedLocation(String latitude, String longitude, int generalisationInMetres) {
        this.generalisationInMetres = generalisationInMetres;
        this.originalLatitude = latitude;
        this.originalLongitude = longitude;
        generaliseCoordinates();
    }

    public String getGeneralisedLatitude() {
        return generalisedLatitude;
    }

    public String getGeneralisedLongitude() {
        return generalisedLongitude;
    }

    private void generaliseCoordinates() {

        if (StringUtils.isBlank(this.originalLatitude) || StringUtils.isBlank(this.originalLongitude)) {
            // location not provided
            generalisedLatitude = originalLatitude;
            generalisedLongitude = originalLongitude;
        }
        else if (this.generalisationInMetres == 100000 || this.generalisationInMetres == 50000) {
            generaliseCoordinates(0);

        } else if (this.generalisationInMetres == 5000 || this.generalisationInMetres == 10000) {
            generaliseCoordinates(1);
        } else if (this.generalisationInMetres == 2000 || this.generalisationInMetres == 1000) {
            generaliseCoordinates(2);

        } else if (this.generalisationInMetres == 100) {
            generaliseCoordinates(3);
        } else {
            generalisedLatitude = originalLatitude;
            generalisedLongitude = originalLongitude;
        }
    }


    private void generaliseCoordinates(int decimalPlaces) {
        generalisedLatitude = round(originalLatitude, decimalPlaces);
        generalisedLongitude = round(originalLongitude, decimalPlaces);
    }

    private String round(String number, int decimalPlaces) {
        if (number == null || number.equals("")) {
            return "";
        } else {
            BigDecimal bd = new BigDecimal(number);
            if (bd.scale() > decimalPlaces) {
                return String.format("%." + decimalPlaces + "f", bd);
            } else {
                return number;
            }
        }
    }


}
