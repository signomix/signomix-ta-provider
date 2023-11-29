/*
 * Copyright 2021 Grzegorz Skorupa <g.skorupa at gmail.com>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.signomix.provider.formatter.kanarek;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jboss.logging.Logger;

import com.cedarsoftware.util.io.JsonWriter;
import com.signomix.common.iot.ChannelData;

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class KanarekFormatter {

    Logger logger = Logger.getLogger(KanarekFormatter.class.getName());

    private Map args;

    public KanarekFormatter() {
        args = new HashMap();
    }

    /**
     * Translates response result to JSON representation
     *
     * @param prettyPrint pretty print JSON or not
     * @param result      response result
     * @return response as JSON String
     */
    public String format(boolean prettyPrint, List data) {
        //logger.info("formatting Kanarek response");
        args.clear();
        args.put(JsonWriter.PRETTY_PRINT, prettyPrint);
        args.put(JsonWriter.DATE_FORMAT, "dd/MMM/yyyy:kk:mm:ss Z");
        args.put(JsonWriter.SKIP_NULL_FIELDS, true);
        args.put(JsonWriter.TYPE, false);
        KanarekDto kdto = new KanarekDto();
        ChannelData cdata;
        //String ownerName = result.getHeaders().getFirst("X-Group-Name");
        //String groupHref = result.getHeaders().getFirst("X-Group-Dashboard-Href");
        String ownerName = "Otwarta Sieć Rzeczy - udostępnione dla Kanarka";
        String groupHref =  "https://signomix.com/gt/asmp";

        String channelName;
        for (int i = 0; i < data.size(); i++) {
            try {
                KanarekStationDto kStation = new KanarekStationDto();
                if (null != ownerName && !ownerName.isBlank()) {
                    kStation.owner = ownerName;
                }
                if (null != groupHref) {
                    kStation.href = groupHref;
                }
                if (((List) data.get(i)).size() < 1) {
                    continue;
                }
                List channels = (List) ((List) data.get(i)).get(0);
                if (channels.size() < 1) {
                    continue;
                }
                cdata = (ChannelData) channels.get(0);
                if(cdata == null){
                    continue;
                }
                try {
                    kStation.id = Long.parseLong((cdata).getDeviceEUI(), 16);
                } catch (Exception e) {
                    logger.warn("malformed station EUI: " + e.getMessage());
                }
                kStation.name = cdata.getDeviceEUI();
                for (int j = 0; j < channels.size(); j++) {
                    // kStation.href="";
                    cdata = (ChannelData) channels.get(j);
                    if(cdata == null){
                        continue;
                    }
                    channelName = cdata.getName();
                    switch (channelName.toUpperCase()) {
                        case "LATITUDE":
                        case "GPS_LATITUDE":
                        case "LAT":
                            kStation.lat = cdata.getValue();
                            break;
                        case "LONGITUDE":
                        case "GPS_LONGITUDE":
                        case "LON":
                            kStation.lon = cdata.getValue();
                            break;
                        default:
                            KanarekValue kv = new KanarekValue(channelName, cdata.getValue(), cdata.getTimestamp());
                            if (null != kv.type) {
                                kStation.values.add(kv);
                            }
                    }
                }
                if (null != kStation.id && null != kStation.lat && null != kStation.lon) {
                    kdto.stations.add(kStation);
                } else {
                    logger.warn("Unable to add Kanarek Station data - requred values not set.");
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        return JsonWriter.objectToJson(kdto, args) + "\r\n";
    }

}
