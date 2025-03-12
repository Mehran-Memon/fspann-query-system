package com.fspann.data;

import java.io.BufferedReader;
import java.io.FileReader;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.io.InputStreamReader;
import java.io.FileInputStream;

public class GowallaCheckinReader {

    /**
     * Reads Gowalla check-ins from a text file with format:
     * [userId]  [timestamp]        [lat]       [lon]         [locationId]
     * 196514    2010-07-24T13:45:06Z 53.3648119 -2.272365693  145064
     *
     * @param filePath path to the .txt file (uncompressed)
     * @return a list of Checkin objects
     */
    public static List<Checkin> readCheckins(String filePath) throws Exception {
        List<Checkin> checkins = new ArrayList<>();
        try (GZIPInputStream gis = new GZIPInputStream(new FileInputStream(filePath));
        	     BufferedReader br = new BufferedReader(new InputStreamReader(gis))) {
        	String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\\s+");
                if (parts.length < 5) {
                    continue; // skip malformed lines
                }
                
                int userId = Integer.parseInt(parts[0]);
                
                // parse timestamp string, e.g. "2010-07-24T13:45:06Z"
                Instant time = Instant.parse(parts[1]); 
                
                double latitude = Double.parseDouble(parts[2]);
                double longitude = Double.parseDouble(parts[3]);
                int locationId = Integer.parseInt(parts[4]);
                
                checkins.add(new Checkin(userId, time, latitude, longitude, locationId));
            }
        }
        return checkins;
    }
}
