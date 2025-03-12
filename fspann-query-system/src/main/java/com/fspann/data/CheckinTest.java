package com.fspann.data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CheckinTest {
    public static void main(String[] args) {
        try {
            String filePath = "C:\\Users\\Mehran Memon\\eclipse-workspace\\fspann-query-system\\fspann-query-system\\data\\loc-gowalla_totalCheckins.txt.gz";
            List<Checkin> checkins = GowallaCheckinReader.readCheckins(filePath);
            System.out.println("Total checkins read: " + checkins.size());

            for (int i = 0; i < Math.min(0, checkins.size()); i++) {
                System.out.println(checkins.get(i));
            }
            
            Map<Integer, Integer> userCountMap = new HashMap<>();
            for (Checkin c : checkins) {
                userCountMap.put(c.getUserId(), userCountMap.getOrDefault(c.getUserId(), 0) + 1);
            }

            // Print how many distinct users
            System.out.println("Number of distinct users: " + userCountMap.size());

            // Print a few user counts
            userCountMap.entrySet().stream()
                        .limit(10)
                        .forEach(e -> System.out.println("User " + e.getKey() + " => " + e.getValue() + " check-ins"));

        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
    
}
