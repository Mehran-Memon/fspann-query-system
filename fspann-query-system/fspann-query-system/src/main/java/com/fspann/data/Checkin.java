package com.fspann.data;

import java.time.Instant;

public class Checkin {
    private int userId;
    private Instant timestamp;  // or LocalDateTime, depending on your preference
    private double latitude;
    private double longitude;
    private int locationId;

    public Checkin(int userId, Instant timestamp, double latitude, double longitude, int locationId) {
        this.userId = userId;
        this.timestamp = timestamp;
        this.latitude = latitude;
        this.longitude = longitude;
        this.locationId = locationId;
    }

    public int getUserId() {
        return userId;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public double getLatitude() {
        return latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public int getLocationId() {
        return locationId;
    }

    @Override
    public String toString() {
        return "Checkin{" +
               "userId=" + userId +
               ", timestamp=" + timestamp +
               ", latitude=" + latitude +
               ", longitude=" + longitude +
               ", locationId=" + locationId +
               '}';
    }
    
    
}
