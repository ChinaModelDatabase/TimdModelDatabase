package com.tmdb.example;

import java.time.Instant;

/**
 * Sensor Data Model
 */
public class SensorData {
    private Instant timestamp;
    private long deviceId;
    private double temperature;
    private double humidity;
    private String status;

    public SensorData() {
    }

    public SensorData(Instant timestamp, long deviceId, double temperature, double humidity, String status) {
        this.timestamp = timestamp;
        this.deviceId = deviceId;
        this.temperature = temperature;
        this.humidity = humidity;
        this.status = status;
    }

    public Instant getTimestamp() { return timestamp; }
    public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }
    public long getDeviceId() { return deviceId; }
    public void setDeviceId(long deviceId) { this.deviceId = deviceId; }
    public double getTemperature() { return temperature; }
    public void setTemperature(double temperature) { this.temperature = temperature; }
    public double getHumidity() { return humidity; }
    public void setHumidity(double humidity) { this.humidity = humidity; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    @Override
    public String toString() {
        return String.format("SensorData{ts=%s, deviceId=%d, temp=%.1f, humidity=%.1f, status='%s'}",
                timestamp, deviceId, temperature, humidity, status);
    }
}
