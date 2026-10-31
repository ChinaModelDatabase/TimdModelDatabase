package com.tmdb.example;

/**
 * Sensor Statistics
 */
public class SensorStats {
    private long count;
    private double avgTemperature;
    private double maxTemperature;
    private double minTemperature;
    private double avgHumidity;

    public long getCount() { return count; }
    public void setCount(long count) { this.count = count; }
    public double getAvgTemperature() { return avgTemperature; }
    public void setAvgTemperature(double avgTemperature) { this.avgTemperature = avgTemperature; }
    public double getMaxTemperature() { return maxTemperature; }
    public void setMaxTemperature(double maxTemperature) { this.maxTemperature = maxTemperature; }
    public double getMinTemperature() { return minTemperature; }
    public void setMinTemperature(double minTemperature) { this.minTemperature = minTemperature; }
    public double getAvgHumidity() { return avgHumidity; }
    public void setAvgHumidity(double avgHumidity) { this.avgHumidity = avgHumidity; }

    @Override
    public String toString() {
        return String.format("SensorStats{count=%d, avgTemp=%.2f, maxTemp=%.2f, minTemp=%.2f, avgHumidity=%.2f}",
                count, avgTemperature, maxTemperature, minTemperature, avgHumidity);
    }
}
