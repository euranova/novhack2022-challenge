package eu.euranova.novhack;

import java.sql.Time;
import java.sql.Timestamp;

public class WeightedAvgAccumulator {
    public double avg = 0L;
    public int count = 0;
    public Timestamp startBrake = null;
    public long brakeTime = 0;
    public boolean flag = false;
}