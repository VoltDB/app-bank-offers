package procedures;

import java.util.*;

public class PerformanceTimer {
    private long startTime;
    private String name;
    private boolean isRunning = false;
    private Map<String,Long> timings = new LinkedHashMap<String,Long>();

    public PerformanceTimer() {
    }

    public void start(String activityName) {
        if (isRunning)
            stop();
        startTime = System.nanoTime();
        name = activityName;
        isRunning = true;
    }
        
    public void stop() {
        long elapsedTimeInMicros = (System.nanoTime() - startTime)/1000;
        timings.put(name,elapsedTimeInMicros);
        isRunning = false;
    }
        
    public Map<String,Long> getResults() {
        if (isRunning)
            stop();
        return timings;
    }
}
