import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;

public class PerformanceLogger {

    private String loggerName;
    private BigInteger startingTime; //in ms
    private BigInteger endingTime; //in ms
    private BigInteger startingMem;//in bytes
    private BigInteger endingMem;//in bytes

    private PerformanceLogger(String loggerName) {
        this.loggerName = loggerName;
    }

    public static PerformanceLogger getLogger(String name) {
        return new PerformanceLogger(name);
    }

    public void start() {
        startingTime = BigInteger.valueOf(System.currentTimeMillis());
        startingMem = BigInteger.valueOf(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
    }

    public void stop() {
        endingTime = BigInteger.valueOf(System.currentTimeMillis());
        endingMem = BigInteger.valueOf(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
    }

    public void log() {
        try {
            File file = new File("/map_reduce/performance_for_" + loggerName + ".txt");
            if (!file.exists()) {
                file.createNewFile();
            }
            FileWriter fileWriter = new FileWriter(file, true);
            fileWriter.write(loggerName + " execution time : " + endingTime.subtract(startingTime).toString() + " milliseconds\n");
            fileWriter.write(loggerName + " used memory : " + endingMem.subtract(startingMem).toString() + " bytes\n");

            fileWriter.flush();
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
