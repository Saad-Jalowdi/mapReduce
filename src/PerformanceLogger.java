import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;

public class PerformanceLogger {

    private String loggerName ;
    private BigInteger startingTime; //in ms;
    private BigInteger endingTime; //in ms;

    private PerformanceLogger(String loggerName){
        this.loggerName = loggerName;
    }

    public static PerformanceLogger getLogger(String name){
        return new PerformanceLogger(name);
    }

    public void start(){
        startingTime = BigInteger.valueOf(System.currentTimeMillis());
    }

    public void stop(){
        endingTime = BigInteger.valueOf(System.currentTimeMillis());
    }

    public void log() {
        try {
            FileWriter fileWriter = new FileWriter("map_reduce/performance_for_"+loggerName);
            fileWriter.write(endingTime.subtract(startingTime).toString()+"ms");
            fileWriter.flush();
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
