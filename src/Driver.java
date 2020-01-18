import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

public class Driver {
    public static void main(String[] args) {
        try {
            TimeUnit.SECONDS.sleep(10);
            Configuration configuration = new Configuration(new File("/map_reduce/input.txt"), new File("/map_reduce/output.txt"));
            PrintStream printStream = new PrintStream(new FileOutputStream(new File("/map_reduce/stdout.txt")));
            printStream.println(
                    configuration.getMapperIpAddresses() + "\n"+ configuration.getReducerIpAddresses()
                    + "\n"+ configuration.getShufflerIp()+ "\n"+ configuration.getResultIp()
                    + "\n"+ configuration.getMapperNodes()+ "\n"+ configuration.getReducerNodes()
            );
            Job job = new Job(configuration);
            job.setReducerClass(WordCountMapper.class);
            job.setReducerClass(WordCountReducer.class);
            job.start();
            print("testing");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void print(String msg) {
        try {
            PrintStream printStream = new PrintStream(new FileOutputStream(new File("/map_reduce/msgFromDriver.txt")));
            printStream.append(msg+"\n");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

}
