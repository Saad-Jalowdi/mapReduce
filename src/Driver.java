import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;

public class Driver {
    public static void main(String[] args) {
        try {
            Configuration configuration = new Configuration(new File("/map_reduce/input.txt"), new File("/map_reduce/output.txt"));
            PrintStream printStream = new PrintStream(new FileOutputStream(new File("/map_reduce/stdout.txt")));
            printStream.println(
                    configuration.getMapperIpAddresses() + "\n"+ configuration.getReducerIpAddresses()
                    + "\n"+ configuration.getShufflerIp()+ "\n"+ configuration.getResultIp()
                    + "\n"+ configuration.getMapperNodes()+ "\n"+ configuration.getReducerNodes()
            );
            Job job = new Job(configuration);
            job.setMapperClass(WordCountMapper.class);
            job.setReducerClass(WordCountReducer.class);
            job.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
