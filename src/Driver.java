import java.io.File;
import java.util.concurrent.TimeUnit;

public class Driver {
    public static void main(String[] args) {
        try {
            Configuration configuration = new Configuration(new File("/mapReduce/input.txt"), new File("/mapReduce/output.txt"));
            Job job = new Job(configuration);
            job.setMapperClass(WordCountMapper.class);
            job.setReducerClass(WordCountReducer.class);
            job.start();
            TimeUnit.MINUTES.sleep(1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
