import java.io.File;

public class Driver {
    public static void main(String[] args) {
        try {
            Configuration configuration = new Configuration(new File("/mapReduce/input.txt"), new File("/mapReduce/output.txt"));
            Job job = new Job(configuration);
            job.setMapperClass(WordCountMapper.class);
            job.setReducerClass(WordCountReducer.class);
            job.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
