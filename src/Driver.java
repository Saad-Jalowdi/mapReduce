import java.io.File;
import java.io.FileNotFoundException;

public class Driver {
    public static void main(String[] args) {
        try {
            Configuration configuration = new Configuration(new File("/map_reduce/input.txt"), new File("/map_reduce/output.txt"));
            Job job = new Job(configuration);
            job.setMapperClass(WordCountMapper.class);
            job.setReducerClass(WordCountReducer.class);
            job.start();
        }catch (UnExpectedPathException e){
            e.printStackTrace();
        }catch (FileNotFoundException e){
            e.printStackTrace();
        }catch (NoInputFileException e){
            e.printStackTrace();
        }
    }

}
