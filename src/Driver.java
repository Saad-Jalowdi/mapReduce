import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;

public class Driver {
    public static void main(String[] args) throws UnExpectedPathException, FileNotFoundException, NoInputFileException {

            Configuration configuration = new Configuration(new File("/map_reduce/input.txt"), new File("/map_reduce/output.txt"));
            Job job = new Job(configuration);
            job.setMapperClass(WordCountMapper.class);
            job.setReducerClass(WordCountReducer.class);
            job.start();
            print("testing");

    }

    public static void print(String msg) {
        try {
            PrintStream printStream = new PrintStream(new FileOutputStream(new File("/map_reduce/msgFromDriver.txt")));
            printStream.append(msg + "\n");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

}
