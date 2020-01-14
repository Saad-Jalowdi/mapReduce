import java.io.*;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class Job {


    private Class mapperClass;
    private Class reducerClass;
    private Configuration configuration;
    private Input input;

    public Job(Configuration configuration) {
        this.configuration = configuration;
    }

    public void setMapperClass(Class mapperClass) {
        this.mapperClass = mapperClass;
        try {
            validateMapper();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void setReducerClass(Class reducerClass) {
        this.reducerClass = reducerClass;
        try {
            validateReducer();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void validateMapper() throws Exception {

        if (mapperClass.getSuperclass().equals(Mapper.class)) {
            FileOutputStream fileOutputStream = new FileOutputStream(new File("/mapReduce/mapper_class.txt"));
            PrintStream printStream = new PrintStream(fileOutputStream);
            printStream.println(mapperClass.getName());
            while (!containersCreated("mapper_created.txt")) ;

        } else {
            throw new Exception("not a mapper exception"); //TODO change it to NotMapperException
        }

    }

    private void validateReducer() throws Exception {
        if (reducerClass.getSuperclass().equals(Reducer.class)) {
            FileOutputStream fileOutputStream = new FileOutputStream(new File("/mapReduce/reducer_class.txt"));
            PrintStream printStream = new PrintStream(fileOutputStream);
            printStream.println(mapperClass.getName());
            while (!containersCreated("reducer_created.txt")) ;

        } else {
            throw new Exception("not a reducer exception"); //TODO change it to NotReducerException

        }
    }

    private boolean containersCreated(String s) throws InterruptedException, FileNotFoundException {
        while (!new File("/mapReduce/" + s).exists()) {
            TimeUnit.MILLISECONDS.sleep(250);
        }
        File file = new File("/mapReduce/" + s);
        Scanner scanner = new Scanner(file);
        return scanner.next().equals("1") ? true : false;
    }

    public void start() throws Exception {
        input = new Input(configuration);
        input.start();
    }
}
