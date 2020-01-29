import java.io.*;
import java.util.Scanner;

/**
 * this class starts the issued job by user
 * it insures that sub-mapper and sub-reducer classes
 * are inheriting the abstract {@code Mapper.java , Reducer.Java}
 * and it waits until mapper and reducer nodes created then start the input phase.
 *
 * @author Sa'ad Al Jalowdi.
 */
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

    private void validateMapper() throws NotMapperException {
        try {

            if (mapperClass.getSuperclass().equals(Mapper.class)) {
                FileOutputStream fileOutputStream = new FileOutputStream(new File("/map_reduce/mapper_class.txt"));
                PrintStream printStream = new PrintStream(fileOutputStream);
                printStream.println(mapperClass.getName());
                while (!containersCreated("mapper_created.txt")) ;
            } else {
                throw new NotMapperException("make sure that " + mapperClass.getName() + " extends Mapper");
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }


    private void validateReducer() throws Exception {
        if (reducerClass.getSuperclass().equals(Reducer.class)) {
            FileOutputStream fileOutputStream = new FileOutputStream(new File("/map_reduce/reducer_class.txt"));
            PrintStream printStream = new PrintStream(fileOutputStream);
            printStream.println(mapperClass.getName());
            while (!containersCreated("reducer_created.txt")) ;

        } else {
            throw new NotReducerException("make sure that " + reducerClass.getName() + " extends Mapper");
        }
    }

    /**
     * @param s name of the the file
     * @return true if file exists and contains 1 false otherwise
     * @throws FileNotFoundException
     */
    private boolean containersCreated(String s) {
        try {
            File file = new File("/map_reduce/" + s);
            while (!file.exists()) ;
            Scanner scanner = new Scanner(file);
            return scanner.next().equals("1") ? true : false;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return false;
        }
    }

    public final void start() throws NoInputFileException {
        input = Input.getInstance(configuration);
        input.start();
    }
}
