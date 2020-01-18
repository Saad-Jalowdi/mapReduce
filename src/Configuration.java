import java.io.File;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class Configuration implements Serializable {
    private File inputFile;
    private File outputFile;
    private int mapperNodes;
    private int reducerNodes;
    private ArrayList<String> mapperIpAddresses = new ArrayList<>();
    private ArrayList<String> reducerIpAddresses = new ArrayList<>();
    private String shufflerIp;
    private String resultIp;

    public Configuration(File inputFile, File outputFile) throws Exception {
        this.inputFile = inputFile;
        this.outputFile = outputFile;
        readNumberOfNodes();
        readAddresses("/map_reduce/mapper_ip_addresses.txt");
        readAddresses("/map_reduce/reducer_ip_addresses.txt");
        readAddresses("/map_reduce/shuffler_ip_address.txt");
        readAddresses("/map_reduce/result_ip_address.txt");

    }

    private void readNumberOfNodes() throws FileNotFoundException {
        Scanner scanner = new Scanner(new File("/map_reduce/nodes.txt"));
        this.mapperNodes = Integer.parseInt(scanner.next());
        this.reducerNodes = Integer.parseInt(scanner.next());
    }

    private void readAddresses(String pathname) throws Exception {
        try {
            Scanner scanner = new Scanner(new File(pathname));
            ArrayList<String> tmp;
            if (pathname.equals("/map_reduce/mapper_ip_addresses.txt")) {
                waitUntilFileCreated(pathname);
                tmp = mapperIpAddresses;
            } else if (pathname.equals("/map_reduce/reducer_ip_addresses.txt")) {
                waitUntilFileCreated(pathname);
                tmp = reducerIpAddresses;
            } else if (pathname.equals("/map_reduce/shuffler_ip_address.txt")) {
                waitUntilFileCreated(pathname);
                this.shufflerIp = scanner.next();
                return;
            } else if(pathname.equals("/map_reduce/result_ip_address.txt")){
                waitUntilFileCreated(pathname);
                this.resultIp = scanner.next();
                return;
            }else{
                throw new Exception("Unexpected path.");//TODO exception
            }
            while (scanner.hasNext()) {
                tmp.add(scanner.next());
            }
            scanner.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }

    private void waitUntilFileCreated(String pathname) throws InterruptedException {
        while (!new File(pathname).exists()) {
            TimeUnit.MILLISECONDS.sleep(250);
        }
    }


    public File getInputFile() {
        return inputFile;
    }

    public File getOutputFile() {
        return outputFile;
    }

    public int getMapperNodes() {
        return mapperNodes;
    }

    public int getReducerNodes() {
        return reducerNodes;
    }


    public ArrayList<String> getMapperIpAddresses() {
        return mapperIpAddresses;
    }

    public ArrayList<String> getReducerIpAddresses() {
        return reducerIpAddresses;
    }

    public String getShufflerIp() {
        return shufflerIp;
    }

    public String getResultIp() {
        return resultIp;
    }
}
