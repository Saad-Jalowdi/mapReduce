
public class WordCountMapper extends Mapper<String,Integer>{
    @Override
    public void map() {
        for (String word : data){
            context.write(word,1);
        }
    }

    public static void main(String[] args) {
        new WordCountMapper().start();
    }

}
