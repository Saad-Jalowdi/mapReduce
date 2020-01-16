
public class WordCountMapper extends Mapper<String, Integer> {
    @Override
    public void map() {
        for (String word : data) {
            if (word.matches("[a-zA-Z]+"))
                context.write(word, 1);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        new WordCountMapper().start();
    }

}
