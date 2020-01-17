import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WordCountMapper extends Mapper<String, Integer> {
    @Override
    public void map() {
        Pattern pattern = Pattern.compile("[a-zA-Z]+");
        Matcher matcher;
        for (String str : data) {
            matcher = pattern.matcher(str);
            if (matcher.find()) {
                String word = matcher.group();
                context.write(word, 1);
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        new WordCountMapper().start();
    }

}
