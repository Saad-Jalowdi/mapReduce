import com.mapreduce.base.Reducer;

import java.util.concurrent.TimeUnit;

public class WordCountReducer extends Reducer<String, Integer> {
    @Override
    protected void reduce() {
        for (String word : getKeys()) {
            try {
                int sum = 0;
                for (int i : getValuesFor(word)) {
                    sum += i;
                }
                context.write(word, sum);
            } catch (Exception e) {
                print(e.toString() + " context is null");
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
                reduce();
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        new WordCountReducer().start();
    }
}

