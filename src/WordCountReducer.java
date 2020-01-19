/**
 * demo class..
 */
public class WordCountReducer extends Reducer<String, Integer> {
    @Override
    protected void reduce() {
        for (String word : getKeys()) {
            int sum = 0;
            for (int i : getValuesFor(word)) {
                sum += i;
            }
            context.write(word, sum);
        }
    }
        public static void main (String[]args){
            new WordCountReducer().start();
        }
    }

