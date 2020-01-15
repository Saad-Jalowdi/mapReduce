public class WordCountReducer extends Reducer<String, Integer> {
    @Override
    protected void reduce() {
        for (String word : getKeys()) {
            if (word.matches("[a-zA-Z]+")) {
                try {
                    int sum = 0;
                    for (int i : getValuesFor(word)) {
                        sum += i;
                    }
                    context.write(word, sum);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {
        new WordCountReducer().start();
    }
}

