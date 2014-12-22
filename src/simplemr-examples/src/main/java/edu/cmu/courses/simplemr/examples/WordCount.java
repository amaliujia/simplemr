import AbstractMapReduce;
import OutputCollector;

import java.util.Iterator;

/**
 * The word count example.
 * Count the occurrence time of words.
 * In put a line of file.
 * Output in the format:
 * "word number"
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */

public class WordCount extends AbstractMapReduce {

    @Override
    public void map(String key, String value, OutputCollector collector) {
        String[] words = value.split("\\s+");
        for(String word : words){
            collector.collect(word, "1");
        }
    }

    @Override
    public void reduce(String key, Iterator<String> values, OutputCollector collector) {
        int count = 0;
        while(values.hasNext()){
            count++;
            values.next();
        }
        collector.collect(key, String.valueOf(count));
    }

    public static void main(String[] args) {
        new WordCount().run(args);
    }
}
