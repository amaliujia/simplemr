import edu.cmu.courses.simplemr.mapreduce.OutputCollector;

import java.io.Serializable;
import java.util.Iterator;
/**
 * The MapReduce interface.
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */


public interface MapReduce extends Serializable{
    public void map(String key, String value, OutputCollector collector);
    public void reduce(String key, Iterator<String> values, OutputCollector collector);
}
