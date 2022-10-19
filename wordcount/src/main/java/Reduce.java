import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
    ) throws IOException, InterruptedException {
        //log.info("INFO: In reduce method with key " + key);
        //log.warn("WARN: In reduce method with key " + key);
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        IntWritable result = new IntWritable(sum);
        context.write(key, result);
    }

}
