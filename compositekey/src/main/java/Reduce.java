import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reduce extends Reducer<StGroupKey, IntWritable, StGroupKey, FloatWritable> {

    public void reduce(StGroupKey key, Iterable<IntWritable> values,
                       Context context
    ) throws IOException, InterruptedException {
        //log.info("INFO: In reduce method with key " + key);
        //log.warn("WARN: In reduce method with key " + key);
        int sum = 0;
        int cnt = 0;
        float avg = 0;
        for (IntWritable val : values) {
            sum += val.get();
            cnt += 1;
        }
        avg = sum / (float) cnt;
        System.out.println("Reduce : Key " + key + " " + avg + " " + cnt + " " + sum);
        context.write(key, new FloatWritable(avg));
    }

}
