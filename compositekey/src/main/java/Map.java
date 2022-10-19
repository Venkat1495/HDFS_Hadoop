import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class Map extends Mapper<LongWritable, Text, StGroupKey, IntWritable> {

    public void map(LongWritable key, Text value, Context context
    ) throws IOException, InterruptedException {
        //log.info("INFO: In map method");
        //log.warn("WARN: In map method");
        StringTokenizer itr = new StringTokenizer(value.toString());
        String gender = itr.nextToken();
        int level = new Integer(itr.nextToken()).intValue();
        int gpa = new Integer(itr.nextToken()).intValue();
        System.out.println(gender + " " + " " + level + " " + gpa);
        StGroupKey k = new StGroupKey(gender, level);
        context.write(k, new IntWritable(gpa));
    }

}
