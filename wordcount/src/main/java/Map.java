import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable key, Text value, Context context
    ) throws IOException, InterruptedException {
        //log.info("INFO: In map method");
        //log.warn("WARN: In map method");

        StringTokenizer itr = new StringTokenizer(value.toString());
        //
        while (itr.hasMoreTokens()) {
            Text word = new Text(itr.nextToken());
            System.out.println("Key : " + word);
            //
            context.write(word, new IntWritable(1));
        }
    }

}
