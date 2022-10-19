import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.StringUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class StGroupKey implements WritableComparable<StGroupKey> {

    public String gender;
    public int level;

    public StGroupKey(){}

    public StGroupKey(String g, int l) {
        gender = g;
        level = l;
    }
/*
    @Override
    public String toString() {
        return "gender='" + gender + '\'' +
                ", level=" + level;
    }
*/
    public int compareTo(StGroupKey o) {    // 1. 0. -1
        if ("male".equalsIgnoreCase(gender) && o.gender.equalsIgnoreCase("female")) return 1;
        else if ("female".equalsIgnoreCase(gender) && o.gender.equalsIgnoreCase("male")) return -1;
        else {
            if (level > o.level) return 1;
            else if (level < o.level) return -1;
            else return 0;
        }
    }

    // Serialization
    public void write(DataOutput dataOutput) throws IOException {
        new Text(gender).write(dataOutput);
        new IntWritable(level).write(dataOutput);
    }

    // De-serialization
    public void readFields(DataInput dataInput) throws IOException {
        Text genderText = new Text();
        genderText.readFields(dataInput);
        gender = genderText.toString();
        //
        IntWritable levelWritable= new IntWritable();
        levelWritable.readFields(dataInput);
        level = levelWritable.get();
    }

    public static void main(String[] args) throws Exception {
        List<StGroupKey> keyList = new ArrayList<StGroupKey>();

        keyList.add(new StGroupKey("male", 3));
        keyList.add(new StGroupKey("female", 4));
        keyList.add(new StGroupKey("male", 1));
        keyList.add(new StGroupKey("female", 2));
        keyList.add(new StGroupKey("male", 4));

        Collections.sort(keyList);

        for (StGroupKey k : keyList) {
            System.out.println("Gender : " + k.gender + "  Level : " + k.level);
            //
            ByteArrayOutputStream bs = new ByteArrayOutputStream();
            DataOutputStream ds = new DataOutputStream(bs);
            k.write(ds);
            byte[] outStr = bs.toByteArray();
            System.out.println("Output String : " + StringUtils.byteToHexString(outStr));
        }
    }
}
