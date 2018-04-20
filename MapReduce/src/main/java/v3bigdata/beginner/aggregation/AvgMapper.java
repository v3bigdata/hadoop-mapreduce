package v3bigdata.beginner.aggregation;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author ketankk on 17/4/18
 */
public class AvgMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    /**
     * A 98
     * B 90
     * C 87
     * A 88
     * A 90
     * D 99
     * B 92
     * ...
     *
     * K 79
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     *
     * Write subject name as key and it's marks as value
     *
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String subjMark[]=value.toString().split(" ");

        Text subjName=new Text(subjMark[0]);
        IntWritable marks=new IntWritable(Integer.parseInt(subjMark[1]));
        context.write(subjName,marks);
    }
}
