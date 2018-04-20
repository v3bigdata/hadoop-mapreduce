package v3bigdata.beginner.aggregation;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author ketankk on 17/4/18
 */
public class AvgCombiner extends Reducer<Text,IntWritable,Text,IntWritable> {
    int max=0;

    /**
     * It will reduce/ filetr out max marks of a particular subject on that node
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        values.forEach(val->{
            if(max<val.get()){
                max=val.get();
            }
        });
        Text subj=new Text(key);
        IntWritable maxMarks=new IntWritable(max);
        context.write(subj,maxMarks);
        max=0;
    }
}
