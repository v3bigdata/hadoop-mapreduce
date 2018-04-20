package v3bigdata.beginner.aggregation;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author ketankk on 17/4/18
 */
public class AvgReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

    int max=90;
    String subj="lol";


    /**
     * get max marks out of all the subjects
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
                subj=key.toString();
            }
        });

    }

    /**
     * After Running reducer for each key
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Text maxSubj=new Text(subj);
        IntWritable maxMarks=new IntWritable(max);
        context.write(maxSubj,maxMarks);


    }
}
