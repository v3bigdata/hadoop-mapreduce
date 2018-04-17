package beginner.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author ketankk on 11/4/18
 *
 * This is Reducer Class of MapReduce,
 *   It's extending Reducer<> class which has got multiple methods
 *   For Simple WordCount we need to override just reduce() method
 *
 * Please note: superClass Reducer is from <org.apache.hadoop.mapreduce.Reducer> package
 */
public class WCReducer extends Reducer<Text,IntWritable,Text,LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);


        /**
         * Input to the reducer will come from Mapper
         * After passing through Combiner(Will be discussed later)
         *
         * Ram,Shayam,Jack,Jill,Alam,Ashraf,Ram, Vaishali, Jack
         *          * Ram 1,1
         *          * Shayam 1
         *          * Jack 1,1
         *          * Jill 1
         *          * ...
         *          * Vaishali 1
         *          *
         *
         * Key will be same as output key of Mapper
         * Each values will be set of count for each key(Which was set as 1 in Mapper)
         **/

        long sum=0;
        for(IntWritable val:values){
            sum= sum+val.get();
        }
        /**
         * After adding values of each key,
         * it is written for output
         * eg.
         * Ram 2
         * Shayam 1
         * Jack 2
         * ...
         */

        context.write(key,new LongWritable(sum));

    }
}
