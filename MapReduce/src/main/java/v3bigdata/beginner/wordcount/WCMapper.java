package v3bigdata.beginner.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author ketankk on 11/4/18
 *
 * This is Mapper Class of MapReduce,
 * It's extending Mapper<> class which has got multiple methods
 * For Simple WordCount we need to override just map() method
 *
 * Please note: superClass Mapper is from <org.apache.hadoop.mapreduce.Mapper> package
 *
 */
public class WCMapper extends Mapper<LongWritable,Text,Text,IntWritable> {


    /**
     * For wordcount problem,
     * Input will come as each line.
     * key will be offset of each line and value will be string(set of words delimited by delimiter)
     *
     * */


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       // super.map(key, value, context);

        /**
         * Convert String of words delimited by ','(Here considering comma, it can be anything)
         * to Array of words
         */
        String[] words = value.toString().split(",");

        /**
         * We map each word with one(1), count of one word is 1
         *
         *
         * eg. if input is
         * Ram,Shayam,Jack,Jill,Alam,Ashraf,Ram,Vaishali,Jack
         *
         * then it will be mapped as
         * Ram 1
         * Shayam 1
         * Jack
         * Jill 1
         * ...
         * Jack 1
         *
         *
         * this output by mapper will go as input to Reducer
         */
        for (String word:words)
            context.write(new Text(word),new IntWritable(1));


    }
}
