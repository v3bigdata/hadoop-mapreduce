package beginner.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author ketankk on 11/4/18
 */
public class WordCount {

    /**
     * Entry Method for WordCount programme in MapReduce
     * command to run this jar is
     * //TODO
     * hadoop fs -jar ....
     * <p>
     * Input file and Output file location are passed as
     * parameter while submitting java jar
     *
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {

        /**
         *Configuration object is used for setting different hadoop
         *Mapreduce configurations like username etc.
         *
         * this will be explained in advance tutorial/code sample
         *
         */
        Configuration conf = new Configuration();
        /**
         * Each mapreduce code is submitted as a Job
         * We have got instance of Job by passing conf and Job name
         */
        Job job = Job.getInstance(conf, "WordCount");
        job.setJarByClass(WordCount.class);

        /**
         * Set the Mapper class for the job
         */
        job.setMapperClass(WCMapper.class);

        /**
         * Similarly Set the Reducer class for the job
         */
        job.setReducerClass(WCReducer.class);

        /**
         * We can set number of Reducer by this property
         * Number of Mapper can not be set with code,
         * It depends on size of input file
         * Number of Mapper will be No. of InputSplit of Input File
         */
        job.setNumReduceTasks(2);

        /**
         * Set the type of output key
         * In case of WordCount problem key will be words
         * which is String that's why we have used Text.class
         *
         */
        job.setOutputKeyClass(Text.class);


        /**
         * Set the type of output value
         * In case of WordCount problem output value will be count of words
         * which will be Long that's why we have used LongWritable.class
         * For Serialization of objects, we have used Writable data types instead of Java classes
         */

        job.setOutputValueClass(LongWritable.class);
/**
 * Convert input/output path, passed as parameter, value to Path
 */
        Path inputPath = new Path(args[0]);
        Path outPath = new Path(args[1]);

/**
 * Set Location of File in hdfs, which will be used as input
 */
        FileInputFormat.addInputPath(job, inputPath);

        /**
         * Output location in HDFS
         */
        FileOutputFormat.setOutputPath(job, outPath);


    }
}
