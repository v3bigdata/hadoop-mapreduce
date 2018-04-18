package v3bigdata.beginner.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @author ketankk on 11/4/18
 * Run
 * $ mvn clean install
 * It will generate .jar file inside target
 * Copy that Jar to Hadoop cluster node.
 * Run this command for submitting MapReduce job
 *
 * hadoop jar /Jar/location/in/local/JarName.jar v3bigdata.beginner.wordcount.WordCount /inputfile/in/hdfs/filname.ext /outfile/in/hdfs/dirname
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
     * <p>
     * delete the existing output file before running the job
     *
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        System.out.println("Starting MapReduce Job....");
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
        /**
         *  Driver class as JarClass
         */
        job.setJarByClass(WordCount.class);

        /**
         * Set the Mapper class for the job
         */
        job.setMapperClass(WCMapper.class);
        /**
         * Set this property as output `key` of mapper class
         */
        job.setMapOutputKeyClass(Text.class);
        /**
         * Set this property as output `value` of mapper class
         */
        job.setMapOutputValueClass(IntWritable.class);


        /**
         * Similarly Set the Reducer class for the job
         */
        job.setReducerClass(WCReducer.class);

        /**
         * We can set number of Reducer by this property on the other hand
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
 * Set this property manually otherwise it might throw exception
 */
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

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

/**
 * Delete the output location if it already exists otherwise it will throw execption directory already exists
 */

        FileSystem.get(conf).delete(outPath, true);


/**
 * Job must be submitted for it to start
 */
        job.submit();
        System.out.println("Job Submitted....");
        job.waitForCompletion(true);
    }


}
