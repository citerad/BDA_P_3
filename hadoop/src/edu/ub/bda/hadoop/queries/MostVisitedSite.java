package edu.ub.bda.hadoop.queries;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A query to find the most visited site of a day.
 *
 * @author domenicocitera
 */
public class MostVisitedSite extends Configured implements Tool
{

    private static final String root = "/user/hive/warehouse";
    private static final String dbName = "dcitera_olopez.db/";
    private static final String tableName = "bda_wikidump_dcitera_olopez";
    private static final String outRootPath = "/user/olopez/";
    private static final boolean dev = false;
    
    /**
     * The mapper class that maps a context to the number of requests done at a specific hour.
     *
     */
    public static class MyMap extends Mapper<LongWritable, Text, Text, IntWritable> {

        Text word = new Text();
        
        private static final Pattern inputPattern = Pattern.compile("(.+)(\\s)(.+)(\\s)(\\d+)(\\s)(\\d+)");

        /**
         * @param key - Input key - The line offset in the file - ignored.
         * @param value - Input Value - This is the line itself.
         * @param context - Provides access to the OutputCollector and Reporter.
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            Matcher inputMatch = inputPattern.matcher(value.toString());

            if (inputMatch.matches()) {

                word.set(inputMatch.group(3));
                int outValue = Integer.parseInt(inputMatch.group(5));

                context.write(word, new IntWritable(outValue));
            }
        }
    }

    /**
     * The reducer class that adds every request for each context, and returns the page with the
     * maximum quantity of requests.
     *
     */
    public static class MyRed extends Reducer<Text, IntWritable, Text, IntWritable> {

        private int max = 0;
        private Map<String, Integer> maxcont = new HashMap<String, Integer>();
        
        /**
         * @param key - Input key - Name of the context
         * @param values - Input Value - Iterator request context
         * @param context - Used for collecting output
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {

            int sum = 0;

            for (IntWritable value : values)
            {
                sum = sum + value.get();

                if (sum >= max)
                {
                    max = sum;
                    maxcont.put(key.toString(), sum);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException
        {
            for (String key : maxcont.keySet())
            {
                if (maxcont.get(key) == max)
                {
                    context.write(new Text(key), new IntWritable(max));
                }
            }

        }
    }

    public static void main(String[] args) throws Exception
    {
        MostVisitedSite driver = new MostVisitedSite();
        int exitCode = ToolRunner.run(driver, args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {

        String year = null, month = null, day = null;

        if (dev)
        {
            year = "2013";
            month = "11";
            day = "05";
        }
        else
        {
            if (args.length == 3)
            {
                year = args[0];
                month = args[1];
                day = args[2];
            }
            else
            {
                throw new Exception("Bad arguments");
            }
        }

        // Create the job specification object
        Job job = new Job(getConf());
        job.setJarByClass(MostVisitedSite.class);
        job.setJobName(this.getClass().getName());

        // Setup input and output paths
        String p = resPath(year + month + day);
        FileInputFormat.setInputPaths(job, p);

        Path outFilesPath = new Path(outRootPath + "/mes_vista_" + year + "-" + month + "-" + day);

        // Delete and create if exist
        FileSystem.get(new Configuration()).delete(outFilesPath, true);
        FileOutputFormat.setOutputPath(job, outFilesPath);

        // Set the Mapper and Reducer classes
        job.setMapperClass(MyMap.class);
        job.setReducerClass(MyRed.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Specify the type of output keys and values
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    /**
     * Return a string that contains the paths to each partition of the table for a day, separated by commas.
     * 
     */
    public static String resPath(String ymd) throws IOException
    {
        Pattern inputPattern = Pattern.compile("(.*)ds=" + ymd + "-(\\d{4})$");
        String pts = "";
        FileSystem fs = FileSystem.get(new Configuration());
        FileStatus[] status = fs.listStatus(new Path(root + "/" + dbName + tableName + "/"));
        
        for (FileStatus statu : status)
        {
            Matcher inputMatch = inputPattern.matcher(statu.getPath().toString());
            
            if (inputMatch.matches())
            {
                FileStatus[] status2 = fs.listStatus(statu.getPath());
                
                for (FileStatus status21 : status2) {
                    System.out.println(status21.getPath()); //test file input
                    pts = pts.concat(status21.getPath().toString() + ",");
                }
            }
        }
        return pts.substring(0, (pts.length() - 1));
    }
}
