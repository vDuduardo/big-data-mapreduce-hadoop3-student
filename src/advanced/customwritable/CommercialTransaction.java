package advanced.customwritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class CommercialTransaction {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        //input file
        Path input = new Path(files[0]);

        //output file
        Path output = new Path(files[1]);

        //job creation and name
        Job j = new Job(c, "number of transactions");

        //class registry
        j.setJarByClass(CommercialTransaction.class);
        j.setMapperClass(MapForCommercialCount.class);
        j.setReducerClass(ReduceForCommercialCount.class);


        //exit types definition
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);


        //entry and exit files definition
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        //throws job and waits for its execution
        System.exit(j.waitForCompletion(true) ? 0 : 1);

    }

    public static class MapForCommercialCount extends Mapper<LongWritable, Text, Text, IntWritable>{

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {


            String rowText = value.toString(); //Transforms row into String

            String[] field = rowText.split(";");

            String country = field[0];

            if(country.equals("Brazil")){
                context.write(new Text(country), new IntWritable(1));
            }
        }
    }

    public static class ReduceForCommercialCount extends Reducer<Text, IntWritable, Text, IntWritable>{

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for(IntWritable v : values){

                sum += v.get();

            }
            context.write(key, new IntWritable(sum));
        }
    }
}
