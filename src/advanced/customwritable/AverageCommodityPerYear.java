package advanced.customwritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class AverageCommodityPerYear {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        //input file
        Path input = new Path(files[0]);

        //output file
        Path output = new Path(files[1]);

        //job creation and name
        Job j = new Job(c, "Average Commodity Per year");

        //class registry
        j.setJarByClass(AverageCommodityPerYear.class);
        j.setMapperClass(AverageCommodityPerYear.MapForAverageCommodityPerYear.class);
        j.setReducerClass(AverageCommodityPerYear.ReduceForAverageCommodityPerYear.class);
        j.setCombinerClass(AverageCommodityPerYear.CombineForAverageCommodityPerYear.class);


        //exit types definition
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(CommercialTransactionWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);


        //entry and exit files definition
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        //throws job and waits for its execution
        System.exit(j.waitForCompletion(true) ? 0 : 1);

    }

    public static class MapForAverageCommodityPerYear extends Mapper<LongWritable, Text, Text, CommercialTransactionWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {


            String row = value.toString();
            if (row.startsWith("country_or_area"))
                return;

            String[] field = row.split(";");

            String year = field[1];
            String priceString = field[5];

            double price = Double.parseDouble(priceString);

                con.write(new Text("media"), new CommercialTransactionWritable(1, price));
                con.write(new Text(year), new CommercialTransactionWritable(1, price));
        }
    }

    public static class CombineForAverageCommodityPerYear extends Reducer<Text, CommercialTransactionWritable, Text, CommercialTransactionWritable>{

        public void reduce(Text key, Iterable<CommercialTransactionWritable> values, Context con)
                throws IOException, InterruptedException {
            //O objetivo desse combine ẽ SOMAR os Ns e as SOMAS parciais
            int sum = 0;
            double totalSum = 0.0;
            for(CommercialTransactionWritable o : values){
                sum += o.getN();
                totalSum += o.getSoma();
            }

            //enviando do combiner para o sort/shuffle
            con.write(key, new CommercialTransactionWritable(sum, totalSum));
        }
    }

    public static class ReduceForAverageCommodityPerYear extends Reducer<Text, CommercialTransactionWritable, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<CommercialTransactionWritable> values, Context con)
                throws IOException, InterruptedException {

            int sum = 0;
            double totalSum = 0;
            for (CommercialTransactionWritable o : values) {
                sum += o.getN();
                totalSum += o.getSoma();
            }

            double result = totalSum/sum;
            if(!key.toString().startsWith("media")){
                con.write(key, new DoubleWritable(result));
            }
        }
    }
}
