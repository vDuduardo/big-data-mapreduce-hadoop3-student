package TDE01.Ex3;

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

public class CommercialTransactionPerFlowAndYear {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        //input file
        Path input = new Path(files[0]);

        //output file
        Path output = new Path(files[1]);

        //job creation and name
        Job j = new Job(c, "Transactions per Flow and Year");

        //class registry
        j.setJarByClass(CommercialTransactionPerFlowAndYear.class);
        j.setMapperClass(CommercialTransactionPerFlowAndYear.Map.class);
        j.setReducerClass(CommercialTransactionPerFlowAndYear.Reduce.class);


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

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            //Transforma a linha em String
            String row = value.toString();

            //Se a linha começar com "country_or_area", ignora e passa pra próxima linha, ou seja, ignora o cabeçalho
            if(row.startsWith("country_or_area")) return;

            //Separa os campos pelo delimitador ";"
            String[] field = row.split(";");

            //Seleciona o ano e o tipo de flow
            String year = field[1];
            String flow = field[4];

            /**
             * Será emitido uma chave do tipo Text, agrupando todos os anos e flows
             * Será acumulado um valor do tipo IntWritable atribuido a chave
             * Não é feito a mesma verificação para o flow, pois a primeira chave considerada sempre é o ano.
             * As chaves e valores serão enviadas ao Sort/Shuffle, e então para o Reduce
             */

            context.write(new Text(year +" - " + flow), new IntWritable(1));
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            /**
             * O reduce receberá uma série de valores IntWritable, associados à uma chave Text.
             * Foi criado uma variável para guardar o total de transações
             */
            int sum = 0;
            for (IntWritable v : values) {
                //A variável criada será incrementada para cada objeto presente na lista values
                sum += v.get();
            }
            //Envia os valores encontrados
            context.write(key, new IntWritable(sum));
        }
    }
}
