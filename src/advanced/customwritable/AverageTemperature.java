package advanced.customwritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;
import sun.print.DialogOwner;

import java.io.IOException;

public class AverageTemperature {

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "media");

        //Registro de classes
        j.setJarByClass(AverageTemperature.class);
        j.setMapperClass(MapForAverage.class);
        j.setReducerClass(ReduceForAverage.class);
        j.setCombinerClass(CombineForAverage.class);

        //Definição de tipos de saide
        //Map (Text, FireAvgTemperature)
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(FireAvgTempWritable.class);

        //reduce (Text, DoubleWritable
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);

        //Definição de arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }


    public static class MapForAverage extends Mapper<LongWritable, Text, Text, FireAvgTempWritable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            //Obtendo a linha para processamento
            String linha = value.toString();

            //Quebrando em campos
            String campos[] = linha.split(",");

            //Obtendo a temperatura
            double temperatura = Double.parseDouble(campos[8]);

            //Obtendo mes
            String mes = campos[2];

            //Emitir (chave, valor) -> ("media", (n=1, sum=temperatura)
            con.write(new Text("media"), new FireAvgTempWritable(1, temperatura));
            con.write(new Text(mes), new FireAvgTempWritable(1, temperatura));
        }
    }

    public static class CombineForAverage extends Reducer<Text, FireAvgTempWritable, Text, FireAvgTempWritable>{

        public void reduce(Text key, Iterable<FireAvgTempWritable> values, Context con)
                throws IOException, InterruptedException {
            //O objetivo desse combine ẽ SOMAR os Ns e as SOMAS parciais
            int totalN = 0;
            double totalSoma = 0.0;
            for(FireAvgTempWritable o : values){
                totalN += o.getN();
                totalSoma += o.getSoma();
            }

            //enviando do combiner para o sort/shuffle
            con.write(key, new FireAvgTempWritable(totalN, totalSoma));
        }
    }


    public static class ReduceForAverage extends Reducer<Text, FireAvgTempWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<FireAvgTempWritable> values, Context con)
                throws IOException, InterruptedException {

            int nTotal = 0;
            double somaTotal = 0.0;
            for(FireAvgTempWritable o : values){
                nTotal += o.getN();
                somaTotal += o.getSoma();
            }

            double media = somaTotal/ nTotal;

            con.write(key, new DoubleWritable(media));
        }
    }
}
