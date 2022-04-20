package TDE01.Ex4;

import TDE01.Writables.CommercialTransactionWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
        j.setMapperClass(AverageCommodityPerYear.Map.class);
        j.setReducerClass(AverageCommodityPerYear.Reduce.class);
        j.setCombinerClass(AverageCommodityPerYear.Combine.class);


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

    public static class Map extends Mapper<LongWritable, Text, Text, CommercialTransactionWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            //Transforma a linha em String
            String row = value.toString();
            //Se a linha começar com "country_or_area", ignora e passa pra próxima linha
            if (row.startsWith("country_or_area")) return;

            //Separa os campos pelo delimitador ";"
            String[] field = row.split(";");

            //Obtendo o ano
            String year = field[1];

            //Obtendo o preço e convertendo para double
            double price = Double.parseDouble(field[5]);

            /**
             * Será emitido uma chave do tipo Text, para o agrupamento dos valores para a média, e uma chave Text agrupando os anos.
             * Serão acumulados os valores do tipo CommercialTransactionWritable atribuido as duas chaves, que acumulam a quantidade de ocorrência e o valor da transação
             * As chaves e valores serão enviadas ao combiner
             */
            con.write(new Text("media"), new CommercialTransactionWritable(1, price));
            con.write(new Text(year), new CommercialTransactionWritable(1, price));
        }
    }

    public static class Combine extends Reducer<Text, CommercialTransactionWritable, Text, CommercialTransactionWritable>{

        public void reduce(Text key, Iterable<CommercialTransactionWritable> values, Context con)
                throws IOException, InterruptedException {
            /**
             * O objetivo do combiner é realizar uma soma parcial a fim de diminuir o trabalho do reduce.
             * Ele receberá uma série de valores do tipo CommercialTransactionWritable, associados a chaves do tipo Text
             * Foi criado uma variável para guardar o total de valores, e outra para guardar a soma dos valores que será incrementada pelo ForEach
             */
            int sum = 0;
            double totalSum = 0.0;
            //Para cada valor na lista values
            for(CommercialTransactionWritable o : values){
                //As variáveis da soma das quantidades e total de valores das transações serão incrementadas
                sum += o.getN();
                totalSum += o.getSoma();
            }

            //o Combiner envia os resultados parciais ao sort/shuffle
            con.write(key, new CommercialTransactionWritable(sum, totalSum));
        }
    }

    public static class Reduce extends Reducer<Text, CommercialTransactionWritable, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<CommercialTransactionWritable> values, Context con)
                throws IOException, InterruptedException {
            /**
             * O reduce receberá do sort/shuffle uma série de valores do tipo CommercialTransactionWritable, associados a chaves do tipo Text parcialmente já reduzidos
             * E então fará novamente o processo de redução com os valores restantes
             */
            int sum = 0;
            double totalSum = 0;
            for (CommercialTransactionWritable o : values) {
                sum += o.getN();
                totalSum += o.getSoma();
            }

            //Criação uma variável que representa o calculo da média
            double result = totalSum/sum;

            //É realizado a verificação para que seja ignorado a chave que comece com "media", para que não exista uma linha do tipo "media 1" no output
            if(!key.toString().startsWith("media")){
                //Escreve os resultados encontrados
                con.write(key, new DoubleWritable(result));
            }
        }
    }
}
