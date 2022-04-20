package TDE01.Ex6;

import TDE01.Writables.MaxMinMeanWritable;
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
import java.io.IOException;

public class MaxMinMeanTransaction {
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        Path input = new Path(files[0]);

        Path output = new Path(files[1]);

        Job j = new Job(c, "Maximum, Minimum and Mean values per type and year");

        j.setJarByClass(MaxMinMeanTransaction.class);
        j.setMapperClass(MaxMinMeanTransaction.Map.class);
        j.setReducerClass(MaxMinMeanTransaction.Reduce.class);
        j.setCombinerClass(MaxMinMeanTransaction.Combine.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(MaxMinMeanWritable.class);

        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(MaxMinMeanWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class Map extends Mapper<LongWritable, Text, Text, MaxMinMeanWritable> {


        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            //Transforma a linha em String
            String row = value.toString();

            //Se a linha começar com "country_or_area", ignora e passa pra próxima linha, ou seja, ignora o cabeçalho
            if(row.startsWith("country_or_area")) return;


            //Separa os campos pelo delimitador ";"
            String[] fields = row.split(";");

            //Seleciona o ano e o unit type
            String year = fields[1];
            String type = fields[7];

            //seleciona o preço ja convertendo para double
            double values = Double.parseDouble(fields[5]);

            /**
             * Gera chave do tipo Text contatenando year e type
             * Gera os valores do Tipo MaxMinMeanWritable, acumulando o valor máximo, valor mínimo, a contagem de valores acumulados na instância e o valor acumulado.
             * Como é lido apenas uma linha por vez, os valores máximos, mínimos e valores acumuladores serão o próprio valor
             * Por fim, as chaves e valores serão enviadas ao combiner
             */
            con.write(new Text(year + ' ' + type), new MaxMinMeanWritable(values, values, 1, values));
        }
    }

    public static class Combine extends Reducer<Text, MaxMinMeanWritable, Text, MaxMinMeanWritable>{

        public void reduce(Text key, Iterable<MaxMinMeanWritable> values, Context context)
                throws IOException, InterruptedException {

            /**
             * O objetivo do combiner é realizar uma soma parcial dos valores a fim de diminuir o trabalho do reduce.
             * Ele receberá uma série de valores do tipo MaxMinMeanWritable, associados a chaves do tipo Text
             * É criado variáveis para guardar o total de valores, valor máximo, mínimo e soma de valores
             * A variável max recebe o *menor* valor que um double pode ter
             * Assim como min recebe o *maior* valor que um double pode ter
             */
            double max = Double.MIN_VALUE;
            double min = Double.MAX_VALUE;
            double n = 0;
            double sum = 0;

            //Para cada valor na lista values
            for(MaxMinMeanWritable v : values){
                //Se o valor encontrado for maior que o máximo atual, a váriavel max é atualizada
                if(v.getMax() > max) max = v.getMax();
                //Se o valor encontrado for menor que o mínimo atual, a variável min é atualizada
                if(v.getMin() < min) min = v.getMin();

                //Acumulamos o total dos valores, e a soma dos valores
                sum+=v.getSum();
                n+=v.getN();

            }
            //Enviamos a soma parcial realizada para o reduce
            context.write(key, new MaxMinMeanWritable(max, min, n, sum));
        }
    }


    public static class Reduce extends Reducer<Text, MaxMinMeanWritable, Text, MaxMinMeanWritable>{

        public void reduce(Text key, Iterable<MaxMinMeanWritable> values, Context context)
                throws IOException, InterruptedException {

            //O reduce fará o processo novamente, mas desta vez alguns valors ja somados
            double max = Double.MIN_VALUE;
            double min = Double.MAX_VALUE;
            double n = 0;
            double sum = 0;

            for(MaxMinMeanWritable v : values){
                if(v.getMax() > max) max = v.getMax();
                if(v.getMin() < min) min = v.getMin();

                sum+=v.getSum();
                n+=v.getN();

            }
            /**
             * Escrevemos os resultados.
             * A média é calculada no método toString() criado no MaxMinMeanWritable
             */
            context.write(key, new MaxMinMeanWritable(max, min, n, sum));
        }
    }
}
