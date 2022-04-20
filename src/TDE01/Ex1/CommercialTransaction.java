package TDE01.Ex1;

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

        //Arquivo de entrada
        Path input = new Path(files[0]);

        //Arquivo de saída
        Path output = new Path(files[1]);

        //Criação e nomeação do Job
        Job j = new Job(c, "Número de transações envolvendo o Brasil");

        //Registro das Classes
        j.setJarByClass(CommercialTransaction.class);
        j.setMapperClass(Map.class);
        j.setReducerClass(Reduce.class);

        //Definição dos tipos de saída
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        //Definição dos arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        //Lança o Job e aguarda a execução
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            //Transforma a linha em String
            String row = value.toString();

            //Se a linha começar com "country_or_area", ignora e passa pra próxima linha, ou seja, ignora o cabeçalho
            if(row.startsWith("country_or_area")) return;

            //Separa os campos pelo delimitador ";"
            String[] field = row.split(";");

            //Seleciona o país
            String country = field[0];

            /**
             * Será emitido uma chave do tipo Text, agrupando todos os countrys que são iguais à Brazil
             * Será acumulado um valor do tipo IntWritable atribuido a chave
             * As chaves e valores serão enviadas ao Sort/Shuffle, e então para o Reduce
             */
            if(country.equals("Brazil")){
                context.write(new Text(country), new IntWritable(1));
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            /**
             * O reduce receberá uma série de valores IntWritable, associados à uma chave Text.
             * Foi criado uma variável para simbolizar o total de valores, que será incrementada pelo ForEach
             */
            int sum = 0;
            //Para cada valor na lista values
            for(IntWritable v : values){
                //A variável criada será incrementada
                sum += v.get();
            }
            //Envia os valores encontrados
            context.write(key, new IntWritable(sum));
        }
    }
}
