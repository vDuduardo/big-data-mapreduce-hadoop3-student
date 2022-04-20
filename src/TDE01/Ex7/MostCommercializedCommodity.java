package TDE01.Ex7;

import TDE01.Writables.MostCommercializedCommodityWritable;
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

public class MostCommercializedCommodity {

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        Path intermediate = new Path("./src/TDE01/Ex7/intermediate.tmp");

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j1 = new Job(c, "most-commertialized-count");

        // registro das classes
        j1.setJarByClass(MostCommercializedCommodity.class);
        j1.setMapperClass(MostCommercializedCommodity.MapA.class);
        j1.setReducerClass(MostCommercializedCommodity.ReduceA.class);
        j1.setCombinerClass(MostCommercializedCommodity.ReduceA.class);

        // definicao dos tipos de saida
        j1.setMapOutputKeyClass(Text.class);
        j1.setMapOutputValueClass(DoubleWritable.class);
        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(DoubleWritable.class);

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j1, input);
        FileOutputFormat.setOutputPath(j1, intermediate);

        // lanca o job e aguarda sua execucao
        // System.exit(j1.waitForCompletion(true) ? 0 : 1);
        if(!j1.waitForCompletion(true)){
            System.err.println("Error with Job 1!");
            return;
        }

        Job j2 = new Job(c, "most-commertialized-pt2");

        // registro das classes
        j2.setJarByClass(MostCommercializedCommodity.class);
        j2.setMapperClass(MostCommercializedCommodity.MapB.class);
        j2.setCombinerClass(MostCommercializedCommodity.ReduceB.class);
        j2.setReducerClass(MostCommercializedCommodity.ReduceB.class);

        // definicao dos tipos de saida
        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(MostCommercializedCommodityWritable.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(MostCommercializedCommodityWritable.class);

        // arquivos de entrada e saida
        FileInputFormat.addInputPath(j2, intermediate);
        FileOutputFormat.setOutputPath(j2, output);

        if(!j2.waitForCompletion(true)){
            System.err.println("Error with Job 2!");
            return;
        }
    }


    public static class MapA extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            // Obtendo a linha para processamento
            String row = value.toString();

            // Quebrando em campos
            String field[] = row.split(";");

            // Obtendo o ano
            int year;

            // Tentaremos converter o ano para um int, se não conseguirmos a linha é um cabeçalho ou não está formatada
            // da forma que esperamos. Qualquer que seja o caso, ignoraremos a linha (retornaremos).
            try {
                year = Integer.parseInt(field[1]);
            } catch(Exception e) { return; }

            // Se o ano não for 2016, a linha será ignorada
            if (year != 2016) return;

            // Obtendo a commodity, o flow_type e a quantidade comercializada
            String commodity = field[3];
            String flow_type = field[4];
            double quantity;
            // A mesma ideia aplicada na corversão do ano para inteiro é aplicada aqui
            try {
                quantity = Double.parseDouble(field[8]);
            } catch(Exception e) { return; }

            /**
             * Gera chave do tipo Text concatenando flow type e o nome da commodity
             * Associada a essa chave, é enviada a quantidade associada a transação dessa commodity.
             */
            con.write(new Text(commodity + "\t" + flow_type), new DoubleWritable(quantity));
        }
    }

    public static class ReduceA extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context con)
                throws IOException, InterruptedException {
            /**
             * O objetivo desse combiner é acumular as quantidades encontradas de commodity por tipo de fluxo,
             * reduzindo o volume de dados que chegará no Reducer.
             * O reducer fará a mesma coisa, porém acumulará os resultados parciais de cada node, e gerará um resultado
             * total.
             * Devido a função para o Combiner ser identica ao Reducer,  foi utilizado a mesma classe para ambos os casos
             */

            double sum = 0;

            // Para isso, em cada resultado encontrado, o valor será somado à variável acumuladora 'sum'
            for (DoubleWritable v: values) {
                sum += v.get();
            }

            /**
             * No final, a mesma chave recebida será passada adiante, porém associada ao valor da soma atualizado.
             * O resultado dessa primeira etapa (no final do Reduce) é um chave que diz de qual commodity e de qual tipo
             * de fluxo estamos falando, associada a um valor do tipo double, que corresponde a soma de todas as quantidades
             * dessa mesma commodity nesse mesmo fluxo presente no conjunto de dados.
             */

            con.write(key, new DoubleWritable(sum));
        }
    }

    public static class MapB extends Mapper<LongWritable, Text, Text, MostCommercializedCommodityWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // pega linha
            String row = value.toString();

            // no segundo map separaremos a linha em commodity, tipo de fluxo e valor (resultados do primeiro mapReduce)
            String commodity = row.split("\t")[0];
            String flow_type = row.split("\t")[1];
            double quantity = Double.parseDouble(row.split("\t")[2]);

            /**
             * Na sequencia enviaremos um par contendo como chave o flow_type e como valor um
             * MostCommercializedCommodityWritable, que agrega a quantidade total de transações daquela commodity (obtida
             * no primeiro MapReduce) e o nome da commodity
             */

            con.write(new Text(flow_type), new MostCommercializedCommodityWritable(quantity, commodity));
        }
    }

    public static class ReduceB extends Reducer<Text, MostCommercializedCommodityWritable, Text, MostCommercializedCommodityWritable> {
        public void reduce(Text key, Iterable<MostCommercializedCommodityWritable> values, Context con)
                throws IOException, InterruptedException {

            /**
             * Tanto no combine quanto no reduce, receberemos uma lista de MostCommercializedCommodityWritable, associadas
             * a um fluxo específico, e precisamos percorrer essa lista, guardando qual é a maior quantidade encontrada.
             */

            MostCommercializedCommodityWritable mostCommertialized = new MostCommercializedCommodityWritable(0, "none");

            // Para cada elemento da lista
            for (MostCommercializedCommodityWritable m: values) {
                // Se a quantidade do valor atual for maior que a quantidade que temos armazenada
                if (mostCommertialized.getQuantity() < m.getQuantity()) {
                    // Atualizar a variável
                    mostCommertialized.setQuantity(m.getQuantity());
                    mostCommertialized.setCommodity(m.getCommodity());
                }
            }

            /**
             * No final, a variável mostCommertialized contém a maior quantidade dentre todos os valores da lista, e o
             * nome da commodity correspondente, então escrevemos ela como valor associado a chave recebida, que
             * é o tipo de fluxo.
             */
            con.write(key, mostCommertialized);
        }
    }

}