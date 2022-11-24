import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

      public static class TokenizerMapper
                  extends Mapper<Object, Text, Text, IntWritable> {

            private final static IntWritable one = new IntWritable(1);
            private Text word = new Text();

            public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                  StringTokenizer itr = new StringTokenizer(value.toString());

                  /*
                   * //1-Edite o mapeador para contar linhas ao invés de palavras
                   * 
                   * word.set("Linhas:");
                   * context.write(word, one);
                   */

                  /*
                   * //2.Edite o mapeador para contar o número de todas as palavras (não de cada
                   * palavra)
                   * 
                   * while (itr.hasMoreTokens()) {
                   * word.set("Palavra");
                   * itr.nextToken();
                   * context.write(word, one);
                   * }
                   * 
                   */

                  /*
                   * //3.Edite o mapeador para contar linhas e palavras
                   * 
                   * word.set("Linhas:");
                   * context.write(word, one);
                   * 
                   * while (itr.hasMoreTokens()) {
                   * word.set("Palavra");
                   * itr.nextToken();
                   * context.write(word, one);
                   * }
                   */

                  /*
                   * //4.Limpe o resultado do contador de palavras
                   * //Atualmente os resultados da contagem de palavras são muito redundantes
                   * porque as
                   * //palavras podem conter caracteres especiais (Ex.: “Caesars., Caesars’,
                   * “Caesars,”)
                   * //Remova esses caracteres especiais.
                   * 
                   * while (itr.hasMoreTokens()) {
                   * word.set(itr.nextToken().replaceAll("[^0-9a-zA-Z]+", ""));
                   * context.write(word, one);
                   * }
                   * 
                   */

                  /*
                   * //5.Maiúsculas e minúsculas. Algumas palavras podem conter caracteres
                   * maiúsculos e/ou minúsculos
                   * //Conte as palavras ignorando esses casos.
                   * 
                   * while (itr.hasMoreTokens()) {
                   * word.set(itr.nextToken().toLowerCase());
                   * context.write(word, one);
                   * }
                   * 
                   */

                  // 6.Faça a média de palavras por linha

                  word.set("Media");
                  IntWrible qtde = new IntWrible(itr.countTokens());
                  // (Media, 20)
                  // (Media, 15)
                  // (Media, 10)
                  context.write(word, qtde);

            }

            /*
             * 
             * for each documento in conj {
             * palavras = tokenize(document);
             * for each p in palavras {
             * contPalavra[p]++;
             * }
             * }
             * 
             */

            /*
             * while (itr.hasMoreTokens()) {
             * word.set(itr.nextToken());
             * context.write(word, one);
             * }
             */

      }
}

public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
      private IntWritable result = new IntWritable();

      public void reduce(Text key, Iterable<IntWritable> values,
                  Context context) throws IOException, InterruptedException {

            // (Media, [20, 15, 10])

            int sum = 0;
            int count = 0;

            // Cria contador e inicia em 0
            for (IntWritable val : values) {

                  sum += val.get();
                  // Intera contador count++
                  count++;
            }

            Media = sun / count;
            // Media = sun/count

            // result.set(sum);
            result.set(media);
            context.write(key, result);

      }

      public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "word count");
            job.setJarByClass(WordCount.class);
            job.setMapperClass(TokenizerMapper.class);
            job.setCombinerClass(IntSumReducer.class);
            job.setReducerClass(IntSumReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
      }
}