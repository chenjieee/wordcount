import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String inputPath = "input";
        String outputPath = "output";
        boolean caseInsensitive = true;
        int minLength = 0;
        int maxLength = 255;

        if (args.length < 2) {
            System.exit(1);
        }

        inputPath = args[0];
        outputPath = args[1];

        if (args.length >= 3) {
            caseInsensitive = Boolean.parseBoolean(args[2]);
        }

        if (args.length >= 5) {
            minLength = Integer.parseInt(args[3]);
            maxLength = Integer.parseInt(args[4]);
            if (minLength < 0 || minLength > maxLength) {
                minLength = 0;
                maxLength = 255;
            }
        }

        Job job = Job.getInstance(conf, "word count");

        job.setJarByClass(WordCount.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.getConfiguration().setBoolean("wordcount.case.insensitive", caseInsensitive);
        job.getConfiguration().setInt("wordcount.min.length", minLength);
        job.getConfiguration().setInt("wordcount.max.length", maxLength);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
