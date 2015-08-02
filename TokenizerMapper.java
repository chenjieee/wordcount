import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);

    private Configuration conf;

    private boolean caseInsensitive;
    private int minLength;
    private int maxLength;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        conf = context.getConfiguration();
        caseInsensitive = conf.getBoolean("wordcount.case.insensitive", true);
        minLength = conf.getInt("wordcount.min.length", 0);
        maxLength = conf.getInt("wordcount.max.length", 255);
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = (caseInsensitive) ? value.toString().toLowerCase() : value.toString();
        line = line.replaceAll("[^a-zA-Z0-9 ]", " ");

        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            String word = tokenizer.nextToken();
            if (word.length() >= minLength && word.length() <= maxLength) {
                context.write(new Text(word), one);
            }
        }
    }

}
