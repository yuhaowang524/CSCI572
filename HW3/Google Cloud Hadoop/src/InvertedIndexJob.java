import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

public class InvertedIndexJob {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        private Text word = new Text();
        private Text documentID = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] document = value.toString().split("\t", 2);
            // convert all the words to the lowercase
            String text = document[1].toLowerCase();
            // Replace all the occurrences of special characters and numerals by space character
            text = text.replaceAll("[^a-z\\s]", " ");
            text = text.replaceAll("\\s+", " ");

            documentID.set(document[0]);
            StringTokenizer tokenizer = new StringTokenizer(text);

            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                context.write(word, documentID);
            }
        }
    }


    public static class IndexCountReducer extends Reducer<Text, Text, Text, Text> {
        private Text ret = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> documentMap = new HashMap<>();
            for (Text val : values) {
                String documentID = val.toString();
                documentMap.put(documentID, documentMap.getOrDefault(documentID, 0) + 1);
            }

            StringBuilder str = new StringBuilder();
            for (String documentID : documentMap.keySet()) {
                str.append(documentID).append(":").append(documentMap.get(documentID)).append(" ");
            }
            ret.set(str.substring(0, str.length() - 1));
            context.write(key, ret);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Inverted Index");
        job.setJarByClass(InvertedIndexJob.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IndexCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
