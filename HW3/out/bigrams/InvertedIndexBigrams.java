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
import java.util.Map;
import java.util.StringTokenizer;

public class InvertedIndexBigrams {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        private Text bigrams = new Text();
        private Text documentID = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] documents = value.toString().split("\t", 2);
            // convert all the words to the lowercase
            String text = documents[1].toLowerCase();
            // Replace all the occurrences of special characters and numerals by space character
            text = text.replaceAll("[^a-z\\s]", " ");
            text = text.replaceAll("\\s+", " ");
            documentID.set(documents[0]);
            StringTokenizer tokenizer = new StringTokenizer(text);
            // initialize a token to store previous value
            String prev = tokenizer.nextToken();
            while (tokenizer.hasMoreTokens()) {
                String curr = tokenizer.nextToken();
                bigrams.set(prev + " " + curr);
                prev = curr;
                context.write(bigrams, documentID);
            }
        }
    }


    public static class IndexCountReducer extends Reducer<Text, Text, Text, Text> {
        private Text ret = new Text();

        public void reduce(Text key, Iterable<Text> documentIDs, Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> documentMap = new HashMap<>();
            for (Text docID : documentIDs) {
                String documentID = docID.toString();
                documentMap.put(documentID, documentMap.getOrDefault(documentID, 0) + 1);
            }
            StringBuilder str = new StringBuilder();
            for (Map.Entry<String, Integer> entry : documentMap.entrySet()) {
                if (str.length() > 0) {
                    str.append(" ");
                }
                String docuID = entry.getKey();
                int count = entry.getValue();
                str.append(docuID).append(":").append(count);
            }
            ret.set(str.toString());
            context.write(key, ret);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Inverted Index Bigrams");
        job.setJarByClass(InvertedIndexBigrams.class);
        job.setMapperClass(InvertedIndexBigrams.TokenizerMapper.class);
        job.setReducerClass(InvertedIndexBigrams.IndexCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
