import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Recommend {
    public static Integer MAX_ITEMS = 0;

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private IntWritable itemId = new IntWritable();
        private Text userId = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] Tokens = (value.toString()).split(",");
            userId.set(Tokens[0]);
            int tempItem = Integer.parseInt(Tokens[1]);
            itemId.set(tempItem);
            if (tempItem > MAX_ITEMS) {
                MAX_ITEMS = tempItem;
            }
            context.write(userId, itemId);

        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text, ArrayWritable> {
        private ArrayWritable result = new ArrayWritable(IntWritable.class);
        private IntWritable[] arr = new IntWritable[MAX_ITEMS];

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            for (Integer i = 0; i < MAX_ITEMS; ++i) {
                arr[i] = new IntWritable(0);
            }

            for (IntWritable val : values) {
                arr[val.get() - 1] = new IntWritable(1); // 0th item has itemid=1
            }
            result.set(arr);
            context.write(key, result);
        }
    }

    /*public static class MatrixMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private ArrayWritable items = new ArrayWritable(IntWritable.class);
        private Text userId = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            value.toString().split();

            while(Tokens.hasMoreTokens()) {

            }
            userId.set(Tokens.next());
            IntWritable[] tempItem = new IntWritable(Tokens[1]);
            items.set(tempItem);
            if (tempItem > MAX_ITEMS) {
                MAX_ITEMS = tempItem;
            }
            context.write(userId, items);

        }
    }

    public static class MatrixReducer
            extends Reducer<Text,IntWritable,Text, ArrayWritable> {
        private ArrayWritable result = new ArrayWritable(IntWritable.class);
        private IntWritable[] arr = new IntWritable[MAX_ITEMS];

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            for (Integer i = 0; i < MAX_ITEMS; ++i) {
                arr[i] = new IntWritable(0);
            }

            for (IntWritable val : values) {
                arr[val.get() - 1] = new IntWritable(1); // 0th item has itemid=1
            }
            result.set(arr);
            context.write(key, result);
        }
    }*/

    public static void main(String[] args) throws Exception {
        String output1= "tmp-output1";
        String output2 = "tmp-output2";
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "recommender system");
        job1.setJarByClass(Recommend.class);
        job1.setMapperClass(TokenizerMapper.class);
        job1.setReducerClass(IntSumReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(output1));
        job1.waitForCompletion(true);

       /*Configuration conf1 = new Configuration();
        Job job2 = Job.getInstance(conf1, "recommender system");
        job2.setJarByClass(Recommend.class);
        job2.setMapperClass(MatrixMapper.class);
        job2.setReducerClass(MatrixReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(ArrayWritable.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(output1));
        FileOutputFormat.setOutputPath(job2, new Path(output2));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);*/
    }
}