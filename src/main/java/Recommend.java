import java.io.IOException;
import java.security.KeyStore;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Recommend {
    public static Integer MAX_ITEMS = 0;
    public static Map<Integer, Integer[]> COMatrix = new HashMap<>();
    //public static Map<Integer, Integer[]> ItemPr = new HashMap<>();
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
            extends Reducer<Text,IntWritable,Text, Text> {
        private Integer[] arr = new Integer[MAX_ITEMS];
        private Text result = new Text();
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            for (int i = 0; i < MAX_ITEMS; ++i) {
                arr[i] = 0;
            }

            for (IntWritable val : values) {
                arr[val.get() - 1] = 1; // 0th item has itemid=1
            }
            String TempResult = "";
            for (int i = 0; i < MAX_ITEMS; ++i) {
                TempResult += arr[i].toString() + ",";
            }
            //ItemPr.put(Integer.parseInt(key.toString()), arr);
            result.set(TempResult);
            context.write(key, result);
        }
    }

    public static class MatrixMapper
            extends Mapper<Object, Text, IntWritable, IntWritable>{

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] Tokens = ((value.toString()).split("\t"))[1].split(",");
            for (int i = 0; i < MAX_ITEMS - 1; ++i) {
                if (Tokens[i].equals("1")) {
                    for (int j = i+1; j < MAX_ITEMS; ++j) {
                        if (Tokens[j].equals("1")) {
                            context.write(new IntWritable(i), new IntWritable(j));
                            context.write(new IntWritable(j), new IntWritable(i));
                        }
                    }
                }
            }
        }
    }

    public static class MatrixReducer
            extends Reducer<IntWritable,IntWritable,IntWritable,Text> {
        Text result = new Text();
        public void reduce(IntWritable key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            Integer[] itemList = new Integer[MAX_ITEMS];
            for (int i = 0; i < MAX_ITEMS; ++i) {
                itemList[i] = 0;
            }

            for (IntWritable val : values) {
                itemList[val.get()] += 1;
            }
            StringBuilder TempResult = new StringBuilder();
            for (int i = 0; i < MAX_ITEMS; ++i) {
                TempResult.append(itemList[i].toString()).append(",");
            }
            COMatrix.put(key.get(),itemList);
            result.set(TempResult.toString());
            context.write(key, result);
        }
    }

    public static class TokenizerMapper1
            extends Mapper<Object, Text, Text, Text>{

        private Text userId = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] Tokens = (value.toString()).split(",");
            userId.set(Tokens[0]);
            String tempStr = Tokens[1] + "," + Tokens[2];
            context.write(userId, new Text(tempStr));

        }
    }

    public static class IntSumReducer1
            extends Reducer<Text,Text,Text, Text> {
        private String[] arr = new String[MAX_ITEMS];
        private Text result = new Text();
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            for (int i = 0; i < MAX_ITEMS; ++i) {
                arr[i] = "0";
            }

            for (Text val : values) {
                int itemId = Integer.parseInt(val.toString().split(",")[0]);
                arr[itemId - 1] = val.toString().split(",")[1];
            }
            StringBuilder TempResult = new StringBuilder();
            for (int i = 0; i < MAX_ITEMS; ++i) {
                TempResult.append(arr[i]).append(",");
            }
            result.set(String.valueOf(TempResult.toString()));
            context.write(key, result);
        }
    }

    public static class RecommendMapper
            extends Mapper<Object, Text, Text, Text>{
        private Text userId = new Text();
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            userId.set((value.toString()).split("\t")[0]);
            String[] Tokens = ((value.toString()).split("\t"))[1].split(",");
            Float[] NewScore = new Float[MAX_ITEMS];
            Arrays.fill(NewScore, (float) 0.0);
            for (int i = 0; i < MAX_ITEMS; ++i) {
                for (int j = 0; j < MAX_ITEMS; ++j) {
                    NewScore[i] += COMatrix.get(i)[j] * Float.parseFloat(Tokens[j]);
                }
            }
            StringBuilder TempResult = new StringBuilder();
            for (int i = 0; i < MAX_ITEMS; ++i) {
                TempResult.append(NewScore[i]).append(",");
            }
            context.write(userId, new Text(String.valueOf(TempResult)));
        }
    }


    public static class RecommendReducer
            extends Reducer<Text,Text,Text,Text> {
        Text result = new Text();
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            String[] newScore = new String[MAX_ITEMS];
            for (Text val : values) {
                newScore = val.toString().split(",");
            }
            for (int i = 0; i < newScore.length; i++) {
                context.write(key, new Text(i+1 + "," + newScore[i]));
            }
        }
    }

    public static class FilterMapper
            extends Mapper<Object, Text, Text, Text>{
        private String userId = new String();
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] Tokens = (value.toString()).split("[,\t]");
            userId = Tokens[0];
            String item = Tokens[1];
            String score = Tokens[2];
            context.write(new Text(userId + "," + item), new Text(score));
        }
    }


    public static class FilterReducer
            extends Reducer<Text,Text,Text,Text> {
        Text result = new Text();
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int ctr = 0;
            String _score = null;
            String _key = key.toString().split(",")[0];
            String _item = key.toString().split(",")[1];
            for (Text val : values) {
                ++ctr;
                _score = val.toString();
            }
            if (ctr == 1) {
                context.write(new Text(_key), new Text(_item + "," + _score));
            }
        }
    }

    public static class SortMapper
            extends Mapper<Object, Text, Text, Text>{
        private String userId = "";
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] Tokens = (value.toString()).split("[,\t]");
            userId = Tokens[0];
            String item = Tokens[1];
            String score = Tokens[2];
            context.write(new Text(userId), new Text(item + "," + score));
            //tmap.put((-1 * score), userId + "," + item);
        }
    }


    public static class SortReducer
            extends Reducer<Text,Text,Text,Text> {
        public LinkedHashMap<String, Float> sortHashMapByValues2(
                HashMap<String, Float> passedMap) {
            List<String> mapKeys = new ArrayList<>(passedMap.keySet());
            List<Float> mapValues = new ArrayList<>(passedMap.values());
            Collections.sort(mapValues);
            Collections.sort(mapKeys);

            LinkedHashMap<String, Float> sortedMap =
                    new LinkedHashMap<>();

            Iterator<Float> valueIt = mapValues.iterator();
            while (valueIt.hasNext()) {
                Float val = valueIt.next();
                Iterator<String> keyIt = mapKeys.iterator();

                while (keyIt.hasNext()) {
                    String key = keyIt.next();
                    Float comp1 = passedMap.get(key);
                    Float comp2 = val;

                    if (comp1.equals(comp2)) {
                        keyIt.remove();
                        sortedMap.put(key, val);
                        break;
                    }
                }
            }
            return sortedMap;
        }
        public static HashMap<String, Float> sortHashMapByValues(HashMap<String, Float> hm)
        {
            List<Map.Entry<String, Float> > list =
                    new LinkedList<Map.Entry<String, Float> >(hm.entrySet());

            Collections.sort(list, new Comparator<Map.Entry<String, Float> >() {
                public int compare(Map.Entry<String, Float> o1,
                                   Map.Entry<String, Float> o2)
                {
                    if (!o1.getValue().equals(o2.getValue())) {
                        return (o1.getValue()).compareTo(o2.getValue());
                    }
                    return (o1.getKey()).compareTo(o2.getKey());
                }
            });

            HashMap<String, Float> temp = new LinkedHashMap<String, Float>();
            for (Map.Entry<String, Float> aa : list) {
                temp.put(aa.getKey(), aa.getValue());
            }
            return temp;
        }

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            Float score = (float) 0;
            String item = null;

            HashMap<String,Float> tmap2 = new HashMap<>();
            for (Text val : values) {
                item = val.toString().split(",")[0];
                score = Float.parseFloat(val.toString().split(",")[1]);
                tmap2.put(item, -1*score);
            }
            TreeMap<Float, ArrayList<Integer>> map3 = new TreeMap<>();
            for (Map.Entry<String, Float> entry : sortHashMapByValues(tmap2).entrySet()) {
                Float _score = entry.getValue();
                Integer _item = Integer.parseInt(entry.getKey());

                if (map3.containsKey(_score)) {
                    ArrayList<Integer> arlist = map3.get(_score);
                    arlist.add(_item);
                    map3.put(_score, arlist);
                } else {
                    ArrayList<Integer> arlist = new ArrayList<Integer>(Collections.singleton(_item));
                    map3.put(_score, arlist);
                }
                //context.write(key, new Text(_item + "," + -1*_score));
            }
            for (Map.Entry<Float, ArrayList<Integer>> entry : map3.entrySet()) {
                Float _score = entry.getKey();
                ArrayList<Integer> _item = entry.getValue();
                _item.sort(Collections.reverseOrder());
                for (Integer ctr : _item) {
                    context.write(key, new Text(ctr + "," + -1*_score));
                }
            }
        }
    }


    public static void main(String[] args) throws Exception {
        String output1= "tmp-output1";
        String output2 = "tmp-output2";
        String output3 = "tmp-output3";
        String output4 = "tmp-output4";
        String output5 = "tmp-output5";

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

       Configuration conf1 = new Configuration();
        Job job2 = Job.getInstance(conf1, "recommender system");
        job2.setJarByClass(Recommend.class);
        job2.setMapperClass(MatrixMapper.class);
        job2.setReducerClass(MatrixReducer.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(output1));
        FileOutputFormat.setOutputPath(job2, new Path(output2));
        job2.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job3 = Job.getInstance(conf2, "recommender system");
        job3.setJarByClass(Recommend.class);
        job3.setMapperClass(TokenizerMapper1.class);
        job3.setReducerClass(IntSumReducer1.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path(args[0]));
        FileOutputFormat.setOutputPath(job3, new Path(output3));
        job3.waitForCompletion(true);

        Configuration conf3 = new Configuration();
        Job job4 = Job.getInstance(conf3, "recommender system");
        job4.setJarByClass(Recommend.class);
        job4.setMapperClass(RecommendMapper.class);
        job4.setReducerClass(RecommendReducer.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job4, new Path(output3));
        FileOutputFormat.setOutputPath(job4, new Path(output4));
        job4.waitForCompletion(true);

        Configuration conf4 = new Configuration();
        Job job5 = Job.getInstance(conf4, "recommender system");
        job5.setJarByClass(Recommend.class);
        job5.setMapperClass(FilterMapper.class);
        job5.setReducerClass(FilterReducer.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(Text.class);
        job5.setMapOutputKeyClass(Text.class);
        job5.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job5, new Path(output4));
        FileInputFormat.addInputPath(job5, new Path(args[0]));
        FileOutputFormat.setOutputPath(job5, new Path(output5));
        job5.waitForCompletion(true);

        Configuration conf5 = new Configuration();
        Job job6 = Job.getInstance(conf5, "recommender system");
        job6.setJarByClass(Recommend.class);
        job6.setMapperClass(SortMapper.class);
        job6.setReducerClass(SortReducer.class);
        job6.setOutputKeyClass(Text.class);
        job6.setOutputValueClass(Text.class);
        job6.setMapOutputKeyClass(Text.class);
        job6.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job6, new Path(output5));
        FileOutputFormat.setOutputPath(job6, new Path(args[1]));

        System.exit(job6.waitForCompletion(true) ? 0 : 1);
    }
}