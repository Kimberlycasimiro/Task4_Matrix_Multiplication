package org.ulpgc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FrequentItemSet {

    public static class ItemSetMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text itemSet = new Text();
        private int k;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            k = Integer.parseInt(conf.get("k", "2"));
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] items = line.split("\\s+|,");
            List<String> itemList = new ArrayList<>();
            for (String item : items) {
                if (!item.trim().isEmpty()) {
                    itemList.add(item.trim());
                }
            }

            List<List<String>> combinations = generateCombinations(itemList, k);
            for (List<String> combination : combinations) {
                Collections.sort(combination);
                itemSet.set(String.join(" - ", combination));
                context.write(itemSet, one);
            }
        }

        private List<List<String>> generateCombinations(List<String> items, int k) {
            List<List<String>> combinations = new ArrayList<>();
            combineHelper(combinations, new ArrayList<>(), items, k, 0);
            return combinations;
        }

        private void combineHelper(List<List<String>> combinations, List<String> temp, List<String> items, int k, int start) {
            if (temp.size() == k) {
                combinations.add(new ArrayList<>(temp));
                return;
            }
            for (int i = start; i < items.size(); i++) {
                temp.add(items.get(i));
                combineHelper(combinations, temp, items, k, i + 1);
                temp.remove(temp.size() - 1);
            }
        }
    }

    public static class ItemSetReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private int minFrequency;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            minFrequency = Integer.parseInt(conf.get("min.frequency", "1"));
        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            if (sum >= minFrequency) {
                context.write(key, new IntWritable(sum));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: FrequentItemSet <input path> <output path> <k> <min_frequency>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("min.frequency", args[3]);
        conf.set("k", args[2]);

        Job job = Job.getInstance(conf, "Frequent Item Set");
        job.setJarByClass(FrequentItemSet.class);
        job.setMapperClass(ItemSetMapper.class);
        job.setCombinerClass(ItemSetReducer.class);
        job.setReducerClass(ItemSetReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
