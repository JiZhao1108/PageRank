import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.text.DecimalFormat;

public class UnitSum {

    public static class passDataMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //input--> page\tsubPr
            //output--> page\tSubPr
            String[] line = value.toString().trim().split("\t");
            context.write(new Text(line[0]), new DoubleWritable(Double.parseDouble(line[1])));
        }
    }

    public static class betaMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        float beta;
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            //beta
            Configuration conf = context.getConfiguration();
            beta = conf.getFloat("beta", 0.2f);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //input--> page\tpr
            //output--> page\tpr*beta
            String[] page_pr =value.toString().trim().split("\t");
            double betaPr = Double.parseDouble(page_pr[1]) * beta;
            context.write(new Text(page_pr[0]), new DoubleWritable(betaPr));
        }
    }

    public static class sumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            //input--> page\t<SubPr, beta_subPr>
            //sum
            double sum = 0;
            for (DoubleWritable value : values) {
                sum += value.get();
            }

            DecimalFormat df = new DecimalFormat("#.0000");
            double outputValue = Double.parseDouble(df.format(sum));
            context.write(key, new DoubleWritable(outputValue));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setFloat("beta", Float.parseFloat(args[3]));

        Job job = Job.getInstance(conf);

        job.setJarByClass(UnitSum.class);

        ChainMapper.addMapper(job, passDataMapper.class, Object.class, Text.class, Text.class, DoubleWritable.class, conf);
        ChainMapper.addMapper(job, betaMapper.class, Text.class, DoubleWritable.class, Text.class, DoubleWritable.class, conf);
        job.setReducerClass(sumReducer.class);

//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, passDataMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, betaMapper.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }
}
