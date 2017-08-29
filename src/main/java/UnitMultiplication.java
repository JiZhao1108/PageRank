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
import java.util.ArrayList;
import java.util.List;

public class UnitMultiplication {

    public static class transitionMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //input--> fromPage\ttoPage1,toPage2,toPage3...
            //output--> fromPage\ttoPage1=prob1,toPage2=prob2,toPage3=prob3...
            String[] from_to = value.toString().trim().split("\t");
            if (from_to.length != 2 || from_to[1].trim().equals("")) {//dead ends
                return;
            }
            String fromPages = from_to[0];
            String[] toPages = from_to[1].split(",");
            for (int i = 0; i < toPages.length; i++) {
                String outputValue = toPages[i] + "=" + (double)1/(toPages.length + 1);
                context.write(new Text(fromPages), new Text(outputValue));
            }
        }
    }

    public static class prMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //input--> page\tpr
            //output--> page\tpr
            String[] page_pr = value.toString().trim().split("\t");
            context.write(new Text(page_pr[0]), new Text(page_pr[1]));
        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable> {

        float beta;
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            //beta
            Configuration conf = context.getConfiguration();
            beta = conf.getFloat("beta", 0.2f);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //input--> fromPage\t<toPage1=prob1,toPage2=prob2,...pr>
            //output--> toPage\tsubPr
            List<String> probCells = new ArrayList<String>();
            double prCell = 0;

            for (Text value : values) {
                if (value.toString().contains("=")) {
                    probCells.add(value.toString());
                }
                else {
                    prCell = Double.parseDouble(value.toString());
                }
            }
            for (String probCell : probCells) {
                double prob = Double.parseDouble(probCell.trim().split("=")[1]);
                double subPr = prob * prCell * (1 - beta);
                context.write(new Text(probCell.trim().split("=")[0]), new DoubleWritable(subPr));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setFloat("beta", Float.parseFloat(args[3]));

        Job job = Job.getInstance(conf);

        job.setJarByClass(UnitMultiplication.class);

        ChainMapper.addMapper(job, transitionMapper.class, Object.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job, prMapper.class, Object.class, Text.class, Text.class, Text.class, conf);
        job.setMapperClass(transitionMapper.class);
        job.setMapperClass(transitionMapper.class);
        job.setReducerClass(MultiplicationReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);


        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, transitionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, prMapper.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }
}
