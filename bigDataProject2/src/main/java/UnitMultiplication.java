import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UnitMultiplication {

    public static class TransitionMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //input format: fromPage\t toPage1,toPage2,toPage3
            //target: build transition matrix unit -> fromPage\t toPage=probability
            String line = value.toString().trim();
            String[] fromTo = line.split("\t");

            if(fromTo.length == 1 || fromTo[1].trim().equals("")) {
                return;
            }
            String from = fromTo[0];
            String[] tos = fromTo[1].split(",");
            for (String to: tos) {
                context.write(new Text(from), new Text(to + "=" + (double)1/tos.length));
            }
        }
    }

    public static class PRMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //input format: Page\t PageRank
            //target: write to reducer
            String[] pr = value.toString().trim().split("\t");
            context.write(new Text(pr[0]), new Text(pr[1]));

        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {


        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            //input key = fromPage value=<toPage=probability..., pageRank>
            //target: get the unit multiplication
            List<String> transUnit = new ArrayList<String>();
            double probUnit = 0;
            for (Text value: values){
                if (value.toString().contains("=")){
                    transUnit.add(value.toString());
                }else{
                    probUnit = Double.parseDouble(value.toString());
                }
            }

            for (String unit: transUnit){
                String opKey = unit.split("=")[0];
                double p = Double.parseDouble(unit.split("=")[1]);
                String opValue = String.valueOf(p*probUnit);
                context.write(new Text(opKey), new Text(opValue));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(UnitMultiplication.class);

        // chain two mapper classes
        ChainMapper.addMapper(job,TransitionMapper.class, Object.class, Text.class,
                Text.class, Text.class, conf);
        ChainMapper.addMapper(job, PRMapper.class, Object.class, Text.class,
                Text.class, Text.class, conf);

        job.setReducerClass(MultiplicationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PRMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }

}
