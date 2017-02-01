import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class
LanguageModel {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		int threashold;

		@Override
		public void setup(Context context) {
			// how to get the threashold parameter from the configuration?
			Configuration c = context.getConfiguration();
			this.threashold = c.getInt("threashold", 20);
		}

		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if((value == null) || (value.toString().trim()).length() == 0) {
				return;
			}
			//this is cool\t20
			String line = value.toString().trim();
			
			String[] wordsPlusCount = line.split("\t");
			if(wordsPlusCount.length < 2) {
				return;
			}
			
			String[] words = wordsPlusCount[0].split("\\s+");
			int count = Integer.valueOf(wordsPlusCount[1]);
			if (count < this.threashold) return;

			//this is --> cool = 20
			StringBuilder sb = new StringBuilder();
			for(int i = 0; i < words.length-1; i++) {
				sb.append(words[i]).append(" ");
			}
			String oKey = sb.toString().trim();
			String oValue = words[words.length-1];

			if(!((oKey == null) || (oKey.length() <1))) {
				context.write(new Text(oKey), new Text(oValue + "=" + count));
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

		int n;
		// get the n parameter from the configuration
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			n = conf.getInt("n", 5);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			TreeMap<Integer, List<String>> tm = new TreeMap<Integer, List<String>>(Collections.reverseOrder());

			for(Text val: values) {
				String cur = val.toString().trim();
				String word = cur.split("=")[0].trim();
				int count = Integer.parseInt(cur.split("=")[1].trim());
				if(tm.containsKey(count)) {
					tm.get(count).add(word);
				}
				else {
					List<String> list = new ArrayList<String>();
					list.add(word);
					tm.put(count, list);
				}
			}

			//format: <50, <girl, bird>> <60, <boy...>>
			Iterator<Integer> iter = tm.keySet().iterator();
			for(int j=0; iter.hasNext() && j<n; ) {
				int keyCount = iter.next();
				List<String> words = tm.get(keyCount);
				for(String w: words) {
					context.write(new DBOutputWritable(key.toString(), w, keyCount),NullWritable.get());
					j++;
				}
			}

		}
	}
}
