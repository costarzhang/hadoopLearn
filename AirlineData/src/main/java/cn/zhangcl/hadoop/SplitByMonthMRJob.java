package cn.zhangcl.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SplitByMonthMRJob extends Configured implements Tool{

	public static class SplitByMonthMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if(!AirlineDataUtils.isHeader(value)) {
				int month = Integer.parseInt(AirlineDataUtils.getMonth(value.toString().split(",")));
				//输出键值对：月份/记录
				context.write(new IntWritable(month), value);
			}
		}
	}
	
	public static class SplitByMonthReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
		
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for(Text output : values) {
				context.write(NullWritable.get(), new Text(output.toString()));
			}
		}
	}
	
	public static class MonthPartioner extends Partitioner<IntWritable, Text> {
		@Override
		public int getPartition(IntWritable month, Text value, int numPartitions) {
			return (month.get()-1);
		}
	}
	@Override
	public int run(String[] allArgs) throws Exception {
		//新建一个作业
        Job job = Job.getInstance(getConf());
        //作业入口类
        job.setJarByClass(SplitByMonthMRJob.class);
        //指定输入、输出文件的类型
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //指定Mapper输出键、值的类型
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        //指定Mapper作业类
        job.setMapperClass(SplitByMonthMapper.class);
        //指定Reducer作业类
        job.setReducerClass(SplitByMonthReducer.class);
        //指定Partitioner作业类
        job.setPartitionerClass(MonthPartioner.class);
        //指定Reducer的数量,按月分离，每个月一个reducer
        job.setNumReduceTasks(12);
        //运行参数获取
        String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();
        //输入、输出文件路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        //打印程序执行过程
        boolean status = job.waitForCompletion(true);
        if(status) {
            return 0;
        }
        else {
            return 1;
        }
	}
	public static void main(String[] args) throws Exception {
        @SuppressWarnings("unused")
		Configuration conf = new Configuration();
        ToolRunner.run(new SplitByMonthMRJob(), args);
    }
}
