package cn.zhangcl.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class SelectClauseMRJob extends Configured implements Tool {

	/**
	 * 
	 * @ClassName: SelectClauseMapper
	 * @Description:TODO(Mapper类)
	 * @author: cz
	 * @date: Mar 21, 2019 10:46:46 PM
	 */
	public static class SelectClauseMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

		/**
		 * 
		 * @param key     按行读取数据，key为行偏移量
		 * @param value   每行的数据
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
		 *      org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// 跳过带有标题的行
			if (!AirlineDataUtils.isHeader(value)) {
				StringBuilder output = AirlineDataUtils.mergeStringArray(
						// 以","拆分每一行文本，存储到一个数组中
						AirlineDataUtils.getSelectResultsPerRow(value), ",");
				// 将内容输出到HDFS输出文件
				context.write(NullWritable.get(), new Text(output.toString()));
			}
		}
	}

	public int run(String[] allArgs) throws Exception {

		// 新建一个作业
		Job job = Job.getInstance(getConf());
		// 作业入口类
		job.setJarByClass(SelectClauseMRJob.class);
		// 指定输入、输出文件的类型
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		// 指定输出键、值的类型
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		// 指定Mapper作业类
		job.setMapperClass(SelectClauseMapper.class);
		// 指定Reducer的数量，这里不需要reducer.
		job.setNumReduceTasks(0);
		// 运行参数获取
		String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();
		// 输入、输出文件路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 打印程序执行过程
		boolean status = job.waitForCompletion(true);
		if (status) {
			return 0;
		} else {
			return 1;
		}
	}

	public static void main(String[] args) throws Exception {
		@SuppressWarnings("unused")
		Configuration conf = new Configuration();
		ToolRunner.run(new SelectClauseMRJob(), args);
	}
}
