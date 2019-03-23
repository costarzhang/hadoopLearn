package cn.zhangcl.hadoop;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AggregationMRJob extends Configured implements Tool {

	// 用0-6的代码代表六中状态
	public static final IntWritable RECORD = new IntWritable(0);
	public static final IntWritable ARRIVAL_DELAY = new IntWritable(1);
	public static final IntWritable ARRIVAL_ON_TIME = new IntWritable(2);
	public static final IntWritable DEPARTURE_DELAY = new IntWritable(3);
	public static final IntWritable DEPARTURE_ON_TIME = new IntWritable(4);
	public static final IntWritable IS_CANCELLED = new IntWritable(5);
	public static final IntWritable IS_DIVERTED = new IntWritable(6);

	public static class AggregationMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// 不处理带标题数据
			if (!AirlineDataUtils.isHeader(value)) {

				// 以“，”分割数据
				String[] contents = value.toString().split(",");
				// 获取每行数据中的月份
				String month = AirlineDataUtils.getMonth(contents);
				// 获取到达延迟时间，默认为0，表示没有延迟
				int arrivalDelay = AirlineDataUtils.parseMinutes(AirlineDataUtils.getArrivalDelay(contents), 0);
				// 获取起飞延迟时间，默认为0，表示没有延迟
				int departDalay = AirlineDataUtils.parseMinutes(AirlineDataUtils.getDepartureDelay(contents), 0);
				// 航班是否取消
				boolean isCancelled = AirlineDataUtils.parseBoolean(AirlineDataUtils.getCancelled(contents), false);
				// 航班是否转航
				boolean isDiverted = AirlineDataUtils.parseBoolean(AirlineDataUtils.getDiverted(contents), false);

				// 记录航班记录总数，以计算比例
				context.write(new Text(month), RECORD);
				// 延迟到达
				if (arrivalDelay > 0) {
					context.write(new Text(month), ARRIVAL_DELAY);
				}
				// 准点到达
				else {
					context.write(new Text(month), ARRIVAL_ON_TIME);
				}
				// 延迟起飞
				if (departDalay > 0) {
					context.write(new Text(month), DEPARTURE_DELAY);
				}
				// 准点起飞
				else {
					context.write(new Text(month), DEPARTURE_ON_TIME);
				}
				// 航班取消
				if (isCancelled) {
					context.write(new Text(month), IS_CANCELLED);
				}
				// 航班转航
				if (isDiverted) {
					context.write(new Text(month), IS_DIVERTED);
				}
			}
		}
	}

	public static class AggregationReducer extends Reducer<Text, IntWritable, NullWritable, Text> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			// 月航班总记录
			double totalRecords = 0;
			// 月准点到达航班数
			double arrivalOnTime = 0;
			// 月延迟到达航班数
			double arrivalDelays = 0;
			// 月准点起飞航班数
			double departureOnTime = 0;
			// 月延迟起飞航班数
			double departureDelays = 0;
			// 月航班取消数
			double cancellations = 0;
			// 月航班转航数
			double diversions = 0;

			for (IntWritable v : values) {
				// 统计月航班总数
				if (v.equals(RECORD)) {
					totalRecords++;
				}
				// 统计月航班准点到达数
				if (v.equals(ARRIVAL_ON_TIME)) {
					arrivalOnTime++;
				}
				// 统计月航班延迟到达数
				if (v.equals(ARRIVAL_DELAY)) {
					arrivalDelays++;
				}
				// 统计月航班准点起飞数
				if (v.equals(DEPARTURE_ON_TIME)) {
					departureOnTime++;
				}
				// 统计月航班延迟起飞数
				if (v.equals(DEPARTURE_DELAY)) {
					departureDelays++;
				}
				// 统计月航班取消数
				if (v.equals(IS_CANCELLED)) {
					cancellations++;
				}
				// 统计月航班转航数
				if (v.equals(IS_DIVERTED)) {
					diversions++;
				}
			}
			// 指定精度
			DecimalFormat df = new DecimalFormat("0.0000");
			// 计算比例然后输出
			StringBuilder out = new StringBuilder(key.toString());
			out.append(",").append(totalRecords);
			out.append(",").append(df.format(arrivalOnTime / totalRecords));
			out.append(",").append(df.format(arrivalDelays / totalRecords));
			out.append(",").append(df.format(departureOnTime / totalRecords));
			out.append(",").append(df.format(departureDelays / totalRecords));
			out.append(",").append(df.format(cancellations / totalRecords));
			out.append(",").append(df.format(diversions / totalRecords));

			context.write(NullWritable.get(), new Text(out.toString()));
		}
	}

	@Override
	public int run(String[] allArgs) throws Exception {
		// 新建一个作业
		Job job = Job.getInstance(getConf());
		// 作业入口类
		job.setJarByClass(AggregationMRJob.class);
		// 指定输入、输出文件的类型
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		// 指定Mapper输出键、值的类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		// 指定Mapper作业类
		job.setMapperClass(AggregationMapper.class);
		// 指定Reducer作业类
		job.setReducerClass(AggregationReducer.class);
		// 指定Reducer的数量。
		job.setNumReduceTasks(1);
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
		ToolRunner.run(new AggregationMRJob(), args);
	}
}
