package cn.zhangcl.hadoop;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
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


public class AggregationWithCombinerMRJob extends Configured implements Tool {
	
	public static final IntWritable RECORD = new IntWritable(0);
	public static final IntWritable ARRIVAL_DELAY = new IntWritable(1);
	public static final IntWritable ARRIVAL_ON_TIME = new IntWritable(2);
	public static final IntWritable DEPARTURE_DELAY = new IntWritable(3);
	public static final IntWritable DEPARTURE_ON_TIME = new IntWritable(4);
	public static final IntWritable IS_CANCELLED = new IntWritable(5);
	public static final IntWritable IS_DIVERTED = new IntWritable(6);
	
	public static final Text TYPE = new Text("TYPE");
	public static final Text VALUE = new Text("VALUE");

	public static class AggregationWithCombinerMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			//不处理带标题数据
			if(!AirlineDataUtils.isHeader(value)) {
				
				//以“，”分割数据
				String[] contents = value.toString().split(",");
				//获取每行数据中的月份
				String month = AirlineDataUtils.getMonth(contents);
				//获取到达延迟时间，默认为0，表示没有延迟
				int arrivalDelay = AirlineDataUtils.parseMinutes(AirlineDataUtils.getArrivalDelay(contents), 0);
				//获取起飞延迟时间，默认为0，表示没有延迟
				int departDalay = AirlineDataUtils.parseMinutes(AirlineDataUtils.getDepartureDelay(contents), 0);
				//航班是否取消
				boolean isCancelled = AirlineDataUtils.parseBoolean(AirlineDataUtils.getCancelled(contents), false);
				//航班是否转航
				boolean isDiverted = AirlineDataUtils.parseBoolean(AirlineDataUtils.getDiverted(contents), false);
				
				//记录航班记录总数，以计算比例
				context.write(new Text(month), getMapWritable(RECORD, new IntWritable(1)));
				//延迟到达
				if(arrivalDelay>0) {
					context.write(new Text(month), getMapWritable(ARRIVAL_DELAY, new IntWritable(1)));
				}
				//准点到达
				else {
					context.write(new Text(month), getMapWritable(ARRIVAL_ON_TIME, new IntWritable(1)));
				}
				//延迟起飞
				if(departDalay>0) {
					context.write(new Text(month), getMapWritable(DEPARTURE_DELAY, new IntWritable(1)));
				}
				//准点起飞
				else {
					context.write(new Text(month), getMapWritable(DEPARTURE_ON_TIME, new IntWritable(1)));
				}
				//航班取消
				if(isCancelled) {
					context.write(new Text(month), getMapWritable(IS_CANCELLED, new IntWritable(1)));
				}
				//航班转航
				if(isDiverted) {
					context.write(new Text(month), getMapWritable(IS_DIVERTED, new IntWritable(1)));
				}
			}
		}
		public MapWritable getMapWritable(IntWritable type, IntWritable value) {
			MapWritable map = new MapWritable();
			map.put(TYPE, type);
			map.put(VALUE, value);
			return map;
		}
	}
	public static class AggregationCombiner extends Reducer<Text, MapWritable, Text, MapWritable> {
		
		public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
			int totalRecords = 0;
			int arrivalOnTime = 0;
			int arrivalDelays = 0;
			int departureOnTime = 0;
			int departureDelays = 0;
			int cancellations = 0;
			int diversions = 0;
			for(MapWritable v:values) {
				IntWritable type = (IntWritable)v.get(TYPE);
				IntWritable value = (IntWritable)v.get(VALUE);
				//航班记录数
				if(type.equals(RECORD)) {
					totalRecords=totalRecords+value.get();
				}
				//准点到达航班数
				if(type.equals(ARRIVAL_ON_TIME)) {
					arrivalOnTime=arrivalOnTime+value.get();
				}
				//延迟到达航班数
				if(type.equals(ARRIVAL_DELAY)) {
					arrivalDelays=arrivalDelays+value.get();
				}
				//准点起飞航班数
				if(type.equals(DEPARTURE_ON_TIME)) {
					departureOnTime=departureOnTime+value.get();
				}
				//延迟起飞航班数
				if(type.equals(DEPARTURE_DELAY)) {
					departureDelays=departureDelays+value.get();
				}
				//取消航班数
				if(type.equals(IS_CANCELLED)) {
					cancellations=cancellations+value.get();
				}
				//转航航班数
				if(type.equals(IS_DIVERTED)) {
					diversions=diversions+value.get();
				}
			}
			context.write(key, getMapWritable(RECORD, new IntWritable(totalRecords)));
			context.write(key, getMapWritable(ARRIVAL_ON_TIME, new IntWritable(arrivalOnTime)));
			context.write(key, getMapWritable(ARRIVAL_DELAY, new IntWritable(arrivalDelays)));
			context.write(key, getMapWritable(DEPARTURE_ON_TIME, new IntWritable(departureOnTime)));
			context.write(key, getMapWritable(DEPARTURE_DELAY, new IntWritable(departureDelays)));
			context.write(key, getMapWritable(IS_CANCELLED, new IntWritable(cancellations)));
			context.write(key, getMapWritable(IS_DIVERTED, new IntWritable(diversions)));
			
		}
		public MapWritable getMapWritable(IntWritable type, IntWritable value) {
			MapWritable map = new MapWritable();
			map.put(TYPE, type);
			map.put(VALUE, value);
			return map;
		}
	}
	public static class AggregationWithCombinerReducer extends Reducer<Text, MapWritable, NullWritable, Text> {
		
		public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
			
			//月航班总记录
			double totalRecords = 0;
			//月准点到达航班数
			double arrivalOnTime = 0;
			//月延迟到达航班数
			double arrivalDelays = 0;
			//月准点起飞航班数
			double departureOnTime = 0;
			//月延迟起飞航班数
			double departureDelays = 0;
			//月航班取消数
			double cancellations = 0;
			//月航班转航数
			double diversions = 0;
			
			for(MapWritable v:values) {
				IntWritable type = (IntWritable)v.get(TYPE);
				IntWritable value = (IntWritable)v.get(VALUE);
				//航班记录数
				if(type.equals(RECORD)) {
					totalRecords=totalRecords+value.get();
				}
				//准点到达航班数
				if(type.equals(ARRIVAL_ON_TIME)) {
					arrivalOnTime=arrivalOnTime+value.get();
				}
				//延迟到达航班数
				if(type.equals(ARRIVAL_DELAY)) {
					arrivalDelays=arrivalDelays+value.get();
				}
				//准点起飞航班数
				if(type.equals(DEPARTURE_ON_TIME)) {
					departureOnTime=departureOnTime+value.get();
				}
				//延迟起飞航班数
				if(type.equals(DEPARTURE_DELAY)) {
					departureDelays=departureDelays+value.get();
				}
				//取消航班数
				if(type.equals(IS_CANCELLED)) {
					cancellations=cancellations+value.get();
				}
				//转航航班数
				if(type.equals(IS_DIVERTED)) {
					diversions=diversions+value.get();
				}
			}
			//指定精度
			DecimalFormat df = new DecimalFormat("0.0000");
			//计算比例然后输出
			StringBuilder out = new StringBuilder(key.toString());
			out.append(",").append(totalRecords);
			out.append(",").append(df.format(arrivalOnTime/totalRecords));
			out.append(",").append(df.format(arrivalDelays/totalRecords));
			out.append(",").append(df.format(departureOnTime/totalRecords));
			out.append(",").append(df.format(departureDelays/totalRecords));
			out.append(",").append(df.format(cancellations/totalRecords));
			out.append(",").append(df.format(diversions/totalRecords));
			
			context.write(NullWritable.get(), new Text(out.toString()));
		}
	}
	@Override
	public int run(String[] allArgs) throws Exception {
		//新建一个作业
        Job job = Job.getInstance(getConf());
        //作业入口类
        job.setJarByClass(AggregationMRJob.class);
        //指定输入、输出文件的类型
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //指定Mapper输出键、值的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        //指定Mapper作业类
        job.setMapperClass(AggregationWithCombinerMapper.class);
        //指定Reducer作业类
        job.setReducerClass(AggregationWithCombinerReducer.class);
        //指定Combiner作业类
        job.setCombinerClass(AggregationCombiner.class);
        //指定Reducer的数量。
        job.setNumReduceTasks(1);
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
        ToolRunner.run(new AggregationWithCombinerMRJob(), args);
    }
}
