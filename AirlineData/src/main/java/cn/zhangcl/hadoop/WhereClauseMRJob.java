package cn.zhangcl.hadoop;

import java.io.IOException;
import cn.zhangcl.hadoop.AirlineDataUtils;
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


public class WhereClauseMRJob extends Configured implements Tool {

	public static class WhereClauseMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
		
		private int delayInMinutes = 0;
		public void setup(Context context) {
			//从context实例中获取航班延迟的分钟数，用于筛选出延迟超过此值的航班，可由用户指定，默认值设为1
			this.delayInMinutes = context.getConfiguration().getInt("map.where.delay", 1);
		}
		
		public void  map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//不处理带有标题的数据
			if(AirlineDataUtils.isHeader(value)) {
				return;
			}
			//从原始数据集中获取需要的行
			String[] arr = AirlineDataUtils.getSelectResultsPerRow(value);
			//起飞延迟时间
			String depDel = arr[8];
			//到达延迟时间
			String arrDel = arr[9];
			//将延迟时间转为整型，默认值为0，代表没有延迟
			int iDepDel = AirlineDataUtils.parseMinutes(depDel, 0);
			int iArrDel = AirlineDataUtils.parseMinutes(arrDel, 0);
			
			//以","拆分每一行文本，存储到一个数组中
			StringBuilder out = AirlineDataUtils.mergeStringArray(arr, ",");
			//延迟发生在七点和目的地
			if(iDepDel >=this.delayInMinutes && iArrDel >=this.delayInMinutes) {
				out.append(",").append("B");
				context.write(NullWritable.get(), new Text(out.toString()));
			}
			//在起点发生延迟
			else if(iDepDel >=this.delayInMinutes) {
				out.append(",").append("O");
				context.write(NullWritable.get(), new Text(out.toString()));
			}
			//在终点发生了延迟
			else if(iArrDel >=this.delayInMinutes) {
				out.append(",").append("D");
				context.write(NullWritable.get(), new Text(out.toString()));
			}
		}
	}
	@Override
	public int run(String[] allArgs) throws Exception {
		//新建一个作业
        Job job = Job.getInstance(getConf());
        //作业入口类
        job.setJarByClass(WhereClauseMRJob.class);
        //指定输入、输出文件的类型
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //指定输出键、值的类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        //指定Mapper作业类
        job.setMapperClass(WhereClauseMapper.class);
        //指定Reducer的数量，这里不需要reducer.
        job.setNumReduceTasks(0);
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
        ToolRunner.run(new WhereClauseMRJob(), args);
    }
}
