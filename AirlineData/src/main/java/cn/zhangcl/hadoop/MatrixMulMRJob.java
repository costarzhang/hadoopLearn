package cn.zhangcl.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MatrixMulMRJob extends Configured implements Tool {

	// 指定矩阵名称
	private static final String MATRIXFILE1 = "A";
	private static final String MATRIXFILE2 = "B";

	public static class MatrixMulMRMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

		// 矩阵文件名
		private String matrixFile1 = null;
		private String matrixFile2 = null;

		@Override
		protected void setup(Context context) {
			// 从命令行获取参数
			matrixFile1 = context.getConfiguration().get(MATRIXFILE1);
			matrixFile2 = context.getConfiguration().get(MATRIXFILE2);
		}

		/**
		 * 
		 * @param key     按行读取矩阵文件，key为行偏移量
		 * @param value   每行的内容
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
		 *      org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// 计算行号，偏移量（key）为0,4,8...,所以key/4+1就是对应的行号
			LongWritable i = new LongWritable(key.get() / 4 + 1);
			// 矩阵文件名
			String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
			// 以一个或者多个空白符来分割数据
			String[] arr = value.toString().split("\\s+");
			// 发出去N个互不相同的key
			// 处理第一个矩阵
			if (filename.equals(matrixFile1)) {
				for (int j = 1; j <= arr.length; j++) {
					double v = Double.parseDouble(arr[j - 1]);
					// 元素为0时不发出，对于稀疏矩阵这样效率会比较高
					if (v != 0) {
						// 输出键值形如：(j,(A,i,v))，i为行索引，j为列索引，v为具体值
						context.write(new LongWritable(j), new Text(MATRIXFILE1 + "\t" + i.toString() + "\t" + v));
					}
				} // 处理第二个矩阵
			} else if (filename.equals(matrixFile2)) {
				for (int j = 1; j <= arr.length; j++) {
					double v = Double.parseDouble(arr[j - 1]);
					// 元素为0时不发出，对于稀疏矩阵这样效率会比较高
					if (v != 0) {
						// 输出键值形如：(i,(B,j,v)),i为行索引，j为列索引，v为具体值
						context.write(i, new Text(MATRIXFILE2 + "\t" + j + "\t" + v));
					}
				}
			}
		}

		@Override
		protected void cleanup(Context context) {
		}
	}

	public static class MatrixMulMRReducer extends Reducer<LongWritable, Text, Text, DoubleWritable> {

		@Override
		protected void reduce(LongWritable key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			// 存储来自A矩阵的数据：(j,(A,i,v))
			Map<Integer, Double> listA = new HashMap<Integer, Double>();
			// 存储来自B矩阵的数据：(i,(B,j,v))
			Map<Integer, Double> listB = new HashMap<Integer, Double>();
			Iterator<Text> itr = value.iterator();

			while (itr.hasNext()) {
				// 按空白符分割数据
				String[] arr = itr.next().toString().split("\\s+");
				// 矩阵名称A或B
				String matrixTag = arr[0];

				// 数据在A矩阵中的行索引i或在B矩阵中的列索引j
				int pos = Integer.parseInt(arr[1]);
				// 数据的具体值
				double v = Double.parseDouble(arr[2]);
				// 数据属于矩阵A,存储为A[i]=v,记录数据在A矩阵中的行索引
				if (MATRIXFILE1.equals(matrixTag)) {
					listA.put(pos, v);
					// 数据属于矩阵B，存储为B[j]=v,记录数据在B矩阵中的列索引
				} else if (MATRIXFILE2.equals(matrixTag)) {
					listB.put(pos, v);
				}
			}
			// 在此需要进行N*N次的乘法
			for (Entry<Integer, Double> entryA : listA.entrySet()) {
				// 数据行索引
				int posA = entryA.getKey();
				double valA = entryA.getValue();
				for (Entry<Integer, Double> entryB : listB.entrySet()) {
					// 数据列索引
					int posB = entryB.getKey();
					double valB = entryB.getValue();
					// 行*列
					double production = valA * valB;
					// 输出键值对形如((i,j),listA[i]∗listB[j])
					context.write(new Text(posA + "\t" + posB), new DoubleWritable(production));
				}
			}
		}
	}

	public static class SumMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// 按空白符分割
			String[] arr = value.toString().split("\\s+");
			// 确保得到形如((i,j),listA[i]∗listB[j])的键值
			if (arr.length == 3) {
				context.write(new Text(arr[0] + "\t" + arr[1]), new DoubleWritable(Double.parseDouble(arr[2])));
			}
		}
	}

	public static class SumCombiner extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		@Override
		protected void reduce(Text key, Iterable<DoubleWritable> value, Context context)
				throws IOException, InterruptedException {
			double sum = 0;
			Iterator<DoubleWritable> itr = value.iterator();
			// 计算各个位置的数据值，坐标（i,j）相同的决定同一位置的值
			while (itr.hasNext()) {
				sum += itr.next().get();
			}
			context.write(key, new DoubleWritable(sum));
		}
	}

	public static class SumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		@Override
		protected void reduce(Text key, Iterable<DoubleWritable> value, Context context)
				throws IOException, InterruptedException {
			double sum = 0;
			Iterator<DoubleWritable> itr = value.iterator();
			while (itr.hasNext()) {
				sum += itr.next().get();
			}
			context.write(key, new DoubleWritable(sum));
		}
	}

	/**
	 * matrix1 * matrix2 = product matrixFile1：输入文件，m行q列 matrixFile2：输入文件，q行n列
	 * productFile：输出文件，m行n列 各列用空白符分隔。
	 * 
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		@SuppressWarnings("unused")
		Configuration conf = new Configuration();
		ToolRunner.run(new MatrixMulMRJob(), args);
	}

	@Override
	public int run(String[] args) throws Exception {

		// 从命令行输入矩阵A、B的文件，以及输出矩阵的文件
		String matrixFile1 = args[0];
		String matrixFile2 = args[1];
		String productFile = args[2];

		Configuration conf = new Configuration();

		FileSystem fs = FileSystem.get(conf);
		Path inFile1 = new Path(matrixFile1);
		Path inFile2 = new Path(matrixFile2);

		// 映射矩阵A从第一个文件读取，矩阵B从第二个文件读取
		conf.set(MATRIXFILE1, inFile1.getName());
		conf.set(MATRIXFILE2, inFile2.getName());
		// 指定临时数据文件路径
		Path midFile = new Path("/product_tmp");
		// 指定输出文件路路径
		Path outFile = new Path(productFile);

		{
			// 创建一个job作业
			Job job = Job.getInstance(conf);
			job.setJobName("MatrixMultiplicationMRJob1");

			// 指定作业入口类
			job.setJarByClass(MatrixMulMRJob.class);

			// 指定输入文件
			FileInputFormat.addInputPath(job, inFile1);
			FileInputFormat.addInputPath(job, inFile2);

			// 指定作业Mapper类
			job.setMapperClass(MatrixMulMRMapper.class);

			// 指定Mapper输出键值类型
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(Text.class);

			// 指定输出文件
			FileOutputFormat.setOutputPath(job, midFile);

			// 指定作业Reducer类
			job.setReducerClass(MatrixMulMRReducer.class);
			// 指定Reducer数量
			job.setNumReduceTasks(12);
			// 指定输出键值类型
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);
			// 执行程序*/
			job.waitForCompletion(true);
		}
		// 第二阶段,将第一阶段输出中有相同 key 的数据求和
		{
			Job productionJob2 = Job.getInstance(conf);
			productionJob2.setJobName("MatrixMultiplicationMRJob2");
			productionJob2.setJarByClass(MatrixMulMRJob.class);

			FileInputFormat.setInputPaths(productionJob2, midFile);
			// 指定作业Mapper类
			productionJob2.setMapperClass(SumMapper.class);
			// 指定Mapper输出键值类型
			productionJob2.setMapOutputKeyClass(Text.class);
			productionJob2.setMapOutputValueClass(DoubleWritable.class);
			FileOutputFormat.setOutputPath(productionJob2, outFile);
			// 指定作业Combiner类
			productionJob2.setCombinerClass(SumCombiner.class);
			// 指定作业Reducer类
			productionJob2.setReducerClass(SumReducer.class);
			// 指定Reducer数量
			productionJob2.setNumReduceTasks(1);
			// 指定输出键值类型
			productionJob2.setOutputKeyClass(Text.class);
			productionJob2.setOutputValueClass(DoubleWritable.class);
			// 执行作业
			productionJob2.waitForCompletion(true);
		}
		fs.delete(outFile, true);
		return 0;
	}
}