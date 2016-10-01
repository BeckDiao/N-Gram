import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	
	List<String> stopWords = new ArrayList<>();
	
	
	//setup只会在Initialization的时候被调用一次，然后后面再每次调用map 函数时不会再被调用
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		
		//-------------------------
				//OverView
				//split
				//generate database <stopwords>
				//Mapper的输出中的key-value pair 应为<单词，文件名>
				
				//-------------------
				//configuration 告诉mapper 我们的文件放在哪个位置,是从context里面读进来的。
				//然后在configuration里面进行设置
				Configuration conf = context.getConfiguration();
				String filePath = conf.get("filePath"); //"filePath" 是一个key => 具体地址，如/input/stopwords.txt
				
				//把stopwords load 到内存当中 -> 读这个文件
				//filePath 指向的路径是通过terminal传进来的，后面会在Driver里面设置
				Path path = new Path("hdfs:" + filePath); // => "hdfs:/input/stopwords.txt"
				
				FileSystem fs = FileSystem.get(new Configuration());
				
				//BufferedReader用来读file，这里是读stopword,会把整个file load 到buffer里面来
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
				String line;
				line = br.readLine();
				while (line != null) {
					//把读进来的file 放到一个list或者set中 -->存到内存里，后面用来一遍遍的 check
					stopWords.add(line.toLowerCase().trim());
					line = br.readLine();
				}
				
				//至此，stopwords 统计成功
		
		
		
		
		
		
	}
	

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.map(key, value, context);
		
		
		//因为key-value pair 应为<stopword， 本单词所在的文件>，所以，读到单词后还应该找到单词所在的文件
		//context相当于 map-reduce 与 HDFS 交互的一个接口
		//FileSplit .getInputSplit() 后相当于得到一个文件的block，然后getPath().getName()
		//可以把下面一行当做获得文件的一个prototype
		String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		Text name = new Text(fileName);//Mapper 要输出的value get✔
		
		//--------------------------------
		//以下是split的过程
		
		//还是像wordCount一样用tokenizer来实现split
		StringTokenizer tokenizer = new StringTokenizer(value.toString()); //此value便是需要读的文档的内容
		while (tokenizer.hasMoreTokens()) {
			String currentWord = tokenizer.nextToken().toString().toLowerCase();
			//把除了字母之外其他的全部都去掉 => 得到一个clear 的单词
			currentWord.replaceAll("[a-zA-Z]", "");
			//把stopwords去掉
			if (!stopWords.contains(currentWord)) {
				context.write(new Text(currentWord), name);
			}
		}
			
	}

	
	


}
