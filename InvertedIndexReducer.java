import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text>{

	@Override
	protected void reduce(final Text key, final Iterable<Text> values, final Context context)
			throws IOException, InterruptedException {
		
		//进入reducer的数据格式：<keyword, <Doc1, Doc1, Doc1, Doc2, Doc2...>>
		//出reducer的数据格式应为： <keyword, Doc1, Doc2...>
		
		//so, 第一步，去重
		String lastBook = null;
		StringBuilder sb = new StringBuilder();
		int count = 0;
		int threshold = 100;
		
		for (Text value : values) {
			if (lastBook != null && value.toString().trim().equals(lastBook)) {
				count++;
				continue;
			}
			
			// 上一本书的这个词已经全部统计完毕，开始统计下一本书的时候
			if (lastBook != null && count < threshold) {
				count = 1;
				lastBook = value.toString().trim();
				continue;
			} else {
				count = 1;
				sb.append(lastBook);
				sb.append("/t");
			}
			
			//lastBook == null
			if (lastBook == null) {
				lastBook = value.toString().trim();
				count++;
			}
		}
		//最后一组doc x 统计过后还没有决定加不加进StringBuilder
		if (lastBook != null && count > threshold) {
			sb.append(lastBook);
		}
		
		//finally, 写出的过程
		if (!sb.toString().trim().equals("")) {
			context.write(key, new Text(sb.toString()));
		}
		
	}

}
