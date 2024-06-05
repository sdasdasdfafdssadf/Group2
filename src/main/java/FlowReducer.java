import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, Access, Text, Access> {

    @Override
    protected void reduce(Text key, Iterable<Access> values, Context context) throws IOException, InterruptedException {
        long upFlowSum = 0;
        long downFlowSum = 0;

        // 遍历values，累加上行流量和下行流量
        for (Access access : values) {
            upFlowSum += access.getUpFlow();
            downFlowSum += access.getDownFlow();
        }

        // 创建Access对象存储计算结果
        Access result = new Access(key.toString(), upFlowSum, downFlowSum);
        context.write(key, result);
    }
}