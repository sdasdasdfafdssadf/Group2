import org.apache.hadoop.mapreduce.Partitioner;
import org.w3c.dom.Text;

// FlowPartitioner类继承自Partitioner类，用于根据手机号前缀对数据进行分区
public class FlowPartitioner extends Partitioner<Text, Access> {
    // 重写getPartition方法，根据手机号前缀将数据分到不同的reduce任务中
    @Override
    public int getPartition(Text key, Access value, int numReduceTasks) {
        // 获取手机号前两位作为前缀
        String phonePrefix = key.toString().substring(0, 2);

        // 根据不同的手机号前缀返回对应的reduce任务编号
        return switch (phonePrefix) {
            case "13" -> 0;
            case "15" -> 1;
            default -> 2;
        };
    }
}
