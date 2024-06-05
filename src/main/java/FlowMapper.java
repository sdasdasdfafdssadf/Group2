import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, Access> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 假设日志格式为: <timestamp>\t<phone_number>\t<device_id>\t<ip_address>\t...<up_flow>\t<down_flow>\t<total_flow>\t<http_status>
        String[] parts = value.toString().split("\t");
        if (parts.length > 10) { // 确保字段数量足够
            String phone = parts[1]; // 手机号码是第二个字段
            long upFlow = Long.parseLong(parts[parts.length - 3]); // 上行流量是倒数第三个字段
            long downFlow = Long.parseLong(parts[parts.length - 2]); // 下行流量是倒数第二个字段

            // 创建 Access 对象并写入上下文
            Access access = new Access(phone, upFlow, downFlow);
            context.write(new Text(phone), access);
        } else {
            context.getCounter("LogProcessing", "Invalid Records").increment(1);
        }
    }
}