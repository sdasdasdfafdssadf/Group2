import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Access implements Writable {
    private String phone;
    private long upFlow;
    private long downFlow;
    private long totalFlow;

    // 构造函数
    public Access() {}

    // 带参构造函数，初始化对象
    public Access(String phone, long upFlow, long downFlow) {
        this.phone = phone;
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.totalFlow = upFlow + downFlow;
    }

    // Getter和Setter方法
    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
        updateTotalFlow();
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
        updateTotalFlow();
    }

    public long getTotalFlow() {
        return totalFlow;
    }

    // 更新总流量的方法
    private void updateTotalFlow() {
        this.totalFlow = this.upFlow + this.downFlow;
    }

    // 实现Writable接口的write方法
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(phone);
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(totalFlow);
    }

    // 实现Writable接口的readFields方法
    @Override
    public void readFields(DataInput in) throws IOException {
        phone = in.readUTF();
        upFlow = in.readLong();
        downFlow = in.readLong();
        totalFlow = in.readLong();
    }

    // 重写toString方法
    @Override
    public String toString() {
        return "Access{" +
                "phone='" + phone + '\'' +
                ", upFlow=" + upFlow +
                ", downFlow=" + downFlow +
                ", totalFlow=" + totalFlow +
                '}';
    }
}