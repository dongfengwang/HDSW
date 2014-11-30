package Search;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapred.lib.ChainMapper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class SQLSearch {
    public static class ExtractMappper extends MapReduceBase implements
            Mapper<LongWritable, Text, LongWritable, Conn1> {

        @Override
        public void map(LongWritable arg0, Text arg1,
                        OutputCollector<LongWritable, Conn1> arg2, Reporter arg3)
                throws IOException {
            String line = arg1.toString();
            String[] strs = line.split(";");
            Conn1 conn1 = new Conn1();
            conn1.orderKey = Long.parseLong(strs[0]);
            conn1.customer = Long.parseLong(strs[1]);
            conn1.state = strs[2];
            conn1.price = Double.parseDouble(strs[3]);
            LongWritable lw = new LongWritable(conn1.orderKey);
            arg2.collect(lw, conn1);
        }

    }

    private static class Conn1 implements WritableComparable<Conn1> {
        public long orderKey;
        public long customer;
        public String state;
        public double price;


        @Override
        public void readFields(DataInput in) throws IOException {
            orderKey = in.readLong();
            customer = in.readLong();
            state = Text.readString(in);
            price = in.readDouble();

        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(orderKey);
            out.writeLong(customer);
            Text.writeString(out, state);
            out.writeDouble(price);

        }

        @Override
        public int compareTo(Conn1 arg0) {
            // TODO Auto-generated method stub
            return 0;
        }

    }

    public static class Filter1Mapper extends MapReduceBase implements
            Mapper<LongWritable, Conn1, LongWritable, Conn2> {

        @Override
        public void map(LongWritable inKey, Conn1 c2,
                        OutputCollector<LongWritable, Conn2> collector, Reporter report)
                throws IOException {
            if (c2.state.equals("F")) {
                Conn2 inValue = new Conn2();
                inValue.customer = c2.customer;
                inValue.orderKey = c2.orderKey;
                inValue.price = c2.price;
                inValue.state = c2.state;
                collector.collect(inKey, inValue);
            }
        }

    }

    private static class Conn2 implements WritableComparable<Conn1> {
        public long orderKey;
        public long customer;
        public String state;
        public double price;

        @Override
        public void readFields(DataInput in) throws IOException {
            orderKey = in.readLong();
            customer = in.readLong();
            state = Text.readString(in);
            price = in.readDouble();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(orderKey);
            out.writeLong(customer);
            Text.writeString(out, state);
            out.writeDouble(price);
        }

        @Override
        public int compareTo(Conn1 arg0) {
            // TODO Auto-generated method stub
            return 0;
        }

    }

    public static class RegexMapper extends MapReduceBase implements
            Mapper<LongWritable, Conn2, LongWritable, Conn3> {

        @Override
        public void map(LongWritable inKey, Conn2 c3,
                        OutputCollector<LongWritable, Conn3> collector, Reporter report)
                throws IOException {
            c3.state = c3.state.replaceAll("F", "Find");
            Conn3 c2 = new Conn3();
            c2.customer = c3.customer;
            c2.orderKey = c3.orderKey;
            c2.price = c3.price;
            c2.state = c3.state;
            collector.collect(inKey, c2);
        }
    }

    private static class Conn3 implements WritableComparable<Conn1> {
        public long orderKey;
        public long customer;
        public String state;
        public double price;

        @Override
        public void readFields(DataInput in) throws IOException {
            orderKey = in.readLong();
            customer = in.readLong();
            state = Text.readString(in);
            price = in.readDouble();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(orderKey);
            out.writeLong(customer);
            Text.writeString(out, state);
            out.writeDouble(price);
        }

        @Override
        public int compareTo(Conn1 arg0) {
            // TODO Auto-generated method stub
            return 0;
        }

    }

    public static class LoadMapper extends MapReduceBase implements
            Mapper<LongWritable, Conn3, LongWritable, Conn3> {

        @Override
        public void map(LongWritable arg0, Conn3 arg1,
                        OutputCollector<LongWritable, Conn3> arg2, Reporter arg3)
                throws IOException {
            arg2.collect(arg0, arg1);
        }

    }

    public static void main(String[] args) {
        JobConf job = new JobConf(SQLSearch.class);
        job.setJobName("ProcessSample");
        job.setNumReduceTasks(0);
        job.setInputFormat(TextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        JobConf mapper1 = new JobConf();
        JobConf mapper2 = new JobConf();
        JobConf mapper3 = new JobConf();
        JobConf mapper4 = new JobConf();
        ChainMapper cm = new ChainMapper();
        cm.addMapper(job, ExtractMappper.class, LongWritable.class, Text.class,
                LongWritable.class, Conn1.class, true, mapper1);
        cm.addMapper(job, Filter1Mapper.class, LongWritable.class, Conn1.class,
                LongWritable.class, Conn2.class, true, mapper2);
        cm.addMapper(job, RegexMapper.class, LongWritable.class, Conn2.class,
                LongWritable.class, Conn3.class, true, mapper3);
        cm.addMapper(job, LoadMapper.class, LongWritable.class, Conn3.class,
                LongWritable.class, Conn3.class, true, mapper4);
        FileInputFormat.setInputPaths(job, new Path("orderData"));
        FileOutputFormat.setOutputPath(job, new Path("orderDataOutput"));
        Job job1;
        try {
            job1 = new Job(job);
            JobControl jc = new JobControl("test");
            jc.addJob(job1);
            jc.run();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
}