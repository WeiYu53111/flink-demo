package sql;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2022/3/5 0005
 */
public class Demo {


    public static void main(String[] args) throws Exception {

        Configuration c = new Configuration();
        c.setString("execution.target", "remote");
        c.setString("jobmanager.rpc.address", "120.77.54.72");
        c.setString("rest.port", "22088");
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(c);
        StreamExecutionEnvironment env = new StreamExecutionEnvironment(c);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table source = tableEnv.fromValues(Arrays.asList(1, 2, 3, 4, 5, 6));
        tableEnv.createTemporaryView("t", source);

        TableResult result = tableEnv.executeSql("select * from t");
        result.print();

        //env.execute();


        //env.execute();
        //String sessionId = "wy";

    }


}
