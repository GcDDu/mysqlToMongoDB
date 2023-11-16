package Demo;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = null;
        // 本机执行环境
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 远程执行环境
//        env = StreamExecutionEnvironment.createRemoteEnvironment("192.168.137.99", 8081);
        env.enableCheckpointing(3000l).setParallelism(2);
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        // CREATE TABLE source_test
        tableEnv.executeSql(sourceDDL());
        // CREATE TABLE sink_test
        tableEnv.executeSql(sinkDDL());
        // CREATE TABLE sink_test_second
        tableEnv.executeSql(sinkDDLOfSecondDb());
        // 将source_test同步到sink_test和sink_test_second
        tableEnv.getConfig().set("pipeline.name", "Flink Demo - To sink_test");         // 设置job名称
        tableEnv.executeSql("insert into sink_test select * from source_test;");
        tableEnv.getConfig().set("pipeline.name", "Flink Demo - To sink_test_second");  // 设置job名称
        tableEnv.executeSql("insert into sink_test_second select * from source_test;");

    }

    public static String sourceDDL() {
        String sourceDDL = "CREATE TABLE source_test (\n" +
                "  user_id STRING,\n" +
                "  user_name STRING,\n" +
                "  PRIMARY KEY (user_id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'mysql-cdc',\n" +
                "   'hostname' = '192.168.3.31',\n" +
                "   'port' = '3306',\n" +
                "   'username' = 'root',\n" +
                "   'password' = '******',\n" +
                "   'database-name' = 'flink_source',\n" +
                "   'table-name' = 'source_test'\n" +
                ");";
        return sourceDDL;
    }

    public static String sinkDDL() {
        String sinkDDL = "CREATE TABLE sink_test (\n" +
                "  user_id STRING,\n" +
                "  user_name STRING,\n" +
                "  PRIMARY KEY (user_id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://192.168.3.31:3306/flink_sink',\n" +
                "   'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                "   'username' = 'root',\n" +
                "   'password' = '******',\n" +
                "   'table-name' = 'sink_test'\n" +
                ");";
        return sinkDDL;
    }

    public static String sinkDDLOfSecondDb() {
        String sinkDDL = "CREATE TABLE sink_test_second (\n" +
                "  user_id STRING,\n" +
                "  user_name STRING,\n" +
                "  PRIMARY KEY (user_id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://192.168.3.31:3306/flink_sink_second',\n" +
                "   'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                "   'username' = 'root',\n" +
                "   'password' = '******',\n" +
                "   'table-name' = 'sink_test'\n" +
                ");";
        return sinkDDL;
    }
}
