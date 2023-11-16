package test;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;
import org.apache.flink.connector.jdbc.catalog.MySqlCatalog;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.DefaultCatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * flink cdc 整库同步
 */
public class FlinkCdcMultiSyncJdbc {

    private static final Logger log = LoggerFactory.getLogger(FlinkCdcMultiSyncJdbc.class);

    public static void main(String[] args) throws Exception {

        // source端连接信息
        String userName = "root";
        String passWord = "mysql123";
        String host = "localhost";
        String db = "world";
        // 如果是整库，tableList = ".*"
//        String tableList = "lidy.nlp_category,lidy.nlp_classify_man_made3";
        String tableList = ".*";
        int port = 3306;

        // sink连接信息模板
        String sink_url = "mongodb://localhost:27017";
//        String sink_username = "root";
//        String sink_password = "18772247265Ldy@";

        String connectorWithBody =
                " with (\n" +
                        " 'connector' = 'jdbc',\n" +
                        " 'url' = '${sink_url}',\n" +
//                        " 'username' = '${sink_username}',\n" +
//                        " 'password' = '${sink_password}',\n" +
                        " 'table-name' = '${tableName}'\n" +
                        ")";
        connectorWithBody = connectorWithBody.replace("${sink_url}", sink_url);
//                .replace("${sink_username}", sink_username)
//                .replace("${sink_password}", sink_password);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 注册同步的库对应的catalog
        MySqlCatalog mysqlCatalog = new MySqlCatalog("mysql-catalog", db, userName, passWord, String.format("jdbc:mysql://%s:%d", host, port));
        List<String> tables = new ArrayList<>();

        // 如果整库同步，则从catalog里取所有表，否则从指定表中取表名
        if (".*".equals(tableList)) {
            tables = mysqlCatalog.listTables(db);
        } else {
            String[] tableArray = tableList.split(",");
            for (String table : tableArray) {
                tables.add(table.split("\\.")[1]);
            }
        }
        // 创建表名和对应RowTypeInfo映射的Map
        Map<String, RowTypeInfo> tableTypeInformationMap = Maps.newConcurrentMap();
        Map<String, DataType[]> tableDataTypesMap = Maps.newConcurrentMap();
        Map<String, RowType> tableRowTypeMap = Maps.newConcurrentMap();
        for (String table : tables) {
            // 获取mysql catalog中注册的表
            ObjectPath objectPath = new ObjectPath(db, table);
            DefaultCatalogTable catalogBaseTable = (DefaultCatalogTable) mysqlCatalog.getTable(objectPath);
            // 获取表的Schema
            Schema schema = catalogBaseTable.getUnresolvedSchema();
            // 获取表中字段名列表
            String[] fieldNames = new String[schema.getColumns().size()];
            // 获取DataType
            DataType[] fieldDataTypes = new DataType[schema.getColumns().size()];
            LogicalType[] logicalTypes = new LogicalType[schema.getColumns().size()];
            // 获取表字段类型
            TypeInformation<?>[] fieldTypes = new TypeInformation[schema.getColumns().size()];
            // 获取表的主键
            List<String> primaryKeys = schema.getPrimaryKey().get().getColumnNames();

            for (int i = 0; i < schema.getColumns().size(); i++) {
                Schema.UnresolvedPhysicalColumn column = (Schema.UnresolvedPhysicalColumn) schema.getColumns().get(i);
                fieldNames[i] = column.getName();
                fieldDataTypes[i] = (DataType) column.getDataType();
                fieldTypes[i] = InternalTypeInfo.of(((DataType) column.getDataType()).getLogicalType());
                logicalTypes[i] = ((DataType) column.getDataType()).getLogicalType();
            }
            RowType rowType = RowType.of(logicalTypes, fieldNames);
            tableRowTypeMap.put(table, rowType);

            // 组装sink表ddl sql
            StringBuilder stmt = new StringBuilder();
            String tableName = table;
            String jdbcSinkTableName = String.format("sink_%s", tableName);
            stmt.append("create table ").append(jdbcSinkTableName).append("(\n");

            for (int i = 0; i < fieldNames.length; i++) {
                System.out.println(fieldNames[i]);
                String column = "`" + fieldNames[i] + "`";
                String fieldDataType = fieldDataTypes[i].toString();
                stmt.append("\t").append(column).append(" ").append(fieldDataType).append(",\n");
            }
            stmt.append(String.format("PRIMARY KEY (%s) NOT ENFORCED\n)", StringUtils.join(primaryKeys, ",")));
            String formatJdbcSinkWithBody = connectorWithBody
                    .replace("${tableName}", jdbcSinkTableName);
            String createSinkTableDdl = stmt.toString() + formatJdbcSinkWithBody;
            System.out.println(stmt);
            // 创建sink表
            log.info("createSinkTableDdl: {}", createSinkTableDdl);

            tEnv.executeSql(createSinkTableDdl);
            tableDataTypesMap.put(tableName, fieldDataTypes);
            tableTypeInformationMap.put(tableName, new RowTypeInfo(fieldTypes, fieldNames));
        }

        // 监控mysql binlog
        MySqlSource mySqlSource = MySqlSource.<Tuple2<String, Row>>builder()
                .hostname(host)
                .port(port)
                .databaseList(db)
                .tableList(tableList)
                .username(userName)
                .password(passWord)
                .deserializer(new CustomDebeziumDeserializer(tableRowTypeMap))
                .startupOptions(StartupOptions.initial())
                .build();
        SingleOutputStreamOperator<Tuple2<String, Row>> dataStreamSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql cdc").disableChaining();
        StatementSet statementSet = tEnv.createStatementSet();
        // dataStream转Table，创建临时视图，插入sink表
        for (Map.Entry<String, RowTypeInfo> entry : tableTypeInformationMap.entrySet()) {
            String tableName = entry.getKey();
            RowTypeInfo rowTypeInfo = entry.getValue();
            SingleOutputStreamOperator<Row> mapStream = dataStreamSource.filter(data -> data.f0.equals(tableName)).map(data -> data.f1, rowTypeInfo);
            Table table = tEnv.fromChangelogStream(mapStream);
            String temporaryViewName = String.format("t_%s", tableName);
            tEnv.createTemporaryView(temporaryViewName, table);
            String sinkTableName = String.format("sink_%s", tableName);
            String insertSql = String.format("insert into %s select * from %s", sinkTableName, temporaryViewName);
            log.info("add insertSql for {},sql: {}", tableName, insertSql);
            statementSet.addInsertSql(insertSql);
        }
        statementSet.execute();
    }
}
