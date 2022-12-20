# Flink Streaming Connector for [Apache Kudu](https://kudu.apache.org/)
This is a [Apache Flink](https://flink.apache.org/) source connector to provide the continuous, incremental and streaming 
events from Kudu tables based on [Apache Bahir](https://bahir.apache.org/docs/flink/current/flink-streaming-kudu/). 

## Features
The connector will be running in two modes:
1. **Customized query**: the user provides the query logic, e.g. predicates. Then
the connector will fetch the data according to the provided logic. The order of the 
fetched data is promised by the provided query logic.
   
2. **Incremental**: the user specifies the one or several columns which are able to be leveraged as something like a db cursor. 
   The source connector will keep the status of
the cursor as in the checkpoint to make sure it will continuously and monotonically emit the records in the kudu table.
   
## Building
```sh
mvn clean install -DskipTests
```

## How to use
1. Import as a maven dependency
```xml
<dependency>
	<groupId>com.ebay.esp</groupId>
        <artifactId>flink-kudu-streaming-connector</artifactId>
        <version>${version}</version>
</dependency>
```
2. Configuration the kudu table details. Let's say we have a kudu table named **user** with the below schema:
```sql
CREATE TABLE user
(
	created_time BIGINT,
	name STRING,
	age INT32,
	PRIMARY KEY (created_time)
)
```
3. Define a Java POJO class to map the above **user** table:
```java
public class User {
    @ColumnDetail(name = "created_time")
    private Long createdTime;

    @ColumnDetail(name = "name")
    private String name;

    @ColumnDetail(name = "age")
    private Integer age;
}
```
In the above **User** class, **@ColumnDetail** is a defined annotation in the project to map the column name to the Java class field. And the type mapping between kudu and Java is like below:
| Kudu            | Java      |
| :----:          |:----:     |
| INT8            | Byte      |
| INT16           | Short     |
| INT32           | Integer   |
| INT64           | Long      |
| STRING          | String    |
| DOUBLE          | Double    |
| FLOAT           | Float     |
| BOOL            | Boolean   |
| UNIXTIME_MICROS | Timestamp |

4. Define the query predicates, selected columns and kudu table configuration, e.g. table name, master addresses to build the source connector:
```java
public class CustomQueryKuduSourceSinkPipelineBuilder {
    private final static CustomQueryKuduSourceSinkPipelineBuilder INST = new CustomQueryKuduSourceSinkPipelineBuilder();

    private CustomQueryKuduSourceSinkPipelineBuilder() { }

    public static CustomQueryKuduSourceSinkPipelineBuilder getInstance() {
        return INST;
    }

    public void build(TestbedContext testbedContext) throws Exception {
        // Selected columns for the query
        UserTableDataQueryDetail detail = new UserTableDataQueryDetail();
        detail.setProjectedColumns(Arrays.asList("created_time", "name"));

        // Predicates for the query
        UserTableDataQueryFilter filter = UserTableDataQueryFilter.builder()
                .colName("created_time")
                .filterOp(FilterOp.GREATER)
                .filterValueResolver(new MyUserTableDataQueryFilterValueResolver()).build();
        UserTableDataQueryFilter ageFilter = UserTableDataQueryFilter.builder()
                .colName("age")
                .filterOp(FilterOp.EQUAL)
                .filterValueResolver(new ConstantUserTableDataQueryFilterValueResolver(100)).build();
        detail.setUserTableDataQueryFilters(Arrays.asList(filter, ageFilter));

        // Configuration class to hold all the details and passed to the source connector
        KuduStreamingSourceConfiguration<User> configuration =
                KuduStreamingSourceConfiguration.<User>builder()
                        .masterAddresses(testbedContext.getMasterAddresses())
                        .tableName(testbedContext.getTableName())
                        .batchRunningInterval(10000l)
                        .runningMode(KuduStreamingRunningMode.CUSTOM_QUERY)
                        .targetKuduRowClz(User.class)
                        .userTableDataQueryDetailList(Arrays.asList(detail))
                        .build();

        // Build the source connector
        KuduStreamingSourceFunction<User> sourceFunction = new KuduStreamingSourceFunction<>(configuration);
    }
}
```
The above code snippet is an example for the **CUSTOM_QUERY**. If you want to build a source connector in the **INCREMENTAL**
mode, then you may need to specify the columns to be leveraged as the cursor which is defined with **@StreamingKey** we defined.
You can specify multiple columns as a combination of the stream keys with different orders.
```java
public class User {
    @StreamingKey(order = 1)
    @ColumnDetail(name = "created_time")
    private Long createdTime;

    @ColumnDetail(name = "name")
    private String name;

    @ColumnDetail(name = "age")
    private Integer age;
}
```

The streaming key will be leveraged as the cursor to read the data from the kudu table. After each record been read, the cursor will be updated with the stream key of that record so the table is able to be read increasingly. And the cursor will be maintained in the flink check point in case of any failures of the task manager. The user is also able to specify the lower and upper bound of the query before starting the job. Below is the sample code snippet:
```java
UserTableDataQueryDetail detail = new UserTableDataQueryDetail();
detail.setProjectedColumns(Arrays.asList("created_time", "name"));

UserTableDataQueryFilter filter = UserTableDataQueryFilter.builder()
        .colName("created_time")
        .filterOp(FilterOp.GREATER)
        .build();

detail.setUserTableDataQueryFilters(Arrays.asList(filter));
detail.setLowerBoundKey("1671429154104");
detail.setUpperBoundKey("1671429156104");
```

## License
[Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0)
