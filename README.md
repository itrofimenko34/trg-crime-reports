# trg-crime-reports

This project contains the set of tools to interact with UK crime reports.

## Prerequisites
* sbt
* Java 1.8
* Docker 3.x.x

## Installation
Download the project:
```
git clone https://github.com/itrofimenko34/trg-crime-reports.git
```

Go to the project root:
```
cd trg-crime-reports
```

Make the main script executable:
```
chmod +x trg-crime-reports.sh
```
Now you're all set!

## Use case

Start the container: 

```
./trg-crime-reports.sh start ../crime-data-export
```

Export csv data to parquet: 

```
./trg-crime-reports.sh export
```

Expected output: 
```
Submitting spark job...
parquet-exporter Spark job has been started!
Result is persisted in parquet at: data/parquet
Total processing time: 139.138 seconds
Finished!
```

Access an arbitrary subset of data:
```
./trg-crime-reports.sh query "SELECT * FROM data LIMIT 10;"
```
Expected output: 
```
Response code
crimeId crimeType       districtName    lastOutcome     latitude        longitude
00000983358084d4e70b1ca90be352591bf506a6efda820bfd313251786a8c12        Burglary                Investigation complete; no suspect identified   53.897553       -1.992842
00000affd3fe78648152ce87be0c770875a8ad5d0ddaccecccb8bcf220af7733        Other theft     staffordshire   Investigation complete; no suspect identified   52.71153        -2.002281
00000c43a6a21bded41228fc0dcc0645dc0db2c89457055c68fa40a5dcb0aa9e        Other theft     cumbria Unable to prosecute suspect     54.879342       -2.937123
000023db0024661420b4eaa6c708d456369433092ae1f1cc380e3cc0199a20c4        Other theft     kent    Unable to prosecute suspect     51.345056       1.392879
00003596ce4f236efa9c4691f0f4ab685a5017febe90a28c1f45c7c3b37aa69b        Violence and sexual offences    derbyshire      Unable to prosecute suspect     52.923651       -1.49489
00003b0974a2f31ddbd5ac220ff1e6771d4d2bc2f9fc1c536af7884788d19153        Public order    hampshire       Unable to prosecute suspect     50.807654       -1.077917
00003b5148a8f2d89645a6ca9f21cfbc2f2632accab6a856a53c344980c82cbc        Vehicle crime   merseyside      Investigation complete; no suspect identified   53.397993       -2.916722
00003f706f4d8c995bb42dd171e855463c57705418e8b66b8390e313c7dd59c1        Criminal damage and arson       durham  Investigation complete; no suspect identified   54.78764        -1.319953
00004d9424d2b5a3a951280a3e47bcfb35d311188a223695844389c0a46cd88f        Violence and sexual offences    northumbria     Status update unavailable       55.005816       -1.464017
000055cc623df67b1f3539e301abb5e060dc9d81d1f314deaf8b5dd2e2042081        Criminal damage and arson       leicestershire  Investigation complete; no suspect identified   52.635005       -1.117284
Time taken: 0.344 seconds, Fetched 10 row(s)

```

Get the total count of crimes:

```
./trg-crime-reports.sh kpi kpiType=count
```

Expected output:
```
+--------+
|count   |
+--------+
|12561697|
+--------+
Total processing time: 44.277 seconds
```

Get top 10 districts where the crimes are mostly commited:

```
./trg-crime-reports.sh kpi kpiType=count groupKeys=districtName limit=10
```

Expected output:
```
Submitting spark job...
kpi-processor Spark job has been started!
+-----------------+-------+
|districtName     |count  |
+-----------------+-------+
|metropolitan     |2160652|
|west-yorkshire   |622046 |
|west-midlands    |560209 |
|kent             |425233 |
|essex            |405736 |
|lancashire       |397189 |
|northumbria      |388058 |
|hampshire        |383112 |
|thames-valley    |369985 |
|avon-and-somerset|359730 |
+-----------------+-------+
only showing top 10 rows

Total processing time: 39.69 seconds
```

See the top 10 rare crime types:

```
./trg-crime-reports.sh kpi kpiType=count groupKeys=crimeType limit=10 ordering=asc
```

Expected results:

```
Submitting spark job...
kpi-processor Spark job has been started!
+---------------------+------+
|crimeType            |count |
+---------------------+------+
|Possession of weapons|95957 |
|Bicycle theft        |160551|
|Robbery              |165044|
|Theft from the person|184186|
|Other crime          |204447|
|Drugs                |372162|
|Shoplifting          |690508|
|Burglary             |724548|
|Vehicle crime        |823813|
|Public order         |847929|
+---------------------+------+
only showing top 10 rows

Total processing time: 40.933 seconds
```

Stop the container: 

```
./trg-crime-reports.sh stop
```

## Usage

Complete list of commands: 

| Command | Arguments | Command samples | Description |
| --- | --- | --- | --- |
| start | **path** (Required) - base csv files directory<br /><br />**Good example:**<br />../crime-report-export <br /><br />**Invalid values:**<br />../crime-report-export/2018,<br /> ../crime-report-export/2018-09/2018-09/some.csv | ```./trg-crime-reports.sh start ../crime-data-export``` | Starts a spark-master and spark-worker docker containers. You have to to run this task before interacting with data. |
| stop |  (no arguments) | ```./trg-crime-reports.sh stop``` | Stops the docker container. Don't forget to run this command when the work is finished. |
| restart | (no arguments)| ```./trg-crime-reports.sh restart``` | Restarts the docker container. |
| update-jar | (no arguments)| ```./trg-crime-reports.sh update-jar``` | Rebuilds the project artifact. You should have SBT installed on your machine. |
| export |**coresNum**(Optional) - Number of cores to use. SparkSession property. Default: 2<br /><br /> **logLevel**(Optional) - spark application log level. Default: ERROR| ```./trg-crime-reports.sh export``` <br /><br /> With optional arguments:<br />```./trg-crime-reports.sh export coresNum=2 logLevel=INFO``` | Launches export Spark job that reads crimes(street) and outcomes csv files and persist result in parquet format. Output location: ```<base_csv_files_directory>/parquet``` **NOTE:** Temporary parquet is not working in docker container. So the data is persisted in json) |
| kpi | **kpiType**(Required) - the type of KPI to calculate.<br />Supported values:<br />--**count** - return a count of records in the groups specified by *groupKeys*<br />--**percent** - calculates a percentage of groups specified by *groupKeys* from a complete dataset<br /><br /> **groupKeys**(Optional) - a comma-separated list of grouping fields. If not  specified, then the total KPI's will be calculated.<br />**Examples:**<br />districtName<br />districtName,crimeType<br /><br />**limit**(Optional) -the number of records to retrive.<br />Default: 20 <br /><br />**ordering**(Optional)- sorting order (desc/asc).<br />Default: desc<br /><br />**coresNum**(Optional) - Number of cores to use. SparkSession property. Default: 2<br /><br /> **logLevel**(Optional) - spark application log level. Default: ERROR| Calculate total count of crimes:<br /><br /> ```/trg-crime-reports.sh kpi kpiType=count``` <br /><br /> See the percentage of crimes per district:<br /><br />```./trg-crime-reports.sh kpi kpiType=percent groupKeys=districtName```<br /><br />Top 10 crime types: <br /><br />```./trg-crime-reports.sh kpi kpiType=count limit=10```| Starts a KPIProcessor job to calculate different kind of KPI's |
| query | **SQL QUERY** - query that ypu want to perform against the data.<br /><br />**Examples:**<br />"SELECT * FROM data LIMIT 10;"<br /><br />SELECT COUNT(*) FROM data WHERE data.districtName is null or data.districtName = '' LIMIT 10;" | ```./trg-crime-reports.sh query "SELECT * FROM data LIMIT 10;"``` <br /> | Execute SQL query in spark-sql console. Can be used to interact with parquet files. Tablename: ```data```|
