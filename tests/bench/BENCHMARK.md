## benchmark

* rustc 1.45.0 (5c1f21c3b 2020-07-13)
* go-1.14
* python-3.8.1
* java openjdk 14.0.1 2020-04-14 

* platform  ubuntu 20.04 LTS, x86-64,  EC2/t2-small(2Gb)

## client libraries
  - java (clickhouse-native-jdbc )                          | 2.1-stable 
  - go (https://github.com/ClickHouse/clickhouse-go)        | 1.4.1
  - python (https://github.com/mymarilyn/clickhouse-driver) | 0.1.4
  - clickhouse_rs                                           | 0.2.0-alpha.5
  - clickhouse-driver(3468745c3d8a2fbf5182a8d844f1d122f90a6d20)| 0.1.0-alpha.2 (*DRIVER*)


java -Xms256m -Xmx1024m -jar ./clickhouse_driver-1.0-SNAPSHOT.jar -- select

## result of SELECT benchmark, SELECT 100000 rows (id UInt64, name String, dt DateTime) 
 milliseconds

| python   | java-native | go       | clickhouse-rs | *DRIVER*  |  
| --------:|------------:| --------:| -------------:|----------:|
|  OMK     | 2900        | 9234     | 3776          | 2833      |
|  Killed  | 2900        | 9119     | 3603          | 2821      |
|          | 2888        | 9032     | 3681          | 2847      |
|          | 2921        | 9658     | 3666          | 2867      |
|          | **2902**    | **9260** | **3681**      |**2842**   |

## result of INSERT benchmark, 1000 blocks x 10000 rows of (id UInt64, name String, dt DateTime) 
 milliseconds  
 
| python   | java-native | go        | clickhouse-rs | *DRIVER*  |  
| --------:|------------:|----------:| -------------:|----------:|
| 98263    | 32677       | 42395     | 32939         | 3808      |
| 95706    | 32489       | 42161     | 33911         | 3584      |
| 95973    | 35106       | 41783     | 33649         | 3915      |
| 94287    | 35745       | 41544     | 33631         | 3750      |
|**96057** | **34004**   | **41970** | **33532**     |**3764**   |
