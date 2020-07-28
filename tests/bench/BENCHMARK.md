## benchmark

* rustc 1.45.0 (5c1f21c3b 2020-07-13)
* go-1.14
* python-3.8.1

* platform  ubuntu 18.04 LTS , x86-64,  EC2 t2-small(2Gb)

* rust packages
  - clickhouse_rs     | 0.2.0-alpha.5
  - clickhouse-driver | 0.1.0-alpha.1 (*DRIVER*)

## result of SELECT benchmark
 milliseconds

| python   | go       | clickhouse-rs | *DRIVER*  |  
| -------- |:--------:| -------------:|----------:|
|  OMK     | 9234     | 3776          | 2908      |
|  Killed  | 9119     | 3603          | 3033      |
|          | 9032     | 3681          | 2904      |
|          | 9658     | 3666          | 3143      |
|          | **9260** | **3681**      |**2997**   |

## result of INSERT benchmark, 1000 blocks x 10000 rows 
 milliseconds  
 
| python   | go       | clickhouse-rs | *DRIVER*  |  
| -------- |:--------:| -------------:|----------:|
| 98263    | 42395     | 32939        | 3808      |
| 95706    | 42161     | 33911        | 3584      |
| 95973    | 41783     | 33649        | 3915      |
| 94287    | 41544     | 33631        | 3750      |
|**96057** | **41970** | **33532**    |**3764**   |
