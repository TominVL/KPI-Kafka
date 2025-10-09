@echo off
cd /d D:\kafka-energy-tomin\kafka

REM === Batch/Linger ===
echo Running test: batch=16384, linger=0
bin\windows\kafka-producer-perf-test.bat --topic thermal-batch-test --num-records 50000 --record-size 256 --throughput 800 --producer.config conf\p_16384_0.properties | powershell -Command "$input | Select-String 'records sent.*records/sec' | Select -Last 1 | %%{ $_.Line }"

echo Running test: batch=65536, linger=0
bin\windows\kafka-producer-perf-test.bat --topic thermal-batch-test --num-records 50000 --record-size 256 --throughput 800 --producer.config conf\p_65536_0.properties | powershell -Command "$input | Select-String 'records sent.*records/sec' | Select -Last 1 | %%{ $_.Line }"

echo Running test: batch=262144, linger=50
bin\windows\kafka-producer-perf-test.bat --topic thermal-batch-test --num-records 50000 --record-size 256 --throughput 800 --producer.config conf\p_262144_50.properties | powershell -Command "$input | Select-String 'records sent.*records/sec' | Select -Last 1 | %%{ $_.Line }"

REM === Compression ===
echo Running compression test: none
bin\windows\kafka-producer-perf-test.bat --topic thermal-comp-none --num-records 50000 --record-size 256 --throughput 800 --producer.config conf\p_comp_none.properties | powershell -Command "$input | Select-String 'records sent.*records/sec' | Select -Last 1 | %%{ $_.Line }"

echo Running compression test: lz4
bin\windows\kafka-producer-perf-test.bat --topic thermal-comp-lz4 --num-records 50000 --record-size 256 --throughput 800 --producer.config conf\p_comp_lz4.properties | powershell -Command "$input | Select-String 'records sent.*records/sec' | Select -Last 1 | %%{ $_.Line }"

echo Running compression test: zstd
bin\windows\kafka-producer-perf-test.bat --topic thermal-comp-zstd --num-records 50000 --record-size 256 --throughput 800 --producer.config conf\p_comp_zstd.properties | powershell -Command "$input | Select-String 'records sent.*records/sec' | Select -Last 1 | %%{ $_.Line }"

REM === Partitions @ 2000 rec/s ===
echo Running partitions test: 4
bin\windows\kafka-producer-perf-test.bat --topic thermal-part-4 --num-records 100000 --record-size 256 --throughput 2000 --producer.config conf\p_generic.properties | powershell -Command "$input | Select-String 'records sent.*records/sec' | Select -Last 1 | %%{ $_.Line }"

echo Running partitions test: 8
bin\windows\kafka-producer-perf-test.bat --topic thermal-part-8 --num-records 100000 --record-size 256 --throughput 2000 --producer.config conf\p_generic.properties | powershell -Command "$input | Select-String 'records sent.*records/sec' | Select -Last 1 | %%{ $_.Line }"

echo Running partitions test: 12
bin\windows\kafka-producer-perf-test.bat --topic thermal-part-12 --num-records 100000 --record-size 256 --throughput 2000 --producer.config conf\p_generic.properties | powershell -Command "$input | Select-String 'records sent.*records/sec' | Select -Last 1 | %%{ $_.Line }"

echo Tests finished.
