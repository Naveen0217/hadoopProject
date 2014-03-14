#!/bin/bash
time hadoop jar /usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples-2.0.6-alpha.jar sort randomdata sorteddata
hadoop fs -rm -r /user/hadoop/sorteddata
time hadoop jar /usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples-2.0.6-alpha.jar sort randomdata sorteddata
hadoop fs -rm -r /user/hadoop/sorteddata
time hadoop jar /usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples-2.0.6-alpha.jar sort randomdata sorteddata
hadoop fs -rm -r /user/hadoop/sorteddata
