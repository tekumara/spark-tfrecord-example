# spark-tfrecord-example

[Example spark application](src/main/scala/example/SparkTfRecord.scala) writing out gzipped TFRecord, see [spark-tensorflow-connector](https://github.com/tensorflow/ecosystem/tree/master/spark/spark-tensorflow-connector#scala-api).

Ungzipped:

```
$ ls -la test-output.tfrecord/part* | awk '{print $5, $9$10$11}' | column -t 
0    test-output.tfrecord/part-r-00000
0    test-output.tfrecord/part-r-00001
0    test-output.tfrecord/part-r-00002
0    test-output.tfrecord/part-r-00003
0    test-output.tfrecord/part-r-00004
164  test-output.tfrecord/part-r-00005
0    test-output.tfrecord/part-r-00006
0    test-output.tfrecord/part-r-00007
0    test-output.tfrecord/part-r-00008
0    test-output.tfrecord/part-r-00009
0    test-output.tfrecord/part-r-00010
164  test-output.tfrecord/part-r-00011
```

Gzipped

```
$ ls -la test-output.tfrecord/part* | awk '{print $5, $9$10$11}' | column -t 
20   test-output.tfrecord/part-r-00000.gz
20   test-output.tfrecord/part-r-00001.gz
20   test-output.tfrecord/part-r-00002.gz
20   test-output.tfrecord/part-r-00003.gz
20   test-output.tfrecord/part-r-00004.gz
148  test-output.tfrecord/part-r-00005.gz
20   test-output.tfrecord/part-r-00006.gz
20   test-output.tfrecord/part-r-00007.gz
20   test-output.tfrecord/part-r-00008.gz
20   test-output.tfrecord/part-r-00009.gz
20   test-output.tfrecord/part-r-00010.gz
146  test-output.tfrecord/part-r-00011.gz
```