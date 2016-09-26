hadoop fs -rm  output/*
hadoop fs -rmdir  output

OUTPUTDIR=/user/mapr/output/
rm -rd $OUTPUTDIR
#spark-submit --class SparkWordCount --master yarn target/sparkwordcount-1.0.jar /user/mapr/input/alice.txt $OUTPUTDIR
spark-submit --class SparkWordCount --master local[*] target/sparkwordcount-1.0.jar /user/mapr/input/alice.txt $OUTPUTDIR
