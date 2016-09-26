#hadoop fs -rm  output/*
#hadoop fs -rmdir  output

#OUTPUTDIR=/home/mapr/DEV360/lab/output/
#rm -rd $OUTPUTDIR
#spark-submit --class lq.Dev360Lab2p1 --master local[*] target/Dev360Lab-1.0.jar #/user/mapr/input/alice.txt $OUTPUTDIR
#spark-submit --class lq.Dev360Lab2p2 --master local[*] target/Dev360Lab-1.0.jar #/user/mapr/input/alice.txt $OUTPUTDIR
#spark-submit --class lq.Dev361Lab4p2 --master local[*] target/Dev360Lab-1.0.jar #/user/mapr/input/alice.txt $OUTPUTDIR
spark-submit --class lq.Dev361Lab5p1 --master local[*] target/Dev360Lab-1.0.jar #/user/mapr/input/alice.txt $OUTPUTDIR
