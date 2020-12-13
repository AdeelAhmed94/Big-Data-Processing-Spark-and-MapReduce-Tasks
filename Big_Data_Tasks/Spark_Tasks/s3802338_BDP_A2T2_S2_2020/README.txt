How to run Task 2:

1. First drag and drop adeel-0.0.1-SNAPSHOT-jar-with-dependencies.jar file to /user/s3802338 HDFS folder

2. Now copy this jar to aws emr using this command:

hadoop fs -copyToLocal -f /user/s3802338/adeel-0.0.1-SNAPSHOT-jar-with-dependencies.jar ~/
-------------------------------------------------------------------------
 
3. Now run the command below

Command to run Tasks:
--------------------
spark-submit --class streaming.adeel.Tasks --master yarn --deploy-mode client adeel-0.0.1-SNAPSHOT-jar-with-dependencies.jar hdfs:///user/s3802338 hdfs:///user/subtaskA_output hdfs:///user/subtaskB_output hdfs:///user/subtaskC_output
------------------------------------------------------------------------------------------------------------------

4. Now drag and drop the input files 3littlepigs, Melbourne and RMIT files in the folder "/user/s3802338/" 
   under monitoring in sequence order .Please must wait for at least 10 seconds between two files. After that result can be viewed for all three files in folder "hdfs:///user" with time stamp to avoid file over writing.