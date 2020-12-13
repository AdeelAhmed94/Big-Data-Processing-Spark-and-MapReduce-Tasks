How to run Task 1:

1. First drag and drop assignment-0.0.1-SNAPSHOT.jar file to /user/s3802338 HDFS folder

2. Now copy this jar to aws emr using this command:

hadoop fs -copyToLocal -f /user/s3802338/assignment-0.0.1-SNAPSHOT.jar ~/
-------------------------------------------------------------------------

3. Now drag and drop all input files(RMIT, Melbourne and 3littlepigs) to /user/s3802338/inputdata folder 

4. Now run the command below respectiverly for each task

Task 1.1 command (Pair Approach):
---------------------------------
hadoop jar assignment-0.0.1-SNAPSHOT.jar group.adeel.assignment.T1P1 /user/s3802338/inputdata /user/s3802338/Task1/output1.1a
------------------------------------------------------------------------------------------------------------------

Task 1.1 command (Strip Approach):
---------------------------------
hadoop jar assignment-0.0.1-SNAPSHOT.jar group.adeel.assignment.T1P2 /user/s3802338/inputdata /user/s3802338/Task1/output1.1b
------------------------------------------------------------------------------------------------------------------

Task 1.2 command (Pair Approach):
---------------------------------
hadoop jar assignment-0.0.1-SNAPSHOT.jar group.adeel.assignment.T1P3 /user/s3802338/inputdata /user/s3802338/Task1/output1.2a
------------------------------------------------------------------------------------------------------------------

Task 1.2 command (Strip Approach):
---------------------------------
hadoop jar assignment-0.0.1-SNAPSHOT.jar group.adeel.assignment.T1P4 /user/s3802338/inputdata /user/s3802338/Task1/output1.2b
------------------------------------------------------------------------------------------------------------------


(g) Performance Analysis on different Nodes for task 1.1 (Pair Approach):
Was able to run only task beacuse of mapper taking hour and my vm was randomly killed by aws during that multiple time so I ran the code
on a smaller file of around few MBs downlaod from amazon s3 aganist the first pair approach

3 Nodes:
-------

Map Task: 16080 CPU_MILLISECONDS
Reduce Task 1: 630 CPU_MILLISECONDS
Reduce Task 2: 760 CPU_MILLISECONDS
Reduce Task 3: 760 CPU_MILLISECONDS

5 Nodes:
-------

Map Task: 17360 CPU_MILLISECONDS
Reduce Task 1: 760 CPU_MILLISECONDS
Reduce Task 2: 720 CPU_MILLISECONDS
Reduce Task 3: 860 CPU_MILLISECONDS

7 Nodes:
-------

Map Task: 18450 CPU_MILLISECONDS
Reduce Task 1: 670 CPU_MILLISECONDS
Reduce Task 2: 790 CPU_MILLISECONDS
Reduce Task 3: 700 CPU_MILLISECONDS

-----------------------------------

As we can see from above , increasing our number of nodes in EMR cluster. The total CPU time used
for a map task is increasing but the time taken by redcuer task is fluctuating for 3, 5 and 7 nodes.