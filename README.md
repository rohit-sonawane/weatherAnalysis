# weatherAnalysis

--------------------------------
Table of Contents:
--------------------------------

yearly_statistics.pig - Yearly stats for all countries<br>
transfer_files.sh     - shell script to transfer files from local machine to HDFS<br>

--------------------------------
Setup and Infrastructure:
--------------------------------
Launched 16 AWS EC2 instances , 1 namenode and 15 datanodes<br>
create volume of 50 GB and with the help of snapshot recreated data of GSOD in volume <br>
Mounted that data to namenodes disk so that we can start using that data <br>
Configured hadoop on in full distributed mode<br>
Installed PIG on name node <br>
Transfered that data using shell script to HDFS<br>
with the help of pig script got the ouput
