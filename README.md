# PipelineScheduler
Apache Storm Scheduler
This demo scheduler assigns specific tasks of a topology's components to specific slots in a cluster based on our algorithm
@author nicoletnt September 02, 2020 10:11:43 PM

(PipelineScheduler.jar already in /lib)

Step 1 start nimbus-  bin/storm nimbus
Step 2 submit the topology from storm-starter- /usr/local/storm/bin/storm jar target/storm-starter-*.jar org.apache.storm.starter.ExclamationTopology4 special-topology
