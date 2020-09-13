package storm.custom.PipelineScheduler;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.storm.scheduler.EvenScheduler;


import org.apache.storm.scheduler.SchedulerAssignment;

import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.IScheduler;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;




/**
 * This demo scheduler assigns specific tasks of a topology's components to specific slots in a cluster 
 * @author nicoletnt September 02, 2020 10:11:43 PM
 */
public class PipelineScheduler implements IScheduler {
    
	public void prepare(Map conf) {}

    public void schedule(Topologies topologies, Cluster cluster) {
    	System.out.println("DemoScheduler: begin scheduling");
        // Gets the topology which we want to schedule
        TopologyDetails topology = topologies.getByName("special-topology");

        // make sure the special topology is submitted,
        if (topology != null) {
            boolean needsScheduling = cluster.needsScheduling(topology);

            if (!needsScheduling) {
            	System.out.println("Our special topology DOES NOT NEED scheduling.");
            } else {
            	System.out.println("Our special topology needs scheduling.");
            	
                // find out all the needs-scheduling components of this topology
                Map<String, List<ExecutorDetails>> componentToExecutors = cluster.getNeedsSchedulingComponentToExecutors(topology);
                
                System.out.println("needs scheduling(component->executor): " + componentToExecutors);
                System.out.println("needs scheduling(executor->compoenents): " + cluster.getNeedsSchedulingExecutorToComponents(topology));
                SchedulerAssignment currentAssignment = cluster.getAssignmentById(topologies.getByName("special-topology").getId());
                if (currentAssignment != null) {
                	System.out.println("current assignments: " + currentAssignment.getExecutorToSlot());
                } else {
                	System.out.println("current assignments: {}");
                }
                
            
                  //Set the sets of executors to be placed in each slot (we have two slots per supervisor)

                   List<ExecutorDetails> ExecutorsOfAndros1= new ArrayList<>();
                   List<ExecutorDetails> ExecutorsOfAndros2= new ArrayList<>();
                   
                   ExecutorsOfAndros1.add(componentToExecutors.get("word").get(0));
                   ExecutorsOfAndros1.add(componentToExecutors.get("exclaim1").get(1));
                   ExecutorsOfAndros1.add(componentToExecutors.get("exclaim2").get(2));
                   ExecutorsOfAndros1.add(componentToExecutors.get("__acker").get(0));
                   
                   ExecutorsOfAndros2.add(componentToExecutors.get("dummy").get(3));
                   ExecutorsOfAndros2.add(componentToExecutors.get("word").get(4));
                   ExecutorsOfAndros2.add(componentToExecutors.get("dummy").get(7));
                   ExecutorsOfAndros2.add(componentToExecutors.get("__acker").get(1));
                   System.out.println("Executors for Andros): " + ExecutorsOfAndros1 + "and" + ExecutorsOfAndros2);
                  
                   
                   List<ExecutorDetails> ExecutorsOfAntiparos1 = new ArrayList<>();;
                   List<ExecutorDetails> ExecutorsOfAntiparos2 = new ArrayList<>();;
                   
                   ExecutorsOfAntiparos1.add(componentToExecutors.get("exclaim1").get(0));
                   ExecutorsOfAntiparos1.add(componentToExecutors.get("word").get(1));
                   ExecutorsOfAntiparos1.add(componentToExecutors.get("dummy").get(2));
                   ExecutorsOfAntiparos1.add(componentToExecutors.get("__acker").get(2)); 
                   
                   
                   ExecutorsOfAntiparos2.add(componentToExecutors.get("exclaim2").get(3));
                   ExecutorsOfAntiparos2.add(componentToExecutors.get("dummy").get(4));
                   ExecutorsOfAntiparos2.add(componentToExecutors.get("word").get(5));
                   ExecutorsOfAntiparos2.add(componentToExecutors.get("__acker").get(3));
                   
                   System.out.println("Executors for Antiparos): " + ExecutorsOfAntiparos1 + "and" + ExecutorsOfAntiparos2);
                  
              
                   List<ExecutorDetails> ExecutorsOfFolegandros1 = new ArrayList<>();
                   List<ExecutorDetails> ExecutorsOfFolegandros2 = new ArrayList<>();
                   
                   ExecutorsOfFolegandros1.add(componentToExecutors.get("exclaim2").get(0));
                   ExecutorsOfFolegandros1.add(componentToExecutors.get("dummy").get(1));
                   ExecutorsOfFolegandros1.add(componentToExecutors.get("word").get(2));
                   ExecutorsOfFolegandros1.add(componentToExecutors.get("__acker").get(4));
                   
                   ExecutorsOfFolegandros2.add(componentToExecutors.get("exclaim1").get(3));
                   ExecutorsOfFolegandros2.add(componentToExecutors.get("dummy").get(5));
                   ExecutorsOfFolegandros2.add(componentToExecutors.get("dummy").get(8));
                   ExecutorsOfFolegandros2.add(componentToExecutors.get("__acker").get(5));
                  
                   System.out.println("Executors for Folegandros): " + ExecutorsOfFolegandros1 + "and" + ExecutorsOfFolegandros2);
                   
                  
                   List<ExecutorDetails> ExecutorsOfKoufonisi1 = new ArrayList<>();
                   List<ExecutorDetails> ExecutorsOfKoufonisi2 = new ArrayList<>();
                   
                   ExecutorsOfKoufonisi1.add(componentToExecutors.get("dummy").get(0));
                   ExecutorsOfKoufonisi1.add(componentToExecutors.get("exclaim2").get(1));
                   ExecutorsOfKoufonisi1.add(componentToExecutors.get("exclaim1").get(2));
                   ExecutorsOfKoufonisi1.add(componentToExecutors.get("__acker").get(6));
                   
                   ExecutorsOfKoufonisi2.add(componentToExecutors.get("word").get(3));
                   ExecutorsOfKoufonisi2.add(componentToExecutors.get("dummy").get(6));
                   ExecutorsOfKoufonisi2.add(componentToExecutors.get("dummy").get(9));
                   ExecutorsOfKoufonisi2.add(componentToExecutors.get("__acker").get(7));
                   System.out.println("Executors for Koufonisi): " + ExecutorsOfKoufonisi1 + "and" + ExecutorsOfKoufonisi2);

                   
             
                   
                   
                    // Get the clusters' Supervisors based on their ID
                    SupervisorDetails SupervisorAndros = cluster.getSupervisorById("cb00953f-7ff8-48ef-8995-22b2617a4ad5");
                    SupervisorDetails SupervisorAntiparos = cluster.getSupervisorById("9cf64f2b-2a98-426d-97fc-47f1b3da5d7a");
                    SupervisorDetails SupervisorFolegandros = cluster.getSupervisorById("4cf6ec05-5836-43a2-8cb4-dde8bba050ce");
                    SupervisorDetails SupervisorKoufonisi = cluster.getSupervisorById("d76d9533-b59d-49f8-9dc7-f16590eb71bb");
                                                                                     
  
                   
                    
                    if (SupervisorAndros != null) {
                    	System.out.println("Found the supervisor andros");
                        List<WorkerSlot> availableSlots2 = cluster.getAvailableSlots(SupervisorAndros);
                        
                        // if there is no available slots on this supervisor, free some.
                        // for simplicity, we free all the used slots on the supervisor.
                        if (availableSlots2.isEmpty() && !ExecutorsOfAndros1.isEmpty() && !ExecutorsOfAndros2.isEmpty()) {
                            for (Integer port : cluster.getUsedPorts(SupervisorAndros)) {
                                cluster.freeSlot(new WorkerSlot(SupervisorAndros.getId(), port));
                            }
                        }
                        
                        // re-get the aviableSlots
                        availableSlots2 = cluster.getAvailableSlots(SupervisorAndros);
                       
                        // We assign the executors' sets to the corresponding slots as identified by our algorithm
                        
                        cluster.assign(availableSlots2.get(0), topology.getId(), ExecutorsOfAndros1);
                        cluster.assign(availableSlots2.get(1), topology.getId(), ExecutorsOfAndros2);
                        System.out.println("We assigned executors:" + ExecutorsOfAndros1 + " to slot: [" + availableSlots2.get(0).getNodeId() + ", " + availableSlots2.get(0).getPort() + "]");
                        System.out.println("We assigned executors:" + ExecutorsOfAndros2 + " to slot: [" + availableSlots2.get(1).getNodeId() + ", " + availableSlots2.get(1).getPort() + "]");
                    	} else {
                    	System.out.println("There is no such supervisor (Andros)!");
                    	}   
                    
                    
                    
                    
                    
                    
                    if (SupervisorAntiparos != null) {
                    	System.out.println("Found the supervisor antiparos");
                        List<WorkerSlot> availableSlots1 = cluster.getAvailableSlots(SupervisorAntiparos);
                        
                        // if there is no available slots on this supervisor, free some.
                        // for simplicity, we free all the used slots on the supervisor.
                        if (availableSlots1.isEmpty() && !ExecutorsOfAntiparos1.isEmpty() && !ExecutorsOfAntiparos2.isEmpty()) {
                            for (Integer port : cluster.getUsedPorts(SupervisorAntiparos)) {
                                cluster.freeSlot(new WorkerSlot(SupervisorAntiparos.getId(), port));
                            }
                        }

                        // re-get the aviableSlots
                        availableSlots1 = cluster.getAvailableSlots(SupervisorAntiparos);

                        // We assign the executors' sets to the corresponding slots as identified by our algorithm
                        cluster.assign(availableSlots1.get(0), topology.getId(), ExecutorsOfAntiparos1);
                        cluster.assign(availableSlots1.get(1), topology.getId(), ExecutorsOfAntiparos2);
                        System.out.println("We assigned executors:" + ExecutorsOfAntiparos1 + " to slot: [" + availableSlots1.get(0).getNodeId() + ", " + availableSlots1.get(0).getPort() + "]");
                        System.out.println("We assigned executors:" + ExecutorsOfAntiparos2 + " to slot: [" + availableSlots1.get(1).getNodeId() + ", " + availableSlots1.get(1).getPort() + "]");
                    	} else {
                    	System.out.println("There is no such supervisor (Antiparos)!");
                    	}
                    
                    
                    
                    if (SupervisorFolegandros != null) {
                    	System.out.println("Found the supervisor folegandros");
                        List<WorkerSlot> availableSlots3 = cluster.getAvailableSlots(SupervisorFolegandros);
                        
                        // if there is no available slots on this supervisor, free some.
                        // for simplicity, we free all the used slots on the supervisor.
                        if (availableSlots3.isEmpty() && !ExecutorsOfFolegandros1.isEmpty() && !ExecutorsOfFolegandros2.isEmpty()) {
                            for (Integer port : cluster.getUsedPorts(SupervisorFolegandros)) {
                                cluster.freeSlot(new WorkerSlot(SupervisorFolegandros.getId(), port));
                            }
                        }

                        // re-get the aviableSlots
                        availableSlots3 = cluster.getAvailableSlots(SupervisorFolegandros);

                        // We assign the executors' sets to the corresponding slots as identified by our algorithm
                        cluster.assign(availableSlots3.get(0), topology.getId(), ExecutorsOfFolegandros1);
                        cluster.assign(availableSlots3.get(1), topology.getId(), ExecutorsOfFolegandros2);
                        System.out.println("We assigned executors:" + ExecutorsOfFolegandros1 + " to slot: [" + availableSlots3.get(0).getNodeId() + ", " + availableSlots3.get(0).getPort() + "]");
                        System.out.println("We assigned executors:" + ExecutorsOfFolegandros2 + " to slot: [" + availableSlots3.get(1).getNodeId() + ", " + availableSlots3.get(1).getPort() + "]");
                    	} else {
                    	System.out.println("There is no such supervisor (Folegandros) !");
                    	}
                    
                    
                   if (SupervisorKoufonisi != null) {
                    	System.out.println("Found the supervisor koufonisi");
                        List<WorkerSlot> availableSlots4 = cluster.getAvailableSlots(SupervisorKoufonisi);
                        
                        // if there is no available slots on this supervisor, free some.
                        // for simplicity, we free all the used slots on the supervisor.
                        if (availableSlots4.isEmpty() && !ExecutorsOfKoufonisi1.isEmpty() && !ExecutorsOfKoufonisi2.isEmpty()) {
                            for (Integer port : cluster.getUsedPorts(SupervisorKoufonisi)) {
                                cluster.freeSlot(new WorkerSlot(SupervisorKoufonisi.getId(), port));
                            }
                        }
                        
                        // re-get the aviableSlots
                        availableSlots4 = cluster.getAvailableSlots(SupervisorKoufonisi);
                       

                        // We assign the executors' sets to the corresponding slots as identified by our algorithm
                        cluster.assign(availableSlots4.get(0), topology.getId(), ExecutorsOfKoufonisi1);
                        cluster.assign(availableSlots4.get(1), topology.getId(), ExecutorsOfKoufonisi2);
                        System.out.println("We assigned executors:" + ExecutorsOfKoufonisi1 + " to slot: [" + availableSlots4.get(0).getNodeId() + ", " + availableSlots4.get(0).getPort() + "]");
                        System.out.println("We assigned executors:" + ExecutorsOfKoufonisi2 + " to slot: [" + availableSlots4.get(1).getNodeId() + ", " + availableSlots4.get(1).getPort() + "]");
                   		
                   		} else {
                    	System.out.println("There is no such supervisor (Koufonisi)!");
                   		}
                    
                 
                
            }
        }
        
        // let system's even scheduler handle the rest (in case it exists) scheduling work
        // you can also use your own other scheduler here, this is what
        // makes storm's scheduler composable.
       new EvenScheduler().schedule(topologies, cluster);
    }

}
