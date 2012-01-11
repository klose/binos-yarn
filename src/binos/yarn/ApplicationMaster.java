package binos.yarn;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Arrays;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ContainerToken;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.util.Records;
public class ApplicationMaster {
	private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);
	
	 // Counter for completed containers ( complete denotes successful or failed )
	private AtomicInteger numCompletedContainers = new AtomicInteger();
	
	// Allocated container count so that we know how many containers has the RM
	  // allocated to us
	private AtomicInteger numAllocatedContainers = new AtomicInteger();
	
	// Count of failed containers 
	private AtomicInteger numFailedContainers = new AtomicInteger();
	
	 // Launch threads
	private List<Thread> launchThreads = new ArrayList<Thread>();	
	
	private ApplicationAttemptId appAttemptId;
	private Configuration conf;
	private YarnRPC rpc;
	private AMRMProtocol amrmDelegate;
	private ClientRMProtocol crmDelegate;
	private YarnNodesStatus ynService;
	private String binosHome;
	private int totalSlaves; // System settings and adjust as the Binos-Master's workloads. 
	private int slaveMem;
	private int slaveCpus;
	private int masterMem;
	private int masterPort;
	private int slavePort;
	private int failCount;
	private String logDirectory;
	// private String mainClass;
	private String programArgs;
	// private URI jarUri;

	private Process master;
	private String masterUrl;
	private Process application;
	private boolean appExited = false;
	private int appExitCode = -1;

	private int requestId = 0; // For giving unique IDs to YARN resource
								// requests

	// Have a cache of UGIs on different nodes to avoid creating too many RPC
	// client connection objects to the same NodeManager
	private Map<String, UserGroupInformation> ugiMap = new HashMap<String, UserGroupInformation>();

	private Map<NodeId, Container> allocatedContainer = new HashMap<NodeId, Container>();

	public static void main(String[] args) throws Exception {
		new ApplicationMaster(args).run();
	}

	public ApplicationMaster(String[] args) throws Exception {
		try {
			Options opts = new Options();
			opts.addOption("yarn_timestamp", true, "YARN cluster timestamp");
			opts.addOption("yarn_id", true, "YARN application ID");
			opts.addOption("yarn_fail_count", true,
					"YARN application fail count");
			opts.addOption("binos_home", true,
					"directory where Binos is installed");
			opts.addOption("slaves", true, "number of slaves");
			opts.addOption("slave_mem", true, "memory per slave, in MB");
			opts.addOption("slave_cpus", true, "CPUs to use per slave");
			opts.addOption("master_mem", true, "memory for master, in MB");
			opts.addOption("master_port", true,
					"port for master service (default: 6060)");
			opts.addOption("slave_port", true,
					"port for slave service (default: 6061)");

			opts.addOption("class", true, "main class of application");
			opts.addOption("args", true,
					"command line arguments for application");
			// opts.addOption("jar_uri", true,
			// "full URI of the application JAR to download");
			opts.addOption("log_dir", true, "log directory for our container");
			CommandLine line = new GnuParser().parse(opts, args);

			// Get our application attempt ID from the command line arguments
			ApplicationId appId = Records.newRecord(ApplicationId.class);
			appId.setClusterTimestamp(Long.parseLong(line
					.getOptionValue("yarn_timestamp")));
			appId.setId(Integer.parseInt(line.getOptionValue("yarn_id")));
			// int failCount =
			// Integer.parseInt(line.getOptionValue("yarn_fail_count"));
			failCount = Integer.parseInt(line.getOptionValue("yarn_fail_count",
					"1"));
			appAttemptId = Records.newRecord(ApplicationAttemptId.class);
			appAttemptId.setApplicationId(appId);
			appAttemptId.setAttemptId(1);
			LOG.info("Application ID: " + appId + ", fail count: " + failCount);

			// Get other command line arguments
			binosHome = line.getOptionValue("binos_home",
					System.getenv("BINOS_HOME"));
			logDirectory = line.getOptionValue("log_dir");
			totalSlaves = Integer.parseInt(line.getOptionValue("slaves"));
			slaveMem = Integer.parseInt(line.getOptionValue("slave_mem"));
			slaveCpus = Integer.parseInt(line.getOptionValue("slave_cpus"));
			masterMem = Integer.parseInt(line.getOptionValue("master_mem"));
			masterPort = Integer.parseInt(line.getOptionValue("master_port",
					"6060"));
			slavePort = Integer.parseInt(line.getOptionValue("slave_port",
					"6061"));

			programArgs = line.getOptionValue("args", "");

			// Set up our configuration and RPC
			conf = new YarnConfiguration();
			rpc = YarnRPC.create(conf);
			
			
			//create proxy for ClientRMProtocol
			crmDelegate = connectToCRM();
			
			//create proxy for AMRMProtocol
			amrmDelegate = connectToAMRM();
			
			//launch thread to fetch Yarn/Binos nodes status.
			initService();
			
		} catch (ParseException e) {
			System.out.println("Failed to parse command line options: "
					+ e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	private void initService() {
		ynService = new YarnNodesStatus(this.crmDelegate);
		ynService.start();
	}
	
	private void run() throws IOException {

		LOG.info("Starting application master");
		LOG.info("Working directory is " + new File(".").getAbsolutePath());
		// ynService.start();

		
		// Register self with ResourceManager 
	    RegisterApplicationMasterResponse remResp = registerWithRM();
	    
	    // Dump out information about cluster capability as seen by the resource manager
	    int minMem = remResp.getMinimumResourceCapability().getMemory();
	    int maxMem = remResp.getMaximumResourceCapability().getMemory();
	    LOG.info("Min mem capabililty of resources in this cluster " + minMem);
	    LOG.info("Max mem capabililty of resources in this cluster " + maxMem);

	    // A resource ask has to be at least the minimum of the capability of the cluster, the value has to be 
	    // a multiple of the min value and cannot exceed the max. 
	    // If it is not an exact multiple of min, the RM will allocate to the nearest multiple of min		
	    if (slaveMem < minMem) {
	      LOG.info("Container memory specified below min threshold of cluster. Using min value."
	          + ", specified=" + slaveMem
	          + ", min=" + minMem);
	      slaveMem = minMem; 
	    } 
	    else if (slaveMem > maxMem) {
	      LOG.info("Container memory specified above max threshold of cluster. Using max value."
	          + ", specified=" + slaveMem
	          + ", max=" + maxMem);
	      slaveMem = maxMem;
	    }
		
	    LOG.info("Successfully registered with resource manager");

		startBinosMaster();

		// Record the total number of container RM provide, which contains
		// containers that AppMaster rejects.
		int rmResponseContainerNum = 0;
		AMResponse response;
		List<ResourceRequest> noRequests = new ArrayList<ResourceRequest>();
		List<ContainerId> noReleases = new ArrayList<ContainerId>();

		/** apply for containers , and launch the Binos-Slave in the Container. */
		while (numAllocatedContainers.get() < totalSlaves) {
			
			LOG.info("Making resource request for : "
					+ (totalSlaves - numAllocatedContainers.get())
					+ " containers.");
			response = allocate(createRequest(totalSlaves
					- numAllocatedContainers.get()), noReleases);
			LOG.info("allocated containers:"
					+ response.getAllocatedContainers().size());
			for (Container container : response.getAllocatedContainers()) {
				// make sure that every container is assigned to launch
				// binos-slave in different nodes.
				NodeId node = container.getNodeId();
				LOG.info("Get Container on " + node.getHost());
				rmResponseContainerNum++;
				if (!allocatedContainer.keySet().contains(node)) {
					LOG.info("Launching shell command on a new container."
							+ ", containerId=" + container.getId()
							+ ", containerNode="
							+ container.getNodeId().getHost() + ":"
							+ container.getNodeId().getPort()
							+ ", containerNodeURI="
							+ container.getNodeHttpAddress()
							+ ", containerState" + container.getState()
							+ ", containerResourceMemory"
							+ container.getResource().getMemory());
					launchContainer(container);
					numAllocatedContainers.addAndGet(1);
					allocatedContainer.put(node, container);
				} else {
					LOG.info("container in Node "
							+ node.getHost()
							+ " has allocated for one Slave, waiting for another container.");
				}
			}
			
			//monitor the status of allocated container
			monitorAllocatedContainerStatus(response);

			if (numAllocatedContainers.get() == totalSlaves) {
				LOG.info("All Slaves launched." + ", slave number: "
						+ totalSlaves + " , responsed container:"
						+ rmResponseContainerNum + ", accepted container:"
						+ numCompletedContainers.get());
			}

			// delay time to apply for container again.
			// Avoid RM too busy.
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}

		// monitor the container finish state.
		while (totalSlaves != numCompletedContainers.get()) {
			response = allocate(noRequests, noReleases);
			monitorAllocatedContainerStatus(response);
			//TODO  get Binos-Master's workload and adjust the number of slaves.
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}		
		
		// TODO loop without exit		
		unregister(true);
	}
	
	/**
	 * monitor the status of allocated container
	 */
	private void monitorAllocatedContainerStatus(AMResponse response) {
		// Check what the current available resources in the cluster are
		// TODO should we do anything if the available resources are not enough?
		Resource availableResources = response.getAvailableResources();
		LOG.info("Current available resources in the cluster "
				+ availableResources);
		// Check the completed containers
		List<ContainerStatus> completedContainers = response
				.getCompletedContainersStatuses();
		LOG.info("Got response from RM for container ask, completedCnt="
				+ completedContainers.size());
		for (ContainerStatus containerStatus : completedContainers) {
			LOG.info("Got container status for containerID= "
					+ containerStatus.getContainerId() + ", state="
					+ containerStatus.getState() + ", exitStatus="
					+ containerStatus.getExitStatus() + ", diagnostics="
					+ containerStatus.getDiagnostics());

			// non complete containers should not be here
			assert (containerStatus.getState() == ContainerState.COMPLETE);

			// increment counters for completed/failed containers
			int exitStatus = containerStatus.getExitStatus();
			if (0 != exitStatus) {
				// container failed
				if (-100 != exitStatus) {
					// shell script failed
					// counts as completed
					numCompletedContainers.incrementAndGet();
					numFailedContainers.incrementAndGet();
				} else {
					// something else bad happened
					// app job did not complete for some reason
					// we should re-try as the container was lost for some
					// reason
					numAllocatedContainers.decrementAndGet();
					// we do not need to release the container as it would
					// be done
					// by the RM/CM.
				}
			} else {
				// nothing to do
				// container completed successfully
				numCompletedContainers.incrementAndGet();
				LOG.info("Container completed successfully." + ", containerId="
						+ containerStatus.getContainerId());
			}
		}

	}

	/**
	 * Start binos-master and read its URL.
	 * 
	 * @throws IOException
	 */
	private void startBinosMaster() throws IOException {
		String[] command = new String[] { binosHome + "/bin/binos-master.sh",
				"--port=6060", "--log_dir=" + logDirectory };
		System.out.println(Arrays.toString(command));
		master = Runtime.getRuntime().exec(command);
		final SynchronousQueue<String> urlQueue = new SynchronousQueue<String>();

		// Start a thread to redirect the process's stdout to a file
		new Thread("stdout redirector for app-master") {
			public void run() {
				BufferedReader in = new BufferedReader(new InputStreamReader(
						master.getInputStream()));
				PrintWriter out = null;
				try {
					out = new PrintWriter(new FileWriter(logDirectory
							+ "/app-master.stdout"));
					String line = null;
					while ((line = in.readLine()) != null) {
						out.println(line);
					}
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					if (out != null)
						out.close();
				}
			}
		}.start();

		// Start a thread to get Binos-Master URL
		new Thread("Get Binos-Master URL") {
			public void run() {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}

				BufferedReader in = null;
				try {
					in = new BufferedReader(new FileReader(logDirectory
							+ "/binos-master.log"));
					boolean foundUrl = false;
					Pattern pattern = Pattern
							.compile(".*Master started at (.*)");
					String line = null;
					while ((line = in.readLine()) != null) {
						if (!foundUrl) {
							Matcher m = pattern.matcher(line);
							if (m.matches()) {
								String url = m.group(1);
								urlQueue.put(url);
								foundUrl = true;
							}
						}
					}
					in.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}.start();

		// Wait until we've read the URL
		while (masterUrl == null) {
			try {
				System.out.println("Waiting for fetching the Master URL from the output.");
				masterUrl = urlQueue.take();
			} catch (InterruptedException e) {
				
			}
		}
		LOG.info("Binos master started with URL " + masterUrl);
		Thread masterStatusUpdater = new Thread(new MasterMonitorRunnable());
		masterStatusUpdater.start();
	}
	
	/**
	 * Thread to connect to Binos-Master.
	 */
	private class MasterMonitorRunnable implements Runnable {

		@Override
		public void run() {
			while (true) {
				try {
					Thread.sleep(5000); 
					//TODO	connect to Master, and get the workload and other args.			
				} catch (InterruptedException e) {
					master.destroy();
				}

			}

		}
	}
	
	private ClientRMProtocol connectToCRM() {
		YarnConfiguration yarnConf = new YarnConfiguration(conf);
		InetSocketAddress rmAddress = NetUtils.createSocketAddr(this.conf.get(
				YarnConfiguration.RM_ADDRESS,
				YarnConfiguration.DEFAULT_RM_ADDRESS),
				YarnConfiguration.DEFAULT_RM_PORT,
				YarnConfiguration.RM_ADDRESS);
		LOG.info("Connecting to ResourceManager at " + rmAddress +
				", require delegate privilege for ClientRMProtocol.");
		return ((ClientRMProtocol) rpc.getProxy(ClientRMProtocol.class, rmAddress, yarnConf));
	}

	private AMRMProtocol connectToAMRM() {
		YarnConfiguration yarnConf = new YarnConfiguration(conf);
		InetSocketAddress rmAddress = NetUtils.createSocketAddr(yarnConf.get(
				YarnConfiguration.RM_SCHEDULER_ADDRESS,
				YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS));
		LOG.info("Connecting to ResourceManager at " + rmAddress
				+ ", require delegate privilege from AMRMProtocol.");
		return ((AMRMProtocol) rpc
				.getProxy(AMRMProtocol.class, rmAddress, yarnConf));
	}

	private RegisterApplicationMasterResponse registerWithRM() throws YarnRemoteException {
		RegisterApplicationMasterRequest req = Records
				.newRecord(RegisterApplicationMasterRequest.class);
		req.setApplicationAttemptId(appAttemptId);
		req.setHost("");
		req.setRpcPort(1);
		req.setTrackingUrl("");
		return amrmDelegate.registerApplicationMaster(req);
	}

	private List<ResourceRequest> createRequest(int total) {
		List<NodeId> availableNodes = preferredNodes();
		List<ResourceRequest> resList = new ArrayList<ResourceRequest>();
		int reqCount = 0;
		for (NodeId ni: availableNodes) {
			/*debug*/
			LOG.debug("createRequest NodeId" + ni.toString());
			
			ResourceRequest request = Records.newRecord(ResourceRequest.class);
			request.setHostName("*");
			request.setNumContainers(1);
			Priority pri = Records.newRecord(Priority.class);
			pri.setPriority(1);
			request.setPriority(pri);
			Resource capability = Records.newRecord(Resource.class);
			capability.setMemory(slaveMem);
			request.setCapability(capability);
			resList.add(request);
			if ( (++reqCount) == total ) {
				break;
			}
			
		}
		return resList;
	}
	
	/**
	 * calculate the nodes that the system preferred to choose.
	 * @return
	 */
	private List<NodeId> preferredNodes() {
		/*
		 * In Binos-Server, container used by binos-slave, should be launched in different NM
		 * So it calculate that which nodes have not been assigned a binos-slave container.
		 */
		List<NodeId> nmNodesList = this.ynService.getNMNodesList();
		List<NodeId> preferredNodesList = new ArrayList<NodeId>();
		for (NodeId ni : nmNodesList) {
			LOG.info("PreferredNodes: " + ni.getHost());
			if (!allocatedContainer.containsKey(ni)) {
				preferredNodesList.add(ni);
			}
		}
		return preferredNodesList;
	}

	/**
	 * launch binos-slave at allocated container.
	 * 
	 * @param container
	 * @throws IOException
	 */
	private void launchContainer(Container container) throws IOException {
		 LaunchContainerRunnable runnableLaunchContainer = new LaunchContainerRunnable(container);
	     Thread launchThread = new Thread(runnableLaunchContainer);	

	     // launch and start the container on a separate thread to keep the main thread unblocked
	     // as all containers may not be allocated at one go.
	     launchThreads.add(launchThread);
	     launchThread.start();
		
	}

	private AMResponse allocate(List<ResourceRequest> resourceRequest,
			List<ContainerId> releases) throws YarnRemoteException {
		AllocateRequest req = Records.newRecord(AllocateRequest.class);
		req.setResponseId(++requestId);
		req.setApplicationAttemptId(appAttemptId);
		req.addAllAsks(resourceRequest);
		req.addAllReleases(releases);		
		AllocateResponse resp = amrmDelegate.allocate(req);
		return resp.getAMResponse();
	}

	private void unregister(boolean succeeded) throws YarnRemoteException {
		LOG.info("Unregistering application");
		FinishApplicationMasterRequest req = Records
				.newRecord(FinishApplicationMasterRequest.class);
		req.setAppAttemptId(appAttemptId);
		req.setFinishApplicationStatus(succeeded ? FinalApplicationStatus.SUCCEEDED
				: FinalApplicationStatus.FAILED);
		// req.setFinalState(succeeded ? "SUCCEEDED" : "FAILED");
		amrmDelegate.finishApplicationMaster(req);
	}
	
	  /**
	   * Thread to connect to the  ContainerManager and 
	   * launch the container that will execute the shell command. 
	   */
	 private class LaunchContainerRunnable implements Runnable {

	    // Allocated container 
	    Container container;
	    // Handle to communicate with ContainerManager
	    ContainerManager cm;

	    /**
	     * @param lcontainer Allocated container
	     */
	    public LaunchContainerRunnable(Container lcontainer) {
	      this.container = lcontainer;
	    }
	    
	    /*
	     * connect to ContainerManager
	     */
	    private ContainerManager connectToCM(Container container)
				throws IOException {
			// Based on similar code in the ContainerLauncher in Hadoop MapReduce
			ContainerToken contToken = container.getContainerToken();
			final String address = container.getNodeId().getHost() + ":"
					+ container.getNodeId().getPort();
			LOG.info("Connecting to ContainerManager at " + address);
			UserGroupInformation user = UserGroupInformation.getCurrentUser();
			if (UserGroupInformation.isSecurityEnabled()) {
				if (!ugiMap.containsKey(address)) {
					Token<ContainerTokenIdentifier> hadoopToken = new Token<ContainerTokenIdentifier>(
							contToken.getIdentifier().array(), contToken
									.getPassword().array(), new Text(
									contToken.getKind()), new Text(
									contToken.getService()));
					user = UserGroupInformation.createRemoteUser(address);
					user.addToken(hadoopToken);
					ugiMap.put(address, user);
				} else {
					user = ugiMap.get(address);
				}
			}
			ContainerManager mgr = user
					.doAs(new PrivilegedAction<ContainerManager>() {
						public ContainerManager run() {
							YarnRPC rpc = YarnRPC.create(conf);
							return (ContainerManager) rpc.getProxy(
									ContainerManager.class,
									NetUtils.createSocketAddr(address), conf);
						}
					});
			return mgr;
		}

	    @Override
	    /**
	     * Connects to CM, sets up container launch context 
	     * for shell command and eventually dispatches the container 
	     * start request to the CM. 
	     */
		public void run() {
			// Connect to ContainerManager
			LOG.info("Connecting to container manager for containerid="
					+ container.getId());
			try {
				this.cm = connectToCM(this.container);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

			LOG.info("Setting up container launch container for containerid="
					+ container.getId());
			ContainerLaunchContext ctx = Records
					.newRecord(ContainerLaunchContext.class);

			ctx.setContainerId(container.getId());
			ctx.setResource(container.getResource());

			try {
				ctx.setUser(UserGroupInformation.getCurrentUser()
						.getShortUserName());
			} catch (IOException e) {
				LOG.info("Getting current user info failed when trying to launch the container"
						+ e.getMessage());
			}

			// Set the environment
			ctx.setEnvironment(System.getenv());

			List<String> commands = new ArrayList<String>();
			commands.add(binosHome + "/bin/binos-slave.sh " + "--port="
					+ (slavePort++) + " " + "--url=" + masterUrl + " "
					+ "--log_dir=" + logDirectory);
			ctx.setCommands(commands);
			StartContainerRequest startReq = Records
					.newRecord(StartContainerRequest.class);
			startReq.setContainerLaunchContext(ctx);
			try {
				cm.startContainer(startReq);
			} catch (YarnRemoteException e) {
				LOG.info("Start container failed for :" + ", containerId="
						+ container.getId());
				e.printStackTrace();
				// TODO do we need to release this container?
			}

			// Get container status?
			// Left commented out as the shell scripts are short lived
			// and we are relying on the status for completed containers from RM
			// to detect status

			// GetContainerStatusRequest statusReq =
			// Records.newRecord(GetContainerStatusRequest.class);
			// statusReq.setContainerId(container.getId());
			// GetContainerStatusResponse statusResp;
			// try {
			// statusResp = cm.getContainerStatus(statusReq);
			// LOG.info("Container Status"
			// + ", id=" + container.getId()
			// + ", status=" +statusResp.getStatus());
			// } catch (YarnRemoteException e) {
			// e.printStackTrace();
			// }
		}
	}
	
}
