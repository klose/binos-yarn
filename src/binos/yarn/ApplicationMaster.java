package binos.yarn;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.SynchronousQueue;
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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ContainerToken;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.util.Records;
import com.google.common.collect.*;


public class ApplicationMaster {
  private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);

  private ApplicationAttemptId appAttemptId;
  private Configuration conf;
  private YarnRPC rpc;
  private AMRMProtocol resourceManager;
  private String binosHome;
  private int totalSlaves;
  private int slaveMem;
  private int slaveCpus;
  private int masterMem;
  private int masterPort;
  private int slavePort;
  private int failCount;
  private String logDirectory;
  //private String mainClass;
  private String programArgs;
  //private URI jarUri;
  
  private Process master;
  private String masterUrl;
  private Process application;
  private boolean appExited = false;
  private int appExitCode = -1;
  
  private int requestId = 0; // For giving unique IDs to YARN resource requests

  // Have a cache of UGIs on different nodes to avoid creating too many RPC
  // client connection objects to the same NodeManager
  private Map<String, UserGroupInformation> ugiMap = new HashMap<String, UserGroupInformation>();


  public static void main(String[] args) throws Exception {
    new ApplicationMaster(args).run();
  }

  public ApplicationMaster(String[] args) throws Exception {
    try {
      Options opts = new Options();
      opts.addOption("yarn_timestamp", true, "YARN cluster timestamp");
      opts.addOption("yarn_id", true, "YARN application ID");
      opts.addOption("yarn_fail_count", true, "YARN application fail count");
      opts.addOption("binos_home", true, "directory where Binos is installed");
      opts.addOption("slaves", true, "number of slaves");
      opts.addOption("slave_mem", true, "memory per slave, in MB");
      opts.addOption("slave_cpus", true, "CPUs to use per slave");
      opts.addOption("master_mem", true, "memory for master, in MB");
      opts.addOption("master port", true, "port for master service (default: 6060)");
      opts.addOption("slave_port", true, "port for slave service (default: 6061)");
      
      opts.addOption("class", true, "main class of application");
      opts.addOption("args", true, "command line arguments for application");
      //opts.addOption("jar_uri", true, "full URI of the application JAR to download");
      opts.addOption("log_dir", true, "log directory for our container");
      CommandLine line = new GnuParser().parse(opts, args);

      // Get our application attempt ID from the command line arguments
      ApplicationId appId = Records.newRecord(ApplicationId.class);
      appId.setClusterTimestamp(Long.parseLong(line.getOptionValue("yarn_timestamp")));
      appId.setId(Integer.parseInt(line.getOptionValue("yarn_id")));
      //int failCount = Integer.parseInt(line.getOptionValue("yarn_fail_count"));
	  appAttemptId = Records.newRecord(ApplicationAttemptId.class);
      appAttemptId.setApplicationId(appId);
      appAttemptId.setAttemptId(failCount);
      LOG.info("Application ID: " + appId + ", fail count: " + failCount);
      
      // Get other command line arguments
      binosHome = line.getOptionValue("binos_home", System.getenv("BINOS_HOME"));
      logDirectory = line.getOptionValue("log_dir");
      totalSlaves = Integer.parseInt(line.getOptionValue("slaves"));
      slaveMem = Integer.parseInt(line.getOptionValue("slave_mem"));
      slaveCpus = Integer.parseInt(line.getOptionValue("slave_cpus"));
      masterMem = Integer.parseInt(line.getOptionValue("master_mem"));
      masterPort = Integer.parseInt(line.getOptionValue("master_port", "6060"));
      slavePort = Integer.parseInt(line.getOptionValue("slave_port", "6061"));
      failCount = Integer.parseInt(line.getOptionValue("yarn_fail_count", "1"));     
      programArgs = line.getOptionValue("args", "");
      
      // Set up our configuration and RPC
      conf = new Configuration();
      rpc = YarnRPC.create(conf);
    } catch (ParseException e) {
      System.err.println("Failed to parse command line options: " + e.getMessage());
      System.exit(1);
    }
  }

  private void run() throws IOException {
    LOG.info("Starting application master");
    LOG.info("Working directory is " + new File(".").getAbsolutePath());
    
    resourceManager = connectToRM();
    registerWithRM();
    
    /*LOG.info("Downloading job JAR from " + jarUri);
    FileSystem fs = FileSystem.get(jarUri, conf);
    fs.copyToLocalFile(new Path(jarUri), new Path("job.jar"));*/

    startBinosMaster();
    
    // Start and manage the slaves
    int slavesLaunched = 0;
    int slavesFinished = 0;
    boolean sentRequest = false;
    
    List<ResourceRequest> noRequests = new ArrayList<ResourceRequest>();
    List<ContainerId> noReleases = new ArrayList<ContainerId>();
    
    while (slavesFinished != totalSlaves && !appExited) {
      AMResponse response;
      if (!sentRequest) {
        sentRequest = true;
        ResourceRequest request = createRequest(totalSlaves);
        LOG.info("Making resource request: " + request);
        response = allocate(Lists.newArrayList(request), noReleases);
      } else {
        response = allocate(noRequests, noReleases);
      }
      
      for (Container container: response.getAllocatedContainers()) {
        LOG.info("Launching container on " + container.getNodeId().getHost());
        launchContainer(container);
        slavesLaunched++;
        if (slavesLaunched == totalSlaves) {
          LOG.info("All slaves launched; starting user application");
          try {
            Thread.sleep(3000);
          } catch (InterruptedException e) {}
          //startApplication();
        }
      }
      
      for (ContainerStatus container: response.getCompletedContainersStatuses()) {
        LOG.info("Container finished: " + container.getContainerId());
        slavesFinished++;
      }
      
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {}
    }

    boolean succeeded = false;
    if (appExited) {
      LOG.info("Shutting down because user application exited");
      succeeded = (appExitCode == 0); 
    } else {
      LOG.info("Shutting down because all containers died");
    }
    master.destroy();
    if (application != null) {
      application.destroy();
    }
    unregister(succeeded);
  }
  
  // Start binos-master and read its URL into binosUrl
  private void startBinosMaster() throws IOException {
    String[] command = new String[] {binosHome + "/bin/binos-master.sh",
        "--port=6060", "--log_dir=" + logDirectory};
	System.out.println(Arrays.toString(command));
    master = Runtime.getRuntime().exec(command);
    final SynchronousQueue<String> urlQueue = new SynchronousQueue<String>();

    // Start a thread to redirect the process's stdout to a file
    new Thread("stdout redirector for app-master") {
      public void run() {
        BufferedReader in = new BufferedReader(new InputStreamReader(master.getInputStream()));
        PrintWriter out = null;
        try {
          out = new PrintWriter(new FileWriter(logDirectory + "/app-master.stdout"));
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
        PrintWriter out = null;
        try {
          in  = new BufferedReader(new FileReader(logDirectory + "/binos-master.log"));
          boolean foundUrl = false;
          Pattern pattern = Pattern.compile(".*Master started at (.*)");
          String line = null;
          while ((line = in.readLine()) != null) {
            out.println(line);
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
        masterUrl = urlQueue.take();
      } catch (InterruptedException e) {}
    }
    LOG.info("Binos master started with URL " + masterUrl);
    try {
      Thread.sleep(500); // Give binos-master a bit more time to start up
    } catch (InterruptedException e) {}
  }
  
  // Start the user's application
  /*private void startApplication() throws IOException {
    try {
      String sparkClasspath = getSparkClasspath();
      String jobJar = new File("job.jar").getAbsolutePath();
      String javaArgs = "-Xms" + (masterMem - 128) + "m -Xmx" + (masterMem - 128) + "m";
      javaArgs += " -Djava.library.path=" + binosHome + "/lib/java";
      String substitutedArgs = programArgs.replaceAll("\\[MASTER\\]", masterUrl);
      if (mainClass.equals("")) {
        javaArgs += " -cp " + sparkClasspath + " -jar " + jobJar + " " + substitutedArgs; 
      } else {
        javaArgs += " -cp " + sparkClasspath + ":" + jobJar + " " + mainClass + " " + substitutedArgs;
      }
      String java = "java";
      if (System.getenv("JAVA_HOME") != null) {
        java = System.getenv("JAVA_HOME") + "/bin/java";
      }
      String bashCommand = java + " " + javaArgs +
          " 1>" + logDirectory + "/application.stdout" +
          " 2>" + logDirectory + "/application.stderr";
      LOG.info("Command: " + bashCommand);
      String[] command = new String[] {"bash", "-c", bashCommand};
      String[] env = new String[] {"BINOS_HOME=" + binosHome, "MASTER=" + masterUrl, 
          };
      application = Runtime.getRuntime().exec(command, env);
      
      new Thread("wait for user application") {
        public void run() {
          try {
            appExitCode = application.waitFor();
            appExited = true;
            LOG.info("User application exited with code " + appExitCode);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }.start();
      
      LOG.info("User application started");
    } catch (BinosClasspathException e) {
      // Could not find the Spark classpath, either because SPARK_HOME is wrong or Spark wasn't compiled
      LOG.fatal(e.getMessage());
      unregister(false);
      System.exit(1);
      return;
    }
  }
  
  private static class BinosClasspathException extends Exception {
    public BinosClasspathException(String message) {
      super(message);
    }
  }

  private String getSparkClasspath() throws YarnRemoteException, BinosClasspathException {
    String assemblyJar = null;

    // Find the target directory built by SBT
    File targetDir = new File(sparkHome + "/core/target");
    if (!targetDir.exists()) {
      throw new SparkClasspathException("Path " + targetDir + " does not exist: " + 
          "either you gave an invalid SPARK_HOME or you did not build Spark");
    }

    // Look in target to find the assembly JAR
    for (File f: targetDir.listFiles()) {
      if (f.getName().startsWith("core-assembly") && f.getName().endsWith(".jar")) {
        assemblyJar = f.getAbsolutePath();
      }
    }
    if (assemblyJar == null) {
      throw new SparkClasspathException("Could not find Spark assembly JAR in " + targetDir + ": " +
          "you probably didn't run sbt assembly");
    }
    
    return assemblyJar;
  }*/

  private AMRMProtocol connectToRM() {  
    YarnConfiguration yarnConf = new YarnConfiguration(conf);
    InetSocketAddress rmAddress = NetUtils.createSocketAddr(yarnConf.get(
        YarnConfiguration.RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS));		
    LOG.info("Connecting to ResourceManager at " + rmAddress);
    return ((AMRMProtocol) rpc.getProxy(AMRMProtocol.class, rmAddress, conf));
  }

  private void registerWithRM() throws YarnRemoteException {
    RegisterApplicationMasterRequest req =
      Records.newRecord(RegisterApplicationMasterRequest.class);
    req.setApplicationAttemptId(appAttemptId);
    req.setHost("");
    req.setRpcPort(1);
    req.setTrackingUrl("");
    resourceManager.registerApplicationMaster(req);
    LOG.info("Successfully registered with resource manager");
  }

  private ResourceRequest createRequest(int totalTasks) {
    ResourceRequest request = Records.newRecord(ResourceRequest.class);
    request.setHostName("*");
    request.setNumContainers(totalTasks);
    Priority pri = Records.newRecord(Priority.class);
    pri.setPriority(1);
    request.setPriority(pri);
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(slaveMem);
    request.setCapability(capability);
    return request;
  }
  /**
   * launch binos-slave at allocated container.
   * @param container
   * @throws IOException
   */
  private void launchContainer(Container container) throws IOException {
    ContainerManager mgr = connectToCM(container);
    ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
    ctx.setContainerId(container.getId());
    ctx.setResource(container.getResource());
    ctx.setUser(UserGroupInformation.getCurrentUser().getShortUserName());
    List<String> commands = new ArrayList<String> ();
    commands.add(binosHome + "/bin/binos-slave.sh " +
    		"--url=" + masterUrl + " " +
    		"--log_dir=" + this.logDirectory
    		);
    ctx.setCommands(commands);
    StartContainerRequest req = Records.newRecord(StartContainerRequest.class);
    req.setContainerLaunchContext(ctx);
    mgr.startContainer(req);
  }

  private ContainerManager connectToCM(Container container) throws IOException {
    // Based on similar code in the ContainerLauncher in Hadoop MapReduce
    ContainerToken contToken = container.getContainerToken();
    final String address = container.getNodeId().getHost() + ":" + container.getNodeId().getPort();
    LOG.info("Connecting to ContainerManager at " + address);
    UserGroupInformation user = UserGroupInformation.getCurrentUser();
    if (UserGroupInformation.isSecurityEnabled()) {
      if (!ugiMap.containsKey(address)) {
        Token<ContainerTokenIdentifier> hadoopToken = 
          new Token<ContainerTokenIdentifier>(
            contToken.getIdentifier().array(), contToken.getPassword().array(),
            new Text(contToken.getKind()), new Text(contToken.getService()));
        user = UserGroupInformation.createRemoteUser(address);
        user.addToken(hadoopToken);
        ugiMap.put(address, user);
      } else {
        user = ugiMap.get(address);
      }
    }
    ContainerManager mgr = user.doAs(new PrivilegedAction<ContainerManager>() {
      public ContainerManager run() {
        YarnRPC rpc = YarnRPC.create(conf);
        return (ContainerManager) rpc.getProxy(ContainerManager.class,
            NetUtils.createSocketAddr(address), conf);
      }
    });
    return mgr;
  }

  private AMResponse allocate(List<ResourceRequest> resourceRequest, List<ContainerId> releases)
      throws YarnRemoteException {
    AllocateRequest req = Records.newRecord(AllocateRequest.class);
    req.setResponseId(++requestId);
    req.setApplicationAttemptId(appAttemptId);
    req.addAllAsks(resourceRequest);
    req.addAllReleases(releases);
    AllocateResponse resp = resourceManager.allocate(req);
    return resp.getAMResponse();
  }

  private void unregister(boolean succeeded) throws YarnRemoteException {
    LOG.info("Unregistering application");
    FinishApplicationMasterRequest req =
      Records.newRecord(FinishApplicationMasterRequest.class);
    req.setAppAttemptId(appAttemptId);
    req.setFinishApplicationStatus(succeeded ? FinalApplicationStatus.SUCCEEDED: FinalApplicationStatus.FAILED);
    //req.setFinalState(succeeded ? "SUCCEEDED" : "FAILED");
    resourceManager.finishApplicationMaster(req);
  }
}

