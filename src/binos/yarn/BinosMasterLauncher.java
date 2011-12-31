package binos.yarn;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.ResourceMgrDelegate;
import org.apache.hadoop.mapred.TaskLog;
import org.apache.hadoop.mapred.YARNRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class BinosMasterLauncher {
	private static YarnConfiguration conf = new YarnConfiguration();
	private static ResourceMgrDelegate resMgrDelegate = new ResourceMgrDelegate(conf);
	private final static RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
	private static final Log LOG = LogFactory.getLog(BinosMasterLauncher.class);
	private final FileContext defaultFileContext;
	private final Credentials ts;
	private final String jobSubmitDir;
	BinosMasterLauncher(String jobDir) {
		try {
			defaultFileContext = FileContext.getFileContext(this.conf);
		} catch (UnsupportedFileSystemException ufe) {
			throw new RuntimeException(
					"Error in instantiating BinosMasterLauncher", ufe);
		}
		this.ts = new Credentials();
		this.jobSubmitDir = jobDir;
	}

	private LocalResource createApplicationResource(FileContext fs, Path p)
			throws IOException {
		LocalResource rsrc = recordFactory
				.newRecordInstance(LocalResource.class);
		FileStatus rsrcStat = fs.getFileStatus(p);
		rsrc.setResource(ConverterUtils.getYarnUrlFromPath(fs
				.getDefaultFileSystem().resolvePath(rsrcStat.getPath())));
		rsrc.setSize(rsrcStat.getLen());
		rsrc.setTimestamp(rsrcStat.getModificationTime());
		rsrc.setType(LocalResourceType.FILE);
		rsrc.setVisibility(LocalResourceVisibility.APPLICATION);
		return rsrc;
	}
	protected  ApplicationSubmissionContext createApplicationSubmissionContext(
				) throws IOException {
		try {
			resMgrDelegate.getNewJobID();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}    
		ApplicationId applicationId = resMgrDelegate.getApplicationId();
		// Setup resource requirements
		Resource capability = recordFactory.newRecordInstance(Resource.class);
		capability.setMemory(this.conf.getInt(MRJobConfig.MR_AM_VMEM_MB,
				MRJobConfig.DEFAULT_MR_AM_VMEM_MB));
		LOG.info("AppMaster capability = " + capability);

		// Setup LocalResources
		Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

		Path jobConfPath = new Path(jobSubmitDir, MRJobConfig.JOB_CONF_FILE);

		URL yarnUrlForJobSubmitDir = ConverterUtils
				.getYarnUrlFromPath(defaultFileContext.getDefaultFileSystem()
						.resolvePath(
								defaultFileContext.makeQualified(new Path(
										jobSubmitDir))));
		LOG.debug("Creating setup context, jobSubmitDir url is "
				+ yarnUrlForJobSubmitDir);

		localResources.put(MRJobConfig.JOB_CONF_FILE,
				createApplicationResource(defaultFileContext, jobConfPath));
		if (this.conf.get(MRJobConfig.JAR) != null) {
			localResources.put(
					MRJobConfig.JOB_JAR,
					createApplicationResource(defaultFileContext, new Path(
							jobSubmitDir, MRJobConfig.JOB_JAR)));
		} else {
			// Job jar may be null. For e.g, for pipes, the job jar is the
			// hadoop
			// mapreduce jar itself which is already on the classpath.
			LOG.info("Job jar is not present. "
					+ "Not adding any jar to the list of resources.");
		}

		// TODO gross hack
		for (String s : new String[] { MRJobConfig.JOB_SPLIT,
				MRJobConfig.JOB_SPLIT_METAINFO,
				MRJobConfig.APPLICATION_TOKENS_FILE }) {
			localResources.put(
					MRJobConfig.JOB_SUBMIT_DIR + "/" + s,
					createApplicationResource(defaultFileContext, new Path(
							jobSubmitDir, s)));
		}

		// Setup security tokens
		ByteBuffer securityTokens = null;
		if (UserGroupInformation.isSecurityEnabled()) {
			DataOutputBuffer dob = new DataOutputBuffer();
			ts.writeTokenStorageToStream(dob);
			securityTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
		}

		// Setup the command to run the AM
		List<String> vargs = new ArrayList<String>(8);
		vargs.add(Environment.JAVA_HOME.$() + "/bin/java");

		// TODO: why do we use 'conf' some places and 'jobConf' others?
		long logSize = TaskLog.getTaskLogLength(new JobConf(this.conf));
		String logLevel = this.conf.get(MRJobConfig.MR_AM_LOG_LEVEL,
				MRJobConfig.DEFAULT_MR_AM_LOG_LEVEL);
		MRApps.addLog4jSystemProperties(logLevel, logSize, vargs);

		vargs.add(conf.get(MRJobConfig.MR_AM_COMMAND_OPTS,
				MRJobConfig.DEFAULT_MR_AM_COMMAND_OPTS));

		vargs.add(MRJobConfig.APPLICATION_MASTER_CLASS);
		vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR
				+ Path.SEPARATOR + ApplicationConstants.STDOUT);
		vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR
				+ Path.SEPARATOR + ApplicationConstants.STDERR);

		Vector<String> vargsFinal = new Vector<String>(8);
		// Final commmand
		StringBuilder mergedCommand = new StringBuilder();
		for (CharSequence str : vargs) {
			mergedCommand.append(str).append(" ");
		}
		vargsFinal.add(mergedCommand.toString());

		LOG.info("Command to launch container for ApplicationMaster is : "
				+ mergedCommand);
		
		// Setup the CLASSPATH in environment
		// i.e. add { job jar, CWD, Hadoop jars} to classpath.
		Map<String, String> environment = new HashMap<String, String>();
		MRApps.setClasspath(environment);

		// Parse distributed cache
		MRApps.setupDistributedCache(this.conf, localResources);

		Map<ApplicationAccessType, String> acls = new HashMap<ApplicationAccessType, String>(
				2);
		acls.put(ApplicationAccessType.VIEW_APP, this.conf.get(
				MRJobConfig.JOB_ACL_VIEW_JOB,
				MRJobConfig.DEFAULT_JOB_ACL_VIEW_JOB));
		acls.put(ApplicationAccessType.MODIFY_APP, this.conf.get(
				MRJobConfig.JOB_ACL_MODIFY_JOB,
				MRJobConfig.DEFAULT_JOB_ACL_MODIFY_JOB));

		// Setup ContainerLaunchContext for AM container
		ContainerLaunchContext amContainer = BuilderUtils
				.newContainerLaunchContext(null, UserGroupInformation
						.getCurrentUser().getShortUserName(), capability,
						localResources, environment, vargsFinal, null,
						securityTokens, acls);

		// Set up the ApplicationSubmissionContext
		ApplicationSubmissionContext appContext = recordFactory
				.newRecordInstance(ApplicationSubmissionContext.class);
		appContext.setApplicationId(applicationId); // ApplicationId
		appContext.setUser( // User name
				UserGroupInformation.getCurrentUser().getShortUserName());
		appContext.setQueue( // Queue name
				this.conf.get(JobContext.QUEUE_NAME,
						YarnConfiguration.DEFAULT_QUEUE_NAME));
		appContext.setApplicationName( // Job name
				this.conf.get(JobContext.JOB_NAME,
						YarnConfiguration.DEFAULT_APPLICATION_NAME));
		appContext.setAMContainerSpec(amContainer); // AM Container

		return appContext;
	}
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
//		YarnConfiguration conf = new YarnConfiguration();
//		InetSocketAddress rmAddress =
//		NetUtils.createSocketAddr(this.conf.get(
//				YarnConfiguration.RM_ADDRESS,
//				YarnConfiguration.DEFAULT_RM_ADDRESS),
//				YarnConfiguration.DEFAULT_RM_PORT,
//				YarnConfiguration.RM_ADDRESS);
//		Proxy.newProxyInstance(loader, interfaces, h)
//		ClientRMProtocol applicationsManager =
//		
//		(ClientRMProtocol) rpc.getProxy(ClientRMProtocol.class,
//
//		rmAddress, this.conf);
//		RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
//		YarnConfiguration conf = new YarnConfiguration();
//		YARNRunner submitClient = new YARNRunner(conf);
//		Job job = new Job(conf, "Binos-Master");
//		submitClient.submitJob(job.getJobID(), new String("/tmp/Binos-Master") , job.getCredentials());
//		//submitClient.submitJob(job.getJobID(), job.get, arg2)
//		
//		ApplicationSubmissionContext appContext = recordFactory.
//		ResourceMgrDelegate rmd = new ResourceMgrDelegate(conf);
//		rmd.getNewJobID();
		
		//System.out.println(rmd.getApplicationId());
		String jobDir = "/tmp/binos";
		BinosMasterLauncher bml = new BinosMasterLauncher(jobDir);
		ApplicationSubmissionContext appContext =
				bml.createApplicationSubmissionContext();
		resMgrDelegate.submitApplication(appContext);

	}
}
