package binos.yarn;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.util.Records;

import com.google.common.collect.Lists;

/**
 * YarnNodesStatus: fetching the status of NM status, and provide the info to Application Master.
 * @author jiangbing
 *
 */
public class YarnNodesStatus extends Thread{
	private final static Log LOG = LogFactory.getLog(YarnNodesStatus.class.getName()); 
	private final ClientRMProtocol crmDelegate;
	private Map<NodeId, NodeReport> nodesMap = new ConcurrentHashMap();
	
	YarnNodesStatus(ClientRMProtocol delegate) {
		this.crmDelegate  = delegate;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		GetClusterNodesRequest nodesRequest = Records.newRecord(GetClusterNodesRequest.class);
		List<NodeReport> nodesList = null;
		while (true) {
			try {
				nodesList  = this.crmDelegate.getClusterNodes(nodesRequest).getNodeReports();
				for (NodeReport nr : nodesList) {
					nodesMap.put(nr.getNodeId(), nr);
				}
				Thread.sleep(10000);
			} catch (YarnRemoteException e) {
				// TODO Auto-generated catch block
				LOG.error("Cannot get info from RM, Exception :" + e.getMessage());
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
	
	/**
	 * get the lists of NodeManager. 
	 * @return
	 */
	public List<NodeId> getNMNodesList() {
		return Lists.newArrayList(this.nodesMap.keySet());
	}
}
