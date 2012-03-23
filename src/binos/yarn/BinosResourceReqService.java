package binos.yarn;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import binos.yarn.MsConnProto.NullResponse;
import binos.yarn.MsConnProto.TaskAmountInfo;
import binos.yarn.MsConnProto.TaskAmountUpdateService;

/**
 * BinosResourceReqService: binos-yarn receive resource request from binos-server
 * @author jiangbing
 *
 */
public class BinosResourceReqService extends TaskAmountUpdateService{
	private static AtomicInteger requestAmount = new AtomicInteger(0);
	private static Map<Integer, Integer> requestArgs = new ConcurrentHashMap<Integer, Integer>();
	
	private static NullResponse resp = NullResponse.newBuilder().build(); 
//	private int averageTasksInOneJob;
//	private int waittingJobsNum;
//	private int waittingTasksNum;
	
	@Override
	public void updateTaskAmount(RpcController controller,
			TaskAmountInfo request, RpcCallback<NullResponse> done) {
		// TODO Auto-generated method stub
		requestArgs.put(requestAmount.incrementAndGet(), 
				request.getAverageTasksNumOneJob() * request.getWationgJobNum() + request.getWatingTaskNum());
		done.run(resp);
	}
	public int getRequestAmount() {
		return requestAmount.get();
	}
	public void reCountRequestAmount() {
		requestAmount.set(0);
	}
}
