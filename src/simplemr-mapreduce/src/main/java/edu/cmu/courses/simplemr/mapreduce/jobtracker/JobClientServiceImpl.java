import Utils;
import JobClientService;
import JobConfig;
import Pair;

import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/**
 * Implementation of service class. The class contains a JobTracker
 * instance that can assign tasks to task trackers.
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */

public class JobClientServiceImpl extends UnicastRemoteObject implements JobClientService {

    private JobTracker jobTracker;

    public JobClientServiceImpl(JobTracker jobTracker) throws RemoteException {
        super();
        this.jobTracker = jobTracker;
    }


    @Override
    public Pair<String, Integer> getFileServerInfo() throws RemoteException {
        try {
            String host = Utils.getHost();
            return new Pair<String, Integer>(host, jobTracker.getFileServerPort());
        } catch (UnknownHostException e) {
            throw new RemoteException("can't get host name");
        }
    }

    @Override
    public void submitJob(JobConfig jobConfig) throws RemoteException {
        jobTracker.submitJob(jobConfig);
    }

    @Override
    public String describeJobs() throws RemoteException{
        return jobTracker.describeJobs();
    }
}
