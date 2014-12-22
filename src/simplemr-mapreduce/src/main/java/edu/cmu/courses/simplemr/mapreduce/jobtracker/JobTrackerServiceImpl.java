import Utils;
import JobClientService;
import JobConfig;
import Pair;
import MapperTask;
import ReducerTask;
import edu.cmu.courses.simplemr.mapreduce.tasktracker.TaskTrackerInfo;

import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/**
 * Implementation of service class. The class contains a JobTracker
 * instance that can do the jobs.
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */

public class JobTrackerServiceImpl extends UnicastRemoteObject implements JobTrackerService {

    private JobTracker jobTracker;

    public JobTrackerServiceImpl(JobTracker jobTracker) throws RemoteException {
        super();
        this.jobTracker = jobTracker;
    }

    @Override
    public void heartbeat(TaskTrackerInfo taskTrackerInfo) throws RemoteException {
        jobTracker.updateTaskTracker(taskTrackerInfo);
    }

    @Override
    public void mapperTaskSucceed(MapperTask task) throws RemoteException {
        jobTracker.mapperTaskSucceed(task);
    }

    @Override
    public void reducerTaskSucceed(ReducerTask task) throws RemoteException {
        jobTracker.reducerTaskSucceed(task);
    }

    @Override
    public void mapperTaskFailed(MapperTask task) throws RemoteException {
        jobTracker.mapperTaskFailed(task);
    }

    @Override
    public void reducerTaskFailed(ReducerTask task) throws RemoteException {
        jobTracker.reducerTaskFailed(task);
    }

    @Override
    public void reducerTaskFailedOnMapperTask(ReducerTask reducerTask, MapperTask mapperTask) throws RemoteException {
        jobTracker.reducerTaskFailedOnMapper(reducerTask, mapperTask);
    }
}
