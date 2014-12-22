import MapperTask;
import ReducerTask;
import Task;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

/**
 * The service interface that a task tracker can provide to job tracker.
 * It extends the Remote interface to be called by RMI.
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */

public interface TaskTrackerService extends Remote{
    public void runMapperTask(MapperTask task) throws RemoteException;
    public void runReducerTask(MapperTask mapperTask, List<ReducerTask> reducerTasks) throws RemoteException;
}
