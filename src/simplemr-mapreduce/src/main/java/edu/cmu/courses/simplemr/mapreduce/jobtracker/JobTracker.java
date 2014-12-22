import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import Constants;
import Utils;
import JobClientService;
import JobConfig;
import MapReduceConstants;
import FileServer;
import DFSFileSplitter;
import FileBlock;
import MapperTask;
import ReducerTask;
import Task;
import TaskStatus;
import edu.cmu.courses.simplemr.mapreduce.tasktracker.TaskTrackerInfo;
import edu.cmu.courses.simplemr.mapreduce.tasktracker.TaskTrackerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;
import java.util.concurrent.*;

/**
 * The Job Tracker class for assigning and coordinating map and reduce
 * task trackers. Task trackers will inform the job tracker no matter
 * its succeed or failed. Then the job tracker take following steps
 * to get the whole jobs done.
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */

public class JobTracker {
    private Logger LOG = LoggerFactory.getLogger(JobTracker.class);

    @Parameter(names = {"-dh", "--dfs-master-registry-host"}, description = "the registry host of DFS master")
    private String dfsMasterRegistryHost = Constants.DEFAULT_REGISTRY_HOST;

    @Parameter(names = {"-dp", "--dfs-master-registry-port"}, description = "the registry port of DFS master")
    private int dfsMasterRegistryPort = Constants.DEFAULT_REGISTRY_PORT;

    @Parameter(names = {"-rp", "--registry-port"}, description = "the registry port")
    private int registryPort = Constants.DEFAULT_REGISTRY_PORT;

    @Parameter(names = {"-fp", "--file-server-port"}, description = "the port of file server")
    private int fileServerPort = MapReduceConstants.DEFAULT_FILE_SERVER_PORT;

    @Parameter(names = {"-n", "--num-threads"}, description = "the number of threads")
    private int threadPoolSize = Constants.DEFAULT_THREAD_POOL_SIZE;

    @Parameter(names = {"-c", "--check-period"}, description = "the period of checking task tracker failure (ms)")
    private long checkPeriod = Constants.HEARTBEAT_CHECK;

    @Parameter(names = {"-t", "--temp-dir"}, description = "the directory of temporary files")
    private String tempDir = "/tmp/simplemr-mapreduce-jobtracker";

    @Parameter(names = {"-h", "--help"}, help = true)
    private boolean help;

    private ConcurrentHashMap<String, TaskTrackerInfo> taskTackers;
    private ConcurrentHashMap<Integer, JobInfo> jobs;
    private PriorityBlockingQueue<MapperTask> mapperTasksQueue;
    private ScheduledExecutorService periodicalChecker;
    private ExecutorService threadPool;
    private JobTrackerService service;
    private Thread scheduler;
    private Registry registry;

    public JobTracker() {
        taskTackers = new ConcurrentHashMap<String, TaskTrackerInfo>();
        jobs = new ConcurrentHashMap<Integer, JobInfo>();
        mapperTasksQueue = new PriorityBlockingQueue<MapperTask>();
        periodicalChecker = Executors.newScheduledThreadPool(Constants.DEFAULT_SCHEDULED_THREAD_POOL_SIZE);
        threadPool = Executors.newFixedThreadPool(threadPoolSize);
    }

    public void start()
            throws Exception {
        bindService();
        new FileServer(fileServerPort, tempDir).start();
        periodicalChecker.scheduleAtFixedRate(new JobTrackerChecker(this), 0, checkPeriod, TimeUnit.MILLISECONDS);
        startScheduler();
    }

    public void submitJob(JobConfig jobConfig) {
        jobConfig.validate();
        JobInfo job = new JobInfo(jobConfig);
        jobs.put(job.getId(), job);
        threadPool.execute(new JobTrackerWorker(this, job.getId()));
    }

    public void startJob(int jobId)
            throws Exception{
        JobInfo job = jobs.get(jobId);
        if(job != null){
            generateMapperTasks(job);
            generateReducerTasks(job);
            dispatchMapperTasks(job);
        }
    }

    public void startJobFailed(int jobId){
        JobInfo job = jobs.get(jobId);
        if(job != null){
            job.setJobStatus(JobStatus.FAILED);
        }
    }

    public void dispatchMapperTask(MapperTask task){
        if(task.getStatus() == TaskStatus.PENDING){
            try {
                TaskTrackerInfo taskTracker = taskTackers.get(task.getTaskTrackerName());
                Registry registry = LocateRegistry.getRegistry(taskTracker.getHost(), taskTracker.getRegistryPort());
                TaskTrackerService taskTrackerService = (TaskTrackerService)registry.lookup(task.getTaskTrackerName());
                taskTrackerService.runMapperTask(task);
            } catch (Exception e) {
                taskTrackerFailed(task.getTaskTrackerName());
            }
        }
    }

    public MapperTask takeMapperTask()
            throws InterruptedException {
        return mapperTasksQueue.take();
    }

    public void updateTaskTracker(TaskTrackerInfo taskTracker){
        TaskTrackerInfo old = taskTackers.putIfAbsent(taskTracker.toString(), taskTracker);
        if(old != null){
            old.setTimestamp(System.currentTimeMillis());
            old.setMapperTaskNumber(taskTracker.getMapperTaskNumber());
            old.setReduceTaskNumber(taskTracker.getReducerTaskNumber());
        } else {
            taskTracker.setTimestamp(System.currentTimeMillis());
        }
    }

    public void mapperTaskSucceed(MapperTask task){
        JobInfo job = jobs.get(task.getJobId());
        if(!taskExpire(job, task)){
            LOG.debug("mapper task " + task.getTaskId() + " job " + task.getJobId() + " succeed");
            Task myTask = job.getTask(task.getTaskId());
            myTask.setStatus(TaskStatus.SUCCEED);
            sendReducerTask(job, task);
        }
    }

    public void mapperTaskFailed(MapperTask task){
        JobInfo job = jobs.get(task.getJobId());
        if(!taskExpire(job, task)){
            Task myTask = job.getTask(task.getTaskId());
            if(myTask.getAttemptCount() >= job.getConfig().getMaxAttemptCount()){
                LOG.debug("mapper task " + task.getTaskId() + " job " + task.getJobId() + " failed");
                myTask.setStatus(TaskStatus.FAILED);
            } else {
                if(!migrateTaskTracker(task)){
                    LOG.debug("mapper task " + task.getTaskId() + " job " + task.getJobId() + " failed");
                    myTask.setStatus(TaskStatus.FAILED);
                } else {
                    LOG.debug("mapper task " + task.getTaskId() + " job " + task.getJobId() + " retrying");
                    myTask.increaseAttemptCount();
                    myTask.setStatus(TaskStatus.PENDING);
                    mapperTasksQueue.offer((MapperTask) myTask);
                }
            }
        }
    }

    public void reducerTaskSucceed(ReducerTask task){
        JobInfo job = jobs.get(task.getJobId());
        if(!taskExpire(job, task)){
            LOG.debug("reducer task " + task.getTaskId() + " job " + task.getJobId() + " succeed");
            Task myTask = job.getTask(task.getTaskId());
            myTask.setStatus(TaskStatus.SUCCEED);
        }
    }

    public void reducerTaskFailed(ReducerTask task){
        JobInfo job = jobs.get(task.getJobId());
        if(!taskExpire(job, task)){
            Task myTask = job.getTask(task.getTaskId());
            if(myTask.getAttemptCount() >= job.getConfig().getMaxAttemptCount()){
                LOG.debug("reducer task " + task.getTaskId() + " job " + task.getJobId() + " failed");
                myTask.setStatus(TaskStatus.FAILED);
            } else {
                if(!migrateTaskTracker(task)){
                    LOG.debug("reducer task " + task.getTaskId() + " job " + task.getJobId() + " failed");
                    myTask.setStatus(TaskStatus.FAILED);
                } else {
                    LOG.debug("reducer task " + task.getTaskId() + " job " + task.getJobId() + " retrying");
                    myTask.increaseAttemptCount();
                    myTask.setStatus(TaskStatus.PENDING);
                    List<MapperTask> mapperTasks = job.getMapperTasks();
                    for(MapperTask mapperTask : mapperTasks){
                        sendReducerTask(task.getTaskTrackerName(), mapperTask, Arrays.asList(new ReducerTask[]{task}));
                    }
                }
            }
        }
    }

    public void reducerTaskFailedOnMapper(ReducerTask reducerTask, MapperTask mapperTask){
        JobInfo job = jobs.get(reducerTask.getJobId());
        if(!taskExpire(job, reducerTask)){
            LOG.debug("reducer task " + reducerTask.getTaskId() + " failed on mapper task " +
                     mapperTask.getTaskId() + " job " + reducerTask.getJobId());
            Task myTask = job.getTask(mapperTask.getTaskId());
            synchronized (myTask){
                if(myTask.getStatus() == TaskStatus.SUCCEED){
                    myTask.setStatus(TaskStatus.PENDING);
                    mapperTaskFailed(mapperTask);
                }
            }
        }
    }

    public void checkTaskTrackers(){
        for(TaskTrackerInfo taskTracker : taskTackers.values()){
            if(!taskTracker.isValid()){
                taskTrackerFailed(taskTracker.toString());
            }
        }
    }

    public boolean needHelp(){
        return help;
    }

    public int getFileServerPort(){
        return fileServerPort;
    }

    public String describeJobs(){
        Collection<JobInfo> jobs = this.jobs.values();
        List<JobInfo> jobList = new ArrayList<JobInfo>(jobs);
        Collections.sort(jobList);
        StringBuffer sb = new StringBuffer();
        if(jobList.size() == 0){
            sb.append("---No Jobs Here---\n");
        }
        for(JobInfo job : jobList){
            sb.append(job.describeJob() + "\n");
        }
        return sb.toString();
    }

    private void bindService()
            throws RemoteException, UnknownHostException {
        service = new JobTrackerServiceImpl(this);
        registry = LocateRegistry.getRegistry(Utils.getHost(), registryPort);
        registry.rebind(JobTrackerService.class.getCanonicalName(), service);
        registry.rebind(JobClientService.class.getCanonicalName(), new JobClientServiceImpl(this));
    }

    private void startScheduler(){
        scheduler = new Thread(new JobTrackerScheduler(this, threadPoolSize));
        scheduler.start();
        try {
            scheduler.join();
        } catch (InterruptedException e) {
            LOG.error("join of scheduler is interrupted", e);
        } finally {
            LOG.error("scheduler is exited!");
            System.exit(-1);
        }
    }

    private List<FileBlock> splitInputFile(JobInfo job)
            throws Exception {
        DFSFileSplitter splitter = null;
        try {
            splitter = new DFSFileSplitter(dfsMasterRegistryHost, dfsMasterRegistryPort);
            List<FileBlock> fileBlocks = splitter.split(job.getConfig().getInputFile(), job.getConfig().getMapperAmount());
            if(fileBlocks.size() == 0){
                throw new IllegalArgumentException("Invalid input file");
            }
            return fileBlocks;
        } catch (Exception e){
            job.setJobStatus(JobStatus.FAILED);
            throw e;
        }
    }

    private void generateMapperTasks(JobInfo job)
            throws Exception {
        List<FileBlock> fileBlocks = splitInputFile(job);
        job.getConfig().setMapperAmount(fileBlocks.size());
        for(FileBlock fileBlock : fileBlocks){
            MapperTask task = new MapperTask(job.getId(), fileBlock, job.getConfig().getReducerAmount());
            TaskTrackerInfo taskTracker = getMapperTaskTracker();
            if(taskTracker == null){
                job.setJobStatus(JobStatus.FAILED);
                throw new RemoteException("No available task tracker now");
            }
            task.setTaskTrackerName(taskTracker);
            task.setStatus(TaskStatus.PENDING);
            task.setMRClassName(job.getConfig().getClassName());
            job.addMapperTask(task);
        }
    }

    private void generateReducerTasks(JobInfo job)
            throws RemoteException {
        int reducerAmount = job.getConfig().getReducerAmount();
        for(int i = 0; i < reducerAmount; i++){
            ReducerTask task = new ReducerTask(job.getId());
            TaskTrackerInfo taskTracker = getReducerTaskTracker();
            if(taskTracker == null){
                job.setJobStatus(JobStatus.FAILED);
                throw new RemoteException("No available task tracker now");
            }
            task.setTaskTrackerName(taskTracker);
            task.setStatus(TaskStatus.PENDING);
            task.setMapperAmount(job.getConfig().getMapperAmount());
            task.setOutputFile(job.getConfig().getOutputFile());
            task.setLineCount(job.getConfig().getOutputFileBlockSize());
            task.setReplicas(job.getConfig().getOutputFileReplica());
            task.setPartitionIndex(i);
            task.setMRClassName(job.getConfig().getClassName());
            job.addReducerTask(task);
        }
    }

    private void dispatchMapperTasks(JobInfo job){
        List<MapperTask> mapperTasks = job.getMapperTasks();
        job.setJobStatus(JobStatus.PENDING);
        for(MapperTask mapperTask : mapperTasks){
            mapperTasksQueue.offer(mapperTask);
        }
    }

    private TaskTrackerInfo getMapperTaskTracker(){
        TaskTrackerInfo minTaskTracker = null;
        Iterator<TaskTrackerInfo> iterator = taskTackers.values().iterator();
        while(iterator.hasNext()){
            TaskTrackerInfo taskTracker = iterator.next();
            if(taskTracker.isValid() &&
               (minTaskTracker == null || taskTracker.getMapperTaskNumber() < minTaskTracker.getMapperTaskNumber())){
                minTaskTracker = taskTracker;
            }
        }
        if(minTaskTracker != null){
            minTaskTracker.increaseMapperTaskNumber();
        }
        return minTaskTracker;
    }

    private TaskTrackerInfo getReducerTaskTracker(){
        TaskTrackerInfo minTaskTracker = null;
        Iterator<TaskTrackerInfo> iterator = taskTackers.values().iterator();
        while(iterator.hasNext()){
            TaskTrackerInfo taskTracker = iterator.next();
            if(taskTracker.isValid() &&
               (minTaskTracker == null || taskTracker.getReducerTaskNumber() < minTaskTracker.getReducerTaskNumber())){
                minTaskTracker = taskTracker;
            }
        }
        if(minTaskTracker != null){
            minTaskTracker.increaseReducerTaskNumber();
        }
        return minTaskTracker;
    }

    private void sendReducerTask(JobInfo job, MapperTask mapperTask){
        List<ReducerTask> reducerTasks = job.getReducerTasks();
        Map<String, List<ReducerTask>> map = new HashMap<String, List<ReducerTask>>();
        for(ReducerTask reducerTask : reducerTasks){
            List<ReducerTask> subList = map.get(reducerTask.getTaskTrackerName());
            if(subList == null){
                subList = new ArrayList<ReducerTask>();
                map.put(reducerTask.getTaskTrackerName(), subList);
            }
            subList.add(reducerTask);
        }
        for(String taskTrackerName : map.keySet()){
            sendReducerTask(taskTrackerName, mapperTask, map.get(taskTrackerName));
        }
    }

    private void sendReducerTask(String taskTrackerName, MapperTask mapperTask, List<ReducerTask> reducerTasks) {
        TaskTrackerService taskTrackerService = null;
        try {
            TaskTrackerInfo taskTracker = taskTackers.get(taskTrackerName);
            Registry registry = LocateRegistry.getRegistry(taskTracker.getHost(), taskTracker.getRegistryPort());
            taskTrackerService = (TaskTrackerService)registry.lookup(taskTrackerName);
            taskTrackerService.runReducerTask(mapperTask, reducerTasks);
        } catch (Exception e) {
            taskTrackerFailed(taskTrackerName);
        }
    }

    private void taskTrackerFailed(String taskTrackerName){
        TaskTrackerInfo taskTracker = taskTackers.remove(taskTrackerName);
        LOG.warn("task tracker " + taskTrackerName + " is unavailable now! remain " + taskTackers.size());
        if(taskTracker != null){
            List<MapperTask> pendingMapperTasks = taskTracker.getPendingMapperTask();
            List<ReducerTask> pendingReducerTasks = taskTracker.getPendingReducerTask();
            TaskTrackerInfo newMapperTaskTracker = getMapperTaskTracker();
            TaskTrackerInfo newReducerTaskTracker = getReducerTaskTracker();

            if(newMapperTaskTracker != null && newReducerTaskTracker != null){
                for(MapperTask mapperTask : pendingMapperTasks){
                    migrateTaskTracker(mapperTask, newMapperTaskTracker);
                    mapperTasksQueue.offer(mapperTask);
                }
                for(ReducerTask reducerTask : pendingReducerTasks){
                    migrateTaskTracker(reducerTask, newReducerTaskTracker);
                }
                List<MapperTask> finishedMapperTasks = getFinishedMapper(pendingReducerTasks);
                for(MapperTask mapperTask : finishedMapperTasks){
                    sendReducerTask(newReducerTaskTracker.toString(), mapperTask, pendingReducerTasks);
                }
                return;
            }
            LOG.error("no available task tracker now");
            for(Task task : taskTracker.getPendingTasks()){
                task.setStatus(TaskStatus.FAILED);
            }
        }
    }

    private boolean taskExpire(JobInfo job, Task task){
        synchronized (job){
            if(job.getStatus() != JobStatus.PENDING){
                return true;
            }
            if(job.getTask(task.getTaskId()).getStatus() != TaskStatus.PENDING){
                return true;
            }
            if(!job.getTask(task.getTaskId()).getTaskTrackerName().equals(task.getTaskTrackerName())){
                return true;
            }
            if(job.getTask(task.getTaskId()).getAttemptCount() != task.getAttemptCount()){
                return true;
            }
            return false;
        }
    }

    private boolean migrateTaskTracker(Task task){
        TaskTrackerInfo taskTracker = getMapperTaskTracker();
        if(task instanceof ReducerTask){
            taskTracker = getReducerTaskTracker();
        }
        if(taskTracker == null){
            return false;
        }
        migrateTaskTracker(task, taskTracker);
        return true;
    }

    private void migrateTaskTracker(Task task, TaskTrackerInfo newTaskTracker){
        TaskTrackerInfo oldTaskTracker = taskTackers.get(task.getTaskTrackerName());
        if(oldTaskTracker != null){
            oldTaskTracker.removeTask(task);
        }
        task.setTaskTrackerName(newTaskTracker);
    }

    private List<MapperTask> getFinishedMapper(List<ReducerTask> reducerTasks){
        List<MapperTask> mapperTasks = new ArrayList<MapperTask>();
        for(ReducerTask reducerTask : reducerTasks){
            mapperTasks.addAll(getFinishedMapper(reducerTask));
        }
        return mapperTasks;
    }

    private List<MapperTask> getFinishedMapper(ReducerTask reducerTask){
        JobInfo job = jobs.get(reducerTask.getJobId());
        List<MapperTask> mapperTasks = new ArrayList<MapperTask>();
        for(MapperTask mapperTask : job.getMapperTasks()){
            if(mapperTask.getStatus() == TaskStatus.SUCCEED){
                mapperTasks.add(mapperTask);
            }
        }
        return mapperTasks;
    }

    public static void main(String[] args)
            throws Exception {
        JobTracker jobTracker = new JobTracker();
        JCommander commander = new JCommander(jobTracker, args);
        commander.setProgramName("mapreduce-jobtracker");
        if(jobTracker.needHelp()){
            commander.usage();
        } else {
            jobTracker.start();
        }
    }

}
