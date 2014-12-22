import Utils;
import DFSMasterService;
import DFSNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;

/**
 * The DFS slave periodically heart beat to DFS master
 * to acclaim healthy.
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */

public class DFSSlaveHeartbeatWorker implements Runnable {

    private static Logger LOG = LoggerFactory.getLogger(DFSSlaveHeartbeatWorker.class);

    private DFSSlave slave;
    private Registry masterRegistry;

    public DFSSlaveHeartbeatWorker(DFSSlave slave, Registry masterRegistry){
        this.masterRegistry = masterRegistry;
        this.slave = slave;
    }

    @Override
    public void run() {
        try {
            DFSMasterService masterService = (DFSMasterService)
                    masterRegistry.lookup(DFSMasterService.class.getCanonicalName());
            masterService.heartbeat(slave.getServiceName(), Utils.getHost(), slave.getRegistryPort(),
                    slave.getChunkNumber());
        } catch (RemoteException e) {
            LOG.error("master node error", e);
        } catch (NotBoundException e) {
            LOG.error("master service not found", e);
        } catch (UnknownHostException e) {
            LOG.error("can't resolve hostname");
        }
    }
}
