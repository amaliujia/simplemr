import DFSChunk;
import DFSFile;
import DFSMasterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/**
 * Implementation of service class. The class contains a DFSMetaData
 * instance that change according to the users requests.
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */

public class DFSMasterServiceImpl extends UnicastRemoteObject implements DFSMasterService {

    private static Logger LOG = LoggerFactory.getLogger(DFSMasterServiceImpl.class);

    private DFSMetaData metaData;

    protected DFSMasterServiceImpl(DFSMetaData metaData) throws RemoteException {
        super();
        this.metaData = metaData;
    }

    @Override
    public void heartbeat(String serviceName, String registryHost,
                          int registryPort, int chunkNumber) throws RemoteException {
        metaData.updateDataNode(serviceName, registryHost, registryPort,
                chunkNumber, System.currentTimeMillis(), true);
    }

    @Override
    public DFSFile createFile(String fileName, int replicas) throws RemoteException {
        LOG.debug("create file " + fileName + ", replica number " + replicas);
        return metaData.createFile(fileName, replicas, true);
    }

    @Override
    public DFSFile getFile(String fileName) throws RemoteException {
        LOG.debug("get file " + fileName);
        return metaData.getFile(fileName);
    }

    @Override
    public DFSFile[] listFiles() throws RemoteException {
        LOG.debug("list files");
        return metaData.listFiles();
    }

    @Override
    public DFSChunk createChunk(long fileId, long offset, int size) throws RemoteException {
        LOG.debug("create chunk for file " + fileId + ", offset " + offset + ", size " + size);
        return metaData.createChunk(fileId, offset, size, true);
    }

    @Override
    public void deleteFile(long fileId) throws RemoteException {
        LOG.debug("delete file " + fileId);
        metaData.deleteFile(fileId, true);
    }
}
