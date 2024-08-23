package Database.DBUtil;

import Database.DBRepository;

import java.io.IOException;
import java.util.Objects;
import java.util.Queue;
import java.util.logging.Logger;

import static Database.DBUtil.Constants.*;

public class DataIndexFileRemover {

    private String databaseName;
    private String tableName;
    private DBRepository repo;

    private FCBReaderWriter FCBReaderWriter;

    public DataIndexFileRemover(String databaseName, String tableName) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.repo = new DBRepository(databaseName);
        this.FCBReaderWriter = new FCBReaderWriter(databaseName);
    }

    public void remove() {
        removeData();
        removeFCB();
    }

    public void removeData() {
        int rootNum = FCBReaderWriter.getRootIndexBlockNum(tableName);
        if (rootNum == -1) {
            Logger.getLogger(DataIndexFileRemover.class.getName()).severe("Table not found");
            return;
        }
        System.out.println("Root index block: " + rootNum);
        // Run BFS to clear all data and index blocks
        try {
            Queue<Integer> queue = new java.util.LinkedList<>();
            queue.add(rootNum);
            while (!queue.isEmpty()) {
                int blockNum = queue.poll();
                int currentBlock = blockNum % BLOCK_NUM_PER_FILE;
                int PFSFileNum = blockNum / BLOCK_NUM_PER_FILE;
                if (repo.readChar(PFSFileNum, FILE_TYPE_MARKER_OFFSET, currentBlock).equals(DATA_MARKER)) {
                    clearBlock(blockNum);
                    continue;
                }
                int currentOffset = 0;
                do {
                    String indexBlock = repo.read(PFSFileNum, currentOffset, currentBlock, BLOCK_NUM_LENGTH);
                    if (!Objects.equals(indexBlock, " ".repeat(BLOCK_NUM_LENGTH)) && !Objects.equals(indexBlock, "99999")) {
                        queue.add(Integer.parseInt(indexBlock));
                    } else if (indexBlock.equals(" ".repeat(BLOCK_NUM_LENGTH))) {
                        break;
                    }
                    String dataBlock = repo.read(PFSFileNum, currentOffset + KEY_LENGTH + BLOCK_NUM_LENGTH, currentBlock, BLOCK_NUM_LENGTH);
                    if (!Objects.equals(dataBlock, " ".repeat(BLOCK_NUM_LENGTH)) && !Objects.equals(dataBlock, "99999")) {
                        queue.add(Integer.parseInt(dataBlock));
                    }
                    currentOffset += BLOCK_NUM_LENGTH + KEY_LENGTH + BLOCK_NUM_LENGTH;
                } while (currentOffset < BLOCK_SIZE);
                clearBlock(blockNum);
            }
        } catch (IOException e) {
            Logger.getLogger(DataIndexFileRemover.class.getName()).severe(e.getMessage());
        }
    }

    public void removeFCB() {
        FCBReaderWriter fcbReaderWriter = new Database.DBUtil.FCBReaderWriter(databaseName);
        int fcbNum = fcbReaderWriter.getFCBNumByTableName(tableName);
        fcbReaderWriter.clear(fcbNum);
        MetadataReaderWriter metadataReaderWriter = new MetadataReaderWriter(databaseName);
        metadataReaderWriter.write(
                metadataReaderWriter.getPFSFileCount(),
                metadataReaderWriter.getKVTableCount() - 1);
    }

    public void clearBlock(int blockNum) {
        try {
            int currentBlock = blockNum % Constants.BLOCK_NUM_PER_FILE;
            int PFSFileNum = blockNum / Constants.BLOCK_NUM_PER_FILE;
            repo.write(PFSFileNum, 0, currentBlock, " ".repeat(BLOCK_SIZE));
            FSMReaderWriter fsmReaderWriter = new FSMReaderWriter(databaseName);
            fsmReaderWriter.setAvailability(blockNum, true);
        } catch (IOException e) {
            Logger.getLogger(DataIndexFileRemover.class.getName()).severe(e.getMessage());
        }
    }

}
