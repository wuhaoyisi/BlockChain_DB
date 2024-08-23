package Database.DBUtil;

import Database.DBRepository;

import java.io.IOException;
import java.util.logging.Logger;

import static Database.DBUtil.Constants.*;
import static Database.DBUtil.StringUtils.*;

public class DataIndexFileReader {

    private String databaseName;

    private String tableName;

    private FCBReaderWriter FCBReaderWriter;

    private DBRepository repo;

    private int blockAccessed = 0;

    public DataIndexFileReader(String databaseName, String tableName) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.repo = new DBRepository(databaseName);
        this.FCBReaderWriter = new FCBReaderWriter(databaseName);
    }

    public String find(int key) {
        String indexBlock = findIndexBlock(key);
        if (indexBlock == null) {
            return "Cannot find the record with key " + key;
        }
        StringBuilder sb = new StringBuilder();
        String record = retrieveRecordByKey(key, Integer.parseInt(indexBlock));
        sb.append(record).append("\n").append("# of blocks: ").append(blockAccessed);
        return sb.toString();
    }

    public String findIndexBlock(int key) {
        int rootNum = FCBReaderWriter.getRootIndexBlockNum(tableName);
        if (rootNum == -1) {
            Logger.getLogger(DataIndexFileReader.class.getName()).severe("Table not found");
            return null;
        }
        return findIndexBlock(key, rootNum);
    }

    // Search base on the ascending order of the key in the indexing block
    private String findIndexBlock(int key, int blockNum) {
        blockAccessed++;
        int currentBlock = blockNum % BLOCK_NUM_PER_FILE;
        int PFSFileNum = blockNum / BLOCK_NUM_PER_FILE;
        int currentKeyOffset = BLOCK_NUM_LENGTH; // Start from the first key
        do {
            try {
                String currentKey = repo.read(PFSFileNum, currentKeyOffset, currentBlock, KEY_LENGTH);
                if (currentKey.equals(" ".repeat(KEY_LENGTH))) {
                    return findIndexBlock(key, Integer.parseInt(repo.read(PFSFileNum, currentKeyOffset - BLOCK_NUM_LENGTH, currentBlock, BLOCK_NUM_LENGTH)));
                }
                if(!currentKey.matches("\\d+")){
                    return "Key is not found in the database";
                }; // Check if the key is a number (integer
                int currentKeyInt = Integer.parseInt(currentKey);
                if (key < currentKeyInt) {
                    return findIndexBlock(key, Integer.parseInt(repo.read(PFSFileNum, currentKeyOffset - BLOCK_NUM_LENGTH, currentBlock, BLOCK_NUM_LENGTH)));
                } else if (key == currentKeyInt) {
                    return repo.read(PFSFileNum, currentKeyOffset + KEY_LENGTH, currentBlock, BLOCK_NUM_LENGTH);
                }
                currentKeyOffset += KEY_LENGTH + 2 * BLOCK_NUM_LENGTH;
            } catch (IOException e) {
                Logger.getLogger(DataIndexFileReader.class.getName()).severe(e.getMessage());
            }
        } while (currentKeyOffset < BLOCK_SIZE);
        return null;
    }

    private String retrieveRecordByKey(int key, int blockNum) {
        int currentBlock = blockNum % BLOCK_NUM_PER_FILE;
        int PFSFileNum = blockNum / BLOCK_NUM_PER_FILE;
        try {
            for (int slot = 0; slot < RECORD_SLOT_SIZE; slot++) {
                if (repo.readChar(PFSFileNum, RECORD_SLOT_OFFSET + slot, currentBlock).equals("1")) {
                    String record = removeTrailingSpaces(repo.read(PFSFileNum,
                            slot * RECORD_SIZE,
                            currentBlock,
                            RECORD_SIZE));
                    int recordKey = Integer.parseInt(record.split(",")[0]);
                    if (recordKey == key) {
                        return record;
                    }
                }
            }
        } catch (IOException e) {
            Logger.getLogger(DataIndexFileReader.class.getName()).severe(e.getMessage());
        }
        return null;
    }
}
