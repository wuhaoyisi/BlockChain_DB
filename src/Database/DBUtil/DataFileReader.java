package Database.DBUtil;

import Database.DBRepository;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.logging.Logger;

import static Database.DBUtil.Constants.*;
public class DataFileReader {

    private final String databaseName;
    private final String tableName;
    private DBRepository repo;

    private FCBReaderWriter FCBReaderWriter;

    private final String workingDirectory;

    public DataFileReader(String databaseName, String tableName, String workingDirectory) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.workingDirectory = workingDirectory;
        this.repo = new DBRepository(databaseName);
        this.FCBReaderWriter = new FCBReaderWriter(databaseName);
    }

    public void readDataFile() {
        String startingBlock = FCBReaderWriter.getStartingDataBlock(tableName);
        if (startingBlock == null) {
            Logger.getLogger(DataFileReader.class.getName()).severe("Table not found");
            return;
        }
        int blockNum = Integer.parseInt(startingBlock);
        File tableFile = new File(workingDirectory + "/" + tableName);
        try (FileWriter writer = new FileWriter(tableFile)) {
            while (true) {
                int currentBlock = blockNum % BLOCK_NUM_PER_FILE;
                int PFSFileNum = blockNum / BLOCK_NUM_PER_FILE;
                for (int i = 0; i < NUM_OF_RECORDS; i++) {
                    if (repo.readChar(PFSFileNum, RECORD_SLOT_OFFSET + i, currentBlock).equals("1")){
                        String record = repo.read(FSM_PFS_FILE_NUM, i * RECORD_SIZE, currentBlock, RECORD_SIZE);
                        writer.write(record);
                    }
                }
                String nextBlock = repo.read(FSM_PFS_FILE_NUM, 250, currentBlock, 5);
                if (nextBlock.equals("EOF  ")) {
                    break;
                } else {
                    blockNum = Integer.parseInt(nextBlock);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
