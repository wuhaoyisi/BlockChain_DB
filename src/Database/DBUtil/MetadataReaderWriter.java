package Database.DBUtil;

import Database.DBRepository;

import java.io.*;
import java.util.logging.Logger;
import static Database.DBUtil.Constants.*;

public class MetadataReaderWriter {

    private final String databaseName;
    private final DBRepository repo;
    public MetadataReaderWriter(String databaseName) {
        this.databaseName = databaseName;
        this.repo = new DBRepository(databaseName);
    }

    public void write(int PFSFileNum, int kvTableCount) {
        try {
            repo.write(METADATA_PFS_FILE_NUM, DB_NAME_OFFSET, METADATA_BLOCK_NUM, databaseName);
            repo.write(METADATA_PFS_FILE_NUM, DB_SIZE_OFFSET, METADATA_BLOCK_NUM, Integer.toString(FILE_SIZE));
            repo.write(METADATA_PFS_FILE_NUM, PFS_FILE_COUNT_OFFSET, METADATA_BLOCK_NUM, Integer.toString(PFSFileNum));
            repo.write(METADATA_PFS_FILE_NUM, BLOCK_SIZE_OFFSET, METADATA_BLOCK_NUM, Integer.toString(BLOCK_SIZE));
            repo.write(METADATA_PFS_FILE_NUM, KV_TABLE_OFFSET, METADATA_BLOCK_NUM, Integer.toString(kvTableCount));
            repo.write(METADATA_PFS_FILE_NUM, FILE_TYPE_MARKER_OFFSET, METADATA_BLOCK_NUM, METADATA_MARKER);
        } catch (IOException e) {
            Logger.getLogger(MetadataReaderWriter.class.getName()).severe(e.getMessage());
        }
    }

    public int getPFSFileCount() {
        try {
            StringBuilder data = new StringBuilder();
            for (int i = 0; i < METADATA_NUM_LENGTH_MAX; i++) {
                String digit = repo.readChar(METADATA_PFS_FILE_NUM, i + PFS_FILE_COUNT_OFFSET, METADATA_BLOCK_NUM);
                if (digit.equals(" ")) break;
                data.append(digit);
            }
            return Integer.parseInt(data.toString());
        } catch (IOException e) {
            Logger.getLogger(MetadataReaderWriter.class.getName()).severe(e.getMessage());
        }
        return -1;
    }

    public int getKVTableCount() {
        try {
            StringBuilder data = new StringBuilder();
            for (int i = 0; i < METADATA_NUM_LENGTH_MAX; i++) {
                String digit = repo.readChar(METADATA_PFS_FILE_NUM, i + KV_TABLE_OFFSET, METADATA_BLOCK_NUM);
                if (digit.equals(" ")) break;
                data.append(digit);
            }
            return Integer.parseInt(data.toString());
        } catch (IOException e) {
            Logger.getLogger(MetadataReaderWriter.class.getName()).severe(e.getMessage());
        }
        return -1;
    }
}
