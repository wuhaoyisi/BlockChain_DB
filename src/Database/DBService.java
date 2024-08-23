package Database;

import Database.DBUtil.*;

import java.io.*;
import java.util.logging.Logger;
import static Database.DBUtil.Constants.*;
import static Database.DBUtil.StringUtils.removeTrailingSpaces;


public class DBService {
    private int PFSFileCount;
    private DBRepository dbRepository;
    private String databaseName;

    // Constructor: Initializes the DBService with a specific database name
    public DBService(String databaseName) throws IllegalArgumentException {
        if (databaseName.length() > FILE_NAME_LENGTH_MAX) {
            throw new IllegalArgumentException("Database name is too long. Max length is " + FILE_NAME_LENGTH_MAX);
        }
        this.PFSFileCount = 0;
        this.dbRepository = new DBRepository(databaseName);
        this.databaseName = databaseName;
    }

    public void create() {
        // Creates a new database file
        dbRepository.createPFSFile(PFSFileCount);
//        MetadataReaderWriter metadataReaderWriter = new MetadataReaderWriter(databaseName);
//        metadataReaderWriter.write(1, 0);
//        FSMReaderWriter fsmReaderWriter = new FSMReaderWriter(databaseName);
//        fsmReaderWriter.initialize(0);
//        FCBReaderWriter fcbReaderWriter = new FCBReaderWriter(databaseName);
//        fcbReaderWriter.initialize();
    }

    public void put(String tableName) throws IllegalArgumentException {
        if (tableName.length() > FILE_NAME_LENGTH_MAX) {
            throw new IllegalArgumentException("Table name is too long. Max length is " + FILE_NAME_LENGTH_MAX);
        }
        // TableName example: "movies-test.csv"

        System.out.println("Putting data into table: " + tableName);
        DataIndexFileWriter writer = new DataIndexFileWriter(tableName, databaseName);
        writer.write();

        // Update the Metadata
        MetadataReaderWriter metadataReaderWriter = new MetadataReaderWriter(databaseName);
        int kvTableCount = metadataReaderWriter.getKVTableCount();
        metadataReaderWriter.write(1, kvTableCount + 1);
        writer.getBTree().display();
    };

    public void get(String OSPath, String tableName) {
        File dir = new File(OSPath);
        if (!dir.exists()) dir.mkdirs();
        File file = new File(OSPath, tableName);
        FCBReaderWriter fcbReaderWriter = new FCBReaderWriter(databaseName);
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            int blockNum = fcbReaderWriter.getStartingBlockNumByTableName(tableName);
            do {
                int currentBlockNum = blockNum % BLOCK_NUM_PER_FILE;
                int currentFileNum = blockNum / BLOCK_NUM_PER_FILE;
                for (int slot = 0; slot < NUM_OF_RECORDS; slot++) {
                    boolean hasData = dbRepository.readChar(currentFileNum,
                            RECORD_SLOT_OFFSET + slot,
                            currentBlockNum).equals("1");
                    if (!hasData) continue;
                    String record = dbRepository.read(currentFileNum, slot * RECORD_SIZE, currentBlockNum, RECORD_SIZE);
                    writer.write(removeTrailingSpaces(record));
                    writer.newLine();
                }
                String nextBlock = dbRepository.read(currentFileNum, NEXT_BLOCK_NUM_OFFSET, currentBlockNum, BLOCK_NUM_LENGTH);
                if (nextBlock.equals("EOF  ")) break;
                blockNum = Integer.parseInt(nextBlock);
            } while (true);
        } catch (IOException e) {
            Logger.getLogger(DBService.class.getName()).severe("Error writing to file: " + e.getMessage());
        }
    };

    public void rm(String tableName) {
        // Remove a table by traversing the index tree
        FCBReaderWriter fcbReaderWriter = new FCBReaderWriter(databaseName);
        DataIndexFileRemover remover = new DataIndexFileRemover(databaseName, tableName);
        remover.remove();
    };

    public void dir() {
        // Lists all the metadata and FCB info
        File directory = new File(DATABASE_DIRECTORY);
        File[] files = directory.listFiles((dir, name) -> name.endsWith(".db0"));

        if (files != null) {
            for (File file : files) {
                String databaseName = file.getName().split("\\.db")[0];
                System.out.println("Database: " + databaseName);

                try {
                    // Read metadata from the METADATA_BLOCK_NUM
                    StringBuilder metadata = new StringBuilder();
                    for (int i = 0; i < 90; i++) {
                        metadata.append(dbRepository.readChar(METADATA_PFS_FILE_NUM, i, METADATA_BLOCK_NUM));
                    }
                    String metadataName = metadata.length() >= 50 ? metadata.substring(0, 50).trim() : metadata.toString();
                    String metadataSize = metadata.length() >= 60 ? metadata.substring(50, 60).trim() : "N/A";
                    String metadataCount = metadata.length() >= 80 ? metadata.substring(80, 90).trim() : "N/A";

                    System.out.println("MetaData: " + metadataName + "  Size: " + metadataSize + "  FileCount: " + metadataCount);


                    // Read file-specific data from the FCB_BLOCK_NUM
                    for (int i = STARTING_FCB_NUM; i < ENDING_FCB_NUM; i++) {
                        StringBuilder fcbData = new StringBuilder();
                        for (int j = 0; j < STARTING_DATA_BLOCK_OFFSET; j++) {
                            fcbData.append(dbRepository.readChar(METADATA_PFS_FILE_NUM, j, i));
                        }
                        if (fcbData.toString().trim().isEmpty()) {
                            continue;
                        }
                        String fcbDataName = fcbData.length() >= 50 ? fcbData.substring(0, 50).trim() : fcbData.toString();
                        String fcbDataSize = fcbData.length() >= 60 ? fcbData.substring(50, 60).trim() : "N/A";
                        String fcbDataTime = fcbData.length() >= 70 ? fcbData.substring(60, 70).trim() : "N/A";

                        System.out.println("FCB Data: " + fcbDataName + "  Size: " + fcbDataSize + "  Time: " + fcbDataTime);
                        fcbData.append("\n");
                    }

                } catch (IOException e) {
                    Logger.getLogger(DBService.class.getName()).severe("Error reading file data: " + e.getMessage());
                }
            }
        } else {
            System.out.println("No PFS files found in the database directory.");
        }
    }

    public void find(String tableName, int key) {
        DataIndexFileReader reader = new DataIndexFileReader(databaseName, tableName);
        System.out.println(reader.find(key));
    };

    public void kill(String databaseName) {
        // Deletes all files related to a specific database
        File directory = new File(DATABASE_DIRECTORY);
        File[] files = directory.listFiles((dir, name) -> name.startsWith(databaseName + ".db"));

        if (files != null) {
            for (File file : files) {
                if (file.delete()) {
                    System.out.println("Deleted file: " + file.getName());
                } else {
                    System.out.println("Failed to delete file: " + file.getName());
                }
            }
        } else {
            System.out.println("No PFS files found for the database: " + databaseName);
        }
    }

    public void quit() {
        System.out.println("Closing the database and exiting.");
    };

}
