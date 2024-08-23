package Database.DBUtil;

import Database.DBRepository;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;
import static Database.DBUtil.Constants.*;
import static Database.DBUtil.StringUtils.removeTrailingSpaces;

// Read and write the free space management block
public class FSMReaderWriter {

    private static final String INITIAL_FIRST_BITMAP = "ffc" + "0".repeat(BLOCK_SIZE - 3);
    private static final String FOLLOWING_FIRST_BITMAP = "03c" + "0".repeat(BLOCK_SIZE - 3);
    private static final String NON_FIRST_BITMAP = "0".repeat(BLOCK_SIZE);

    private static final HashMap<String, String> hexToBin = new HashMap<>() {{
        put("0", "0000");
        put("1", "0001");
        put("2", "0010");
        put("3", "0011");
        put("4", "0100");
        put("5", "0101");
        put("6", "0110");
        put("7", "0111");
        put("8", "1000");
        put("9", "1001");
        put("a", "1010");
        put("b", "1011");
        put("c", "1100");
        put("d", "1101");
        put("e", "1110");
        put("f", "1111");
    }};

    private static final HashMap<String, String> binToHex = new HashMap<>() {{
        put("0000", "0");
        put("0001", "1");
        put("0010", "2");
        put("0011", "3");
        put("0100", "4");
        put("0101", "5");
        put("0110", "6");
        put("0111", "7");
        put("1000", "8");
        put("1001", "9");
        put("1010", "a");
        put("1011", "b");
        put("1100", "c");
        put("1101", "d");
        put("1110", "e");
        put("1111", "f");
    }};

    private String databaseName;

    public FSMReaderWriter(String databaseName) {
        this.databaseName = databaseName;
    }

    public void initialize(int PFSFileNum) {
        try {
            DBRepository repo = new DBRepository(databaseName);
            if (PFSFileNum == 0) {
                repo.write(PFSFileNum, FSM_BLOCK_OFFSET, STARTING_FSM_BLOCK_NUM, INITIAL_FIRST_BITMAP);
            } else {
                repo.write(PFSFileNum, FSM_BLOCK_OFFSET, STARTING_FSM_BLOCK_NUM, FOLLOWING_FIRST_BITMAP);
            }
            for (int i = STARTING_FSM_BLOCK_NUM + 1; i <= ENDING_FSM_BLOCK_NUM; i++) {
                repo.write(PFSFileNum, FSM_BLOCK_OFFSET, i, NON_FIRST_BITMAP);
            }
        } catch (IOException e) {
            Logger.getLogger(FSMReaderWriter.class.getName()).severe(e.getMessage());
        }
    }

    public boolean getAvailability(int blockNum) {
        int currentBlockNum = blockNum % BLOCK_NUM_PER_FILE;
        int PFSFileNum = blockNum / BLOCK_NUM_PER_FILE;
        int offset = currentBlockNum % (FSM_NUM_OF_BLOCK_PER_DIGIT * BLOCK_SIZE);
        int FSMBlockNum = currentBlockNum / (FSM_NUM_OF_BLOCK_PER_DIGIT * BLOCK_SIZE);

        try {
            DBRepository repo = new DBRepository(databaseName);
            String hex = repo.readChar(PFSFileNum, offset, STARTING_FSM_BLOCK_NUM + FSMBlockNum);
            String binary = hexToBin.get(hex);
            return binary.charAt(blockNum % 4) == '0';
        } catch (IOException e) {
            Logger.getLogger(FSMReaderWriter.class.getName()).severe(e.getMessage());
        }
        return false;
    }

    public void setAvailability(int blockNum, boolean available) {
        int currentBlockNum = blockNum % BLOCK_NUM_PER_FILE;
        int PFSFileNum = blockNum / BLOCK_NUM_PER_FILE;
        int FSMBlockNum = currentBlockNum / (FSM_NUM_OF_BLOCK_PER_DIGIT * BLOCK_SIZE);
        int offset = (currentBlockNum % (FSM_NUM_OF_BLOCK_PER_DIGIT * BLOCK_SIZE)) / FSM_NUM_OF_BLOCK_PER_DIGIT;
        try {
            DBRepository repo = new DBRepository(databaseName);
            String hex = repo.readChar(PFSFileNum, offset, STARTING_FSM_BLOCK_NUM + FSMBlockNum);
            String binary = hexToBin.get(hex);
            StringBuilder newBinary = new StringBuilder(binary);
            newBinary.setCharAt(blockNum % FSM_NUM_OF_BLOCK_PER_DIGIT, available ? '0' : '1');
            String newHex = binToHex.get(newBinary.toString());
            repo.write(PFSFileNum, offset, STARTING_FSM_BLOCK_NUM + FSMBlockNum, newHex);
        } catch (IOException e) {
            Logger.getLogger(FSMReaderWriter.class.getName()).severe(e.getMessage());
        }
    }

    public int getNextAvailableBlock() {
        // Check the next available block.
        // If all full, create a new one and return the first block automatically.
        // BlockNum = PFSFileNum * BLOCK_NUM_PER_FILE + currentBlockNum
        DBRepository repo = new DBRepository(databaseName);
        MetadataReaderWriter metadataReaderWriter = new MetadataReaderWriter(databaseName);
        int totalPFSFileNum = metadataReaderWriter.getPFSFileCount();
//        System.out.println("totalPFSFileCount: " + totalPFSFileNum);
        for (int i = 0; i < totalPFSFileNum; i++) {
            int result = getNextAvailableBlock(i);
            if (result != -1) {
                return result;
            }
        }
        repo.createPFSFile(totalPFSFileNum);
        setAvailability(totalPFSFileNum * BLOCK_NUM_PER_FILE, false);
        return totalPFSFileNum * BLOCK_NUM_PER_FILE;
    }



    public int getNextAvailableBlock(int PFSFileNum) {
        DBRepository repo = new DBRepository(databaseName);
        for (int FSMBlockNum = 0; FSMBlockNum <= ENDING_FSM_BLOCK_NUM - STARTING_FSM_BLOCK_NUM; FSMBlockNum++) {
            for (int offset = 0; offset < BLOCK_SIZE; offset++) {
                try {
                    String hex = repo.readChar(PFSFileNum, offset, STARTING_FSM_BLOCK_NUM + FSMBlockNum);
                    String binary = hexToBin.get(hex);
                    for (int i = 0; i < FSM_NUM_OF_BLOCK_PER_DIGIT; i++) {
                        if (binary.charAt(i) == '0') {
                            int blockNum = PFSFileNum * BLOCK_NUM_PER_FILE
                                    + FSMBlockNum * BLOCK_SIZE * FSM_NUM_OF_BLOCK_PER_DIGIT
                                    + offset * FSM_NUM_OF_BLOCK_PER_DIGIT
                                    + i;
                            setAvailability(blockNum, false);
                            return blockNum;
                        }
                    }
                } catch (IOException e) {
                    Logger.getLogger(FSMReaderWriter.class.getName()).severe(e.getMessage());
                }
            }
        }
        return -1;
    }
}
