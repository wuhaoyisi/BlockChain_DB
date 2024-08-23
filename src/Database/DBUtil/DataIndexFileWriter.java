package Database.DBUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

import BTree.BTree;
import BTree.BTreeUtil.BTreeNode;
import BTree.BTreeUtil.InternalNode;
import BTree.BTreeUtil.LeafNode;
import Database.DBRepository;
import static Database.DBUtil.Constants.*;

// Data File Design:
// 60 bytes per record * 4
// 1 byte to mark it is a data file (byte 245)
// 4 bytes for record slot availability (byte 246 - 249)
// 5 bytes for next block number (byte 250 - 254)
// 1 byte for data file marker (byte 255)
// 251 in total
public class DataIndexFileWriter {

    private DBRepository repo;

    private FSMReaderWriter fsmReaderWriter;
    private FCBReaderWriter fcbReaderWriter;

    private BTree bTree;

    private int indexRootBlockNum;
    private int dataStartingBlockNum = -1;
    private int dataEndingBlockNum;
    private String tableName;



    public DataIndexFileWriter(String tableName, String databaseName) {
        this.repo = new DBRepository(databaseName);
        this.fsmReaderWriter = new FSMReaderWriter(databaseName);
        this.fcbReaderWriter = new FCBReaderWriter(databaseName);
        this.bTree = new BTree();
        this.tableName = tableName;
    }

    // Read the data from the csv tableFile and write it into the PFSFiles
    //    int PFSFileNum, int offset, int blockNum, String content

    // Write the entire table into the block
    public void write() {
        writeDataFile();
        writeIndexFile();
        updateFCB();
    }

    public void writeDataFile() {
        File tableFile = new File(TABLE_DIRECTORY + "/" + tableName);
        try (BufferedReader reader = new BufferedReader(new FileReader(tableFile))) {
            Queue<Integer> availableSlots = new LinkedList<>();
            int blockNum = -1;
            int currentBlockNum = -1;
            int PFSFileNum = 0;
            int slotNum;
            String line;
            List<Integer> blockNums = new ArrayList<>();
            while ((line = reader.readLine()) != null) {
                if (!availableSlots.isEmpty()) {
                    slotNum = availableSlots.poll();
                } else {
                    blockNum = fsmReaderWriter.getNextAvailableBlock();
                    if (dataStartingBlockNum == -1) {
                        dataStartingBlockNum = blockNum;
                    }
                    blockNums.add(blockNum);
                    // When a new block is applied, initialize the block
                    currentBlockNum = blockNum % BLOCK_NUM_PER_FILE;
                    PFSFileNum = blockNum / BLOCK_NUM_PER_FILE;
                    repo.write(PFSFileNum, RECORD_SLOT_OFFSET, currentBlockNum, AVAILABLE_MARKER.repeat(NUM_OF_RECORDS));
                    repo.write(PFSFileNum, FILE_TYPE_MARKER_OFFSET, currentBlockNum, DATA_MARKER);
                    // Add all slots from 0 to 3 to availableSlots
                    for (int i = 0; i < NUM_OF_RECORDS; i++) {
                        availableSlots.add(i);
                    }
                    slotNum = availableSlots.poll();
                }

                // Writing process
                // Step 1: Write the record into the block
                // Trim the line if it exceeds the size limit
                if (line.length() > RECORD_SIZE) {
                    line = line.substring(0, RECORD_SIZE);
                }
                repo.write(PFSFileNum, slotNum * RECORD_SIZE, currentBlockNum, line);
                // Step 2: Mark the slot as occupied
                repo.write(PFSFileNum, RECORD_SLOT_OFFSET + slotNum, currentBlockNum, NOT_AVAILABLE_MARKER);
                // Step 3: Update FSM if all slots are occupied
                if (repo.read(PFSFileNum, RECORD_SLOT_OFFSET, currentBlockNum, RECORD_SLOT_SIZE).
                        equals("1111")) {
                    fsmReaderWriter.setAvailability(blockNum, false);
                }
                // Step 4: Add id to the BTree
                String idString = getRecordId(line);
                if (idString.matches("[0-9]+")) {
                    int id = Integer.parseInt(idString);
                    bTree.insert(id, blockNum);
                }
            }
            System.out.println("Data file completed");

            // Update the number of next block for each block occupied
            for (int i = 0; i < blockNums.size() - 1; i++) {
                blockNum = blockNums.get(i);
                currentBlockNum = blockNum % BLOCK_NUM_PER_FILE;
                PFSFileNum = blockNum / BLOCK_NUM_PER_FILE;
                String next = blockNumTo5Digits(blockNums.get(i + 1));
                repo.write(PFSFileNum, NEXT_BLOCK_NUM_OFFSET, currentBlockNum, next);
            }
            // Mark the last block as the ending block
            dataEndingBlockNum = blockNums.get(blockNums.size() - 1);
            repo.write(dataEndingBlockNum / BLOCK_NUM_PER_FILE,
                    NEXT_BLOCK_NUM_OFFSET,
                    dataEndingBlockNum % BLOCK_NUM_PER_FILE,
                    END_OF_FILE);

        } catch (IOException e) {
            Logger.getLogger(DataIndexFileWriter.class.getName()).severe(e.getMessage());
        }
    }
    public void writeIndexFile() {
        try {
            HashSet<BTreeNode> nodes = bTree.getNodes();
            HashMap<Integer, Integer> keyBlock = bTree.getKeyBlock(); // Get the data block number where the key is in
            HashMap<BTreeNode, Integer> nodeBlockNums = new HashMap<>(); // Record the index node's index block number
            for (BTreeNode node : nodes) {
                int blockNum = fsmReaderWriter.getNextAvailableBlock();
                nodeBlockNums.put(node, blockNum);
            }
            for (BTreeNode node : nodes) {
                int blockNum = nodeBlockNums.get(node);
                int currentBlockNum = blockNum % BLOCK_NUM_PER_FILE;
                int PFSFileNum = blockNum / BLOCK_NUM_PER_FILE;
                String serializedNode = serializeBTreeNode(node, keyBlock, nodeBlockNums);
                repo.write(PFSFileNum, 0, currentBlockNum, serializedNode);
                repo.write(PFSFileNum, FILE_TYPE_MARKER_OFFSET, currentBlockNum, INDEX_MARKER);
            }

            indexRootBlockNum = nodeBlockNums.get(bTree.getRoot());
        } catch (IOException e) {
            Logger.getLogger(DataIndexFileWriter.class.getName()).severe(e.getMessage());
        }
    }

    public void updateFCB() {
        File tableFile = new File(TABLE_DIRECTORY + "/" + tableName);
        String size = String.valueOf(tableFile.length());
        int FCBNum = fcbReaderWriter.getNextAvailableFCB();
        String dateTime = fcbReaderWriter.getCurrentDataTime();
        fcbReaderWriter.write(
                tableName,
                size,
                dateTime,
                blockNumTo5Digits(dataStartingBlockNum),
                blockNumTo5Digits(indexRootBlockNum),
                blockNumTo5Digits(dataEndingBlockNum),
                FCBNum
        );
    }

    public String serializeBTreeNode(BTreeNode node, HashMap <Integer, Integer> keyBlock, HashMap <BTreeNode, Integer> nodeBlockNums) {
        if (node instanceof InternalNode) {
            return serializeBTreeNode((InternalNode) node, keyBlock, nodeBlockNums);
        } else {
            return serializeBTreeNode((LeafNode) node, keyBlock, nodeBlockNums);
        }
    }

    public String serializeBTreeNode(InternalNode node, HashMap <Integer, Integer> keyBlock, HashMap <BTreeNode, Integer> nodeBlockNums) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < node.getKeys().size(); i++) {
            String childBlockNum = blockNumTo5Digits(nodeBlockNums.get(node.getChildren().get(i)));
            String key = keyTo8Digits(node.getKeys().get(i));
            String keyBlockNum = blockNumTo5Digits(keyBlock.get(node.getKeys().get(i)));
            result.append(childBlockNum).append(key).append(keyBlockNum);
        }
        result.append(blockNumTo5Digits(nodeBlockNums.get(node.getChildren().get(node.getChildren().size() - 1))));
        return result.toString();
    }

    public String serializeBTreeNode(LeafNode node, HashMap <Integer, Integer> keyBlock, HashMap <BTreeNode, Integer> nodeBlockNums) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < node.getKeys().size(); i++) {
            String key = keyTo8Digits(node.getKeys().get(i));
            String keyBlockNum = blockNumTo5Digits(keyBlock.get(node.getKeys().get(i)));
            result.append(NULL_NODE_NUM).append(key).append(keyBlockNum);
        }
        result.append(NULL_NODE_NUM);
        return result.toString();
    }

    // Write a record into the block
    public void writeDataRecord(String record, int blockNum, int slotNum) {
        try {
            int PFSFileNum = blockNum / BLOCK_NUM_PER_FILE;
            int currentBlockNum = blockNum % BLOCK_NUM_PER_FILE;
            repo.write(PFSFileNum, slotNum * RECORD_SIZE, currentBlockNum, record);
            repo.write(PFSFileNum, RECORD_SLOT_OFFSET + slotNum, currentBlockNum, "1");
            if (repo.read(PFSFileNum, RECORD_SLOT_OFFSET, currentBlockNum, RECORD_SLOT_SIZE).equals("1111")) {
                fsmReaderWriter.setAvailability(blockNum, false);
            }
        } catch (IOException e) {
            Logger.getLogger(DataIndexFileWriter.class.getName()).severe(e.getMessage());
        }
    }

    public void writeNextBlockNum(int blockNum, int nextBlockNum) {
        try {
            int PFSFileNum = blockNum / BLOCK_NUM_PER_FILE;
            int currentBlockNum = blockNum % BLOCK_NUM_PER_FILE;
            repo.write(PFSFileNum, 250, currentBlockNum, blockNumTo5Digits(nextBlockNum));
        } catch (IOException e) {
            Logger.getLogger(DataIndexFileWriter.class.getName()).severe(e.getMessage());
        }
    }

    public void initializeDataFile(int blockNum) {
        try {
            int currentBlockNum = blockNum % BLOCK_NUM_PER_FILE;
            int PFSFileNum = blockNum / BLOCK_NUM_PER_FILE;
            repo.write(PFSFileNum, 246, currentBlockNum, "0000");
            repo.write(PFSFileNum, 255, currentBlockNum, DATA_MARKER);
        } catch (IOException e) {
            Logger.getLogger(DataIndexFileWriter.class.getName()).severe(e.getMessage());
        }
    }

    public String blockNumTo5Digits(int blockNum) {
        StringBuilder result = new StringBuilder(String.valueOf(blockNum));
        while (result.length() < 5) {
            result.insert(0, "0");
        }
        return result.toString();
    }

    public String keyTo8Digits(int key) {
        StringBuilder result = new StringBuilder(String.valueOf(key));
        while (result.length() < 8) {
            result.insert(0, "0");
        }
        return result.toString();
    }

    private int getAvailableSlot(int blockNum) {
        try {
            int PFSFileNum = blockNum / BLOCK_NUM_PER_FILE;
            int currentBlockNum = blockNum % BLOCK_NUM_PER_FILE;
            String availability = repo.read(PFSFileNum, RECORD_SLOT_OFFSET, currentBlockNum, RECORD_SLOT_SIZE);
            for (int i = 0; i < availability.length(); i++) {
                if (availability.charAt(i) == '0') {
                    return i;
                }
            }
        } catch (IOException e) {
            Logger.getLogger(DataIndexFileWriter.class.getName()).severe(e.getMessage());
        }
        return -1;
    }

    private String getRecordId(String record) {
        System.out.println("record: " + record);
        return record.split(",")[0];
    }

    public BTree getBTree() {
        return bTree;
    }
}
