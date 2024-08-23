package Database.DBUtil;

public final class Constants {
    // Offsets for FCB data
    public static final int TABLE_NAME_OFFSET = 0;
    public static final int TABLE_SIZE_OFFSET = 50;
    public static final int TABLE_TIME_OFFSET = 60;
    public static final int STARTING_DATA_BLOCK_OFFSET = 80;
    public static final int ROOT_INDEX_BLOCK_OFFSET = 90;
    public static final int ENDING_DATA_BLOCK_OFFSET = 100;

    // Offsets for database information
    public static final int BLOCK_SIZE = 256;
    public static final int FILE_SIZE = 1024 * 1024;

    // Offsets for metadata
    public static final int METADATA_PFS_FILE_NUM = 0;
    public static final int METADATA_BLOCK_NUM = 0;
    public static final int DB_NAME_OFFSET = 0;
    public static final int DB_SIZE_OFFSET = 50;
    public static final int PFS_FILE_COUNT_OFFSET = 60;
    public static final int BLOCK_SIZE_OFFSET = 70;
    public static final int KV_TABLE_OFFSET = 80;

    // Directory paths
    public static final String DATABASE_DIRECTORY = "./src/Database/PFSFiles";
    public static final String TABLE_DIRECTORY = "./src/KVTables";

    // FSM (Free Space Management) constants
    public static final int STARTING_FSM_BLOCK_NUM = 6;

    public static final int FSM_NUM_OF_BLOCK_PER_DIGIT = 4;

    public static final int ENDING_FSM_BLOCK_NUM = 9;
    public static final int FSM_BLOCK_OFFSET = 0;
    public static final int FSM_PFS_FILE_NUM = 0;

    // Record and block management
    public static final int RECORD_SIZE = 60;
    public static final int BLOCK_NUM_PER_FILE = FILE_SIZE / BLOCK_SIZE;
    public static final int RECORD_SLOT_OFFSET = 246;
    public static final int RECORD_SLOT_SIZE = BLOCK_SIZE / RECORD_SIZE;
    public static final int NEXT_BLOCK_NUM_OFFSET = 250;
    public static final int FILE_TYPE_MARKER_OFFSET = 255;

    // Markers for identifying the type of data in a block
    public static final String METADATA_MARKER = "M"; // Metadata
    public static final String FCB_MARKER = "T"; // Table
    public static final String DATA_MARKER = "R"; // Record
    public static final String INDEX_MARKER = "I"; // Index
    public static final String END_OF_FILE = "EOF";
    public static final String NULL_NODE_NUM = "99999";

    // Offsets for the B-tree Index Block
    public static final int BLOCK_NUM_LENGTH = 5;
    public static final int KEY_LENGTH = 8;
    public static final int NUM_OF_RECORDS = BLOCK_SIZE / RECORD_SIZE;

    public static final int PARENT_BLOCK_NUM_OFFSET = 250;

    public static final int METADATA_NUM_LENGTH_MAX = 10;

    public static final int STARTING_FCB_NUM = 1;

    public static final int ENDING_FCB_NUM = 5;

    public static final int FCB_AVAILABILITY_OFFSET = 254;
    public static final String AVAILABLE_MARKER = "0";
    public static final String NOT_AVAILABLE_MARKER = "1";

    public static final int FILE_NAME_LENGTH_MAX = 20;
    public static final int FCB_PFS_FILE_NUM = METADATA_PFS_FILE_NUM;
}
