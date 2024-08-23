package Database;

import Database.DBUtil.FCBReaderWriter;
import Database.DBUtil.FSMReaderWriter;
import Database.DBUtil.MetadataReaderWriter;

import java.io.*;
import java.nio.charset.StandardCharsets.*;
import java.util.Arrays;
import java.util.logging.Logger;
import static Database.DBUtil.Constants.*;
import static java.nio.charset.StandardCharsets.UTF_8;

public class DBRepository {

    private final String databaseName;

    public DBRepository(String databaseName) {
        this.databaseName = databaseName;
    }

    // Directory: the relative path from the root directory
    //     Example: "./src/Database/PFSFiles"
    // Creates a PFS file and initializes it with empty blocks
    public void createPFSFile(int PFSFileCount) {
        // Create a new file
        String name = databaseName + ".db" + PFSFileCount;
        File directory = new File(DATABASE_DIRECTORY);
        if (!directory.exists()) {
            directory.mkdirs();
        }
        File file = new File(DATABASE_DIRECTORY, name);
        try {
            boolean created = file.createNewFile();
            if (created) {
                System.out.println("File created: " + file.getName());
            } else {
                System.out.println("File already exists.");
            }
        } catch (IOException e) {
            Logger.getLogger(DBRepository.class.getName()).severe(e.getMessage());
        }

        // Fill the file with empty blocks to initialize it
        int numBlocks = FILE_SIZE / BLOCK_SIZE;
        char[] blockContent = new char[BLOCK_SIZE];
        Arrays.fill(blockContent, ' ');
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            for (int i = 0; i < numBlocks; i++) {
                writer.write(blockContent);
            }
        } catch (IOException e) {
            Logger.getLogger(DBRepository.class.getName()).severe(e.getMessage());
        }

        // Initialize the metadata, FSM, and FCB
        System.out.println("Running MetadataReaderWriter, PFSFileCount: " + PFSFileCount);
        if (PFSFileCount == 0) {
            MetadataReaderWriter metadataReaderWriter = new MetadataReaderWriter(databaseName);
            metadataReaderWriter.write(1, 0);
            FCBReaderWriter fcbReaderWriter = new FCBReaderWriter(databaseName);
            fcbReaderWriter.initialize();
        } else {
            MetadataReaderWriter metadataReaderWriter = new MetadataReaderWriter(databaseName);
            metadataReaderWriter.write(
                    PFSFileCount + 1,
                    metadataReaderWriter.getKVTableCount()
            );
        }
        FSMReaderWriter fsmReaderWriter = new FSMReaderWriter(databaseName);
        fsmReaderWriter.initialize(PFSFileCount);

    }

    // Prints the contents of a test file

    // Writes a string to a specific block in a PFS file
    public void write(int PFSFileNum, int offset, int blockNum, String content) throws IOException, IllegalArgumentException {
        String pathname = DATABASE_DIRECTORY + "/" + databaseName + ".db" + PFSFileNum;

        if (offset + content.length() > BLOCK_SIZE) {
            throw new IllegalArgumentException("Content exceeds block size.");
        }

        try (RandomAccessFile file = new RandomAccessFile(pathname, "rw")) {
//            System.out.println("offest: " + offset + " blockNum: " + blockNum + " content: " + content + " PFsFileNum: " + PFSFileNum);
            file.seek(offset + (long) blockNum * BLOCK_SIZE);
            file.writeBytes(content);
        } catch (IOException e) {
            Logger.getLogger(DBRepository.class.getName()).severe(e.getMessage());
        }
    }

    // Reads a specific block from a PFS file
    public String readBlock(int PFSFileCount, int blockNum) throws IOException {
        return read(PFSFileCount, 0, blockNum, BLOCK_SIZE);
    }

    // Reads a single character from a specific location in a PFS file
    public String readChar(int PFSFileCount, int offset, int blockNum) throws IOException {
        return read(PFSFileCount, offset, blockNum, 1);
    }

    public String read(int PFSFileCount, int offset, int blockNum, int length) throws IOException {
        String pathname = DATABASE_DIRECTORY + "/" + databaseName + ".db" + PFSFileCount;
        StringBuilder content = new StringBuilder();
        try (RandomAccessFile file = new RandomAccessFile(pathname, "r")) {
            file.seek(offset + (long) blockNum * BLOCK_SIZE);
            byte[] buffer = new byte[length];
            int bytesRead = file.read(buffer, 0, length);
            content.append(new String(buffer, 0, bytesRead, UTF_8));
        } catch (IOException e) {
            Logger.getLogger(DBRepository.class.getName()).severe(e.getMessage());
        }
        return content.toString();
    }

    // Deletes a PFS file
    public void delete(int PFSFileCount) {
        String pathname = DATABASE_DIRECTORY + "/" + databaseName + ".db" + PFSFileCount;
        File file = new File(pathname);
        if (file.delete()) {
            System.out.println("File deleted: " + file.getName());
        } else {
            System.out.println("Failed to delete the file.");
        }
    }

}
