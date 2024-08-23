package Database;

import java.io.File;
import java.util.Scanner;

import static Database.DBUtil.Constants.DATABASE_DIRECTORY;

public class DBController {

    private DBService dbService;

    private Scanner scanner;

    private String userInput;

    // Manages command-line interaction for the database
    public DBController() {
        this.scanner = new Scanner(System.in);
    }

    // Starts the command-line interface
    public void startCLI() {
        String command;
        do {
            System.out.print("NoSQL>");
            command = scanner.nextLine().trim(); // Reads and trims user input
            processCommand(command); // Processes the command
        } while (!command.equalsIgnoreCase("quit")); // Continues until 'quit' is entered
    }

    // Processes a command entered by the user
    private void processCommand(String command) {
        String[] parts = command.split("\\s+", 2); // Splits the command into parts
        if (parts.length == 0) {
            return; // !!! Should never happen due to the nature of split, might be redundant
        }
        String action = parts[0].toLowerCase(); // Gets the command action
        String argument = parts.length > 1 ? parts[1] : "";

        // Handles different commands
        if (action.equals("open")) {
            dbService = new DBService(argument);  // Initializes DBService with the database name
            //System.out.println("Database " + argument + " opened.");
            File databaseFile = new File(DATABASE_DIRECTORY + "/" + argument + ".db0");
            if (!databaseFile.exists()) {
                dbService.create();
                System.out.println("Database " + argument + " created.");
            } else {
                System.out.println("Database " + argument + " opened.");
            }
        } else if (dbService == null) {
            System.out.println("No database is currently open. Please open a database first.");
        } else {
            switch (action) {
                case "put":
                    dbService.put(argument);
                    break;
                case "get":
                    dbService.get(System.getProperty("user.dir"), argument);
                    break;
                case "rm":
                    dbService.rm(argument);
                    break;
                case "dir":
                    dbService.dir();
                    break;
                case "find":
                    int lastDotIndex = argument.lastIndexOf(".");
                    String tableName = argument.substring(0, lastDotIndex);
                    int key = Integer.parseInt(argument.substring(lastDotIndex + 1));
                    dbService.find(tableName, key);
                    break;
                case "kill":
                    dbService.kill(argument);
                    break;
                case "quit":
                    dbService.quit();
                    break;
                default:
                    System.out.println("Invalid command: " + parts[0]);
            }
        }


    }

    // Getters and setters
    public DBService getDbService() {
        return dbService;
    }

    public void setDbService(DBService dbService) {
        this.dbService = dbService;
    }

    public Scanner getScanner() {
        return scanner;
    }

    public void setScanner(Scanner scanner) {
        this.scanner = scanner;
    }

    public String getUserInput() {
        return userInput;
    }

    public void setUserInput(String userInput) {
        this.userInput = userInput;
    }
}