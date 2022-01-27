import java.util.Scanner
import java.sql.DriverManager
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement
import java.io.PrintWriter
import java.io.File

object Project1 {

    private var scanner = new Scanner(System.in)
    private var statement: Statement = null
    private val log = new PrintWriter(new File("query.log"))
    private var resultSet: ResultSet = null
    private var usersTable: ResultSet = null
    private var individualRecord: ResultSet = null
    private var loggedInUser: ResultSet = null
    private var loggedInUserID: Int = 0
    private var loggedInUsername: String = "null"
    private var loggedInPassword: String = "null"
    private var loggedInAccountType: String = "null"

    def main(args: Array[String]): Unit = {

        val driver = "com.mysql.jdbc.Driver"
        val url = "jdbc:mysql://localhost:3306/project1"
        val username = "root"
        val password = "y6F*_03jgN2"

        scanner.useDelimiter(System.lineSeparator())

        Class.forName(driver)
        val connection = DriverManager.getConnection(url, username, password)
        statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);

        var continue = true

        while(continue) {
            var nextStep = getInput()

            if (nextStep == "1") {
                createAccount()
            }

            if (nextStep == "2") {
                login()
                var accountType = loggedInUser.getString("user_type")
                if (accountType == "user") userMenu()
                else adminMenu()
            }

            if (nextStep == "3") {
                continue = false
            }
        }

        connection.close()
        log.close()
        println("You have exited the program. Bye!")

    }

    def getInput(): String = {
        println("")
        println("Welcome!")
        println("Please select from the options below. Simply type 1, 2, or 3, then hit \"enter\".")
        println("")
        println("1: Create account")
        println("2: Log in")
        println("3: Exit")
        println("")

        return scanner.next().toString()
    }

    def createAccount(): Unit = {
        var resultSet = statement.executeQuery("SELECT username FROM users;")
        log.write("Executing 'SELECT username FROM users;'")
        println("")
        println("Alright! Let's get you set up.")
        println("")
        println("First, please enter a username.")
        println("")

        var newUsername = ""
        var newPassword = ""
        var accountType = ""

        var continueUsername = true
        while (continueUsername) {
            continueUsername = false
            newUsername = scanner.next().toString()
            resultSet.beforeFirst()
            while (resultSet.next()) {
                var resultSetUsername = resultSet.getString("username")
                if (newUsername == resultSetUsername) {
                    println("")
                    println("That username already exists. Please enter a different username.")
                    println("")
                    continueUsername = true
                }
            }
        }
        
        println("")
        println("Great! Now, please create a password.")
        println("")

        var continuePassword = true
        while (continuePassword) {
            continuePassword = false
            newPassword = scanner.next().toString()
            if (newPassword.length < 5) {
                println("")
                println("A password must be at least 5 characters long. Please enter something else.")
                println("")
                continuePassword = true
            }
        }

        println("")
        println("Awesome! Okay, now please select whether this is a USER account or an ADMIN account.")
        println("1: USER")
        println("2: ADMIN")
        println("")
        
        var continueAccountType = true
        while (continueAccountType) {
            continueAccountType = false
            var choice = scanner.next().toString()
            if (choice == "1") {
                accountType = "user"
            }
            else if (choice == "2") {
                accountType = "admin"
            }
            else {
                println("")
                println("I'm sorry, that's not one of the options. Please enter a 1 or a 2.")
                println("")
                continueAccountType = true
            }
        }
        
        statement.executeUpdate(s"INSERT INTO users (username, password, user_type) VALUES (\"$newUsername\", \"$newPassword\", \"$accountType\");")
        log.write("Executing 'INSERT INTO users (username, password, user_type) VALUES (\"$newUsername\", \"$newPassword\", \"$accountType\");'")

        println("")
        print("Nice! You're all set up!")
        println("")
    }

    def getLoggedInUser(username: String): Unit = {
        log.write("Executing 'SELECT * FROM users WHERE username = \"$username\";'")
        loggedInUser = statement.executeQuery(s"SELECT * FROM users WHERE username = \"$username\";")
        while (loggedInUser.next()) {
            loggedInUserID = loggedInUser.getInt("user_id")
            loggedInUsername = loggedInUser.getString("username")
            loggedInPassword = loggedInUser.getString("password")
            loggedInAccountType = loggedInUser.getString("user_type")
        }
        loggedInUser.beforeFirst()
    }

    def login(): Unit = {    
        var loginAttempt = true
        while (loginAttempt) {
            loginAttempt = false
            println("")
            print("Username: ")
            var loginUsername = scanner.next().toString()
            getLoggedInUser(loginUsername)

            if (!loggedInUser.next()) {
                loginAttempt = true
                println("")
                println("Sorry, that's not a valid username.")
            }

            else {
                var invalidPassword = true
                while (invalidPassword) {
                    invalidPassword = false
                    print("Password: ")
                    var loginPassword = scanner.next().toString()

                    if (loginPassword != loggedInPassword) {
                        invalidPassword = true
                        println("")
                        println("Sorry, that's not the correct password. Please try entering it again.")
                        println("")
                    }
                }
            }
        }
    }

    def userMenu(): Unit = {
        var continueUserMenu = true
        while (continueUserMenu) {
            println("")
            println("Good to see you! This is a COVID-19 data tracker. You can use it to get some basic information regarding the COVID-19 pandemic around the world.")
            println("")
            println("Please choose one of the options below.")
            println("1: Countries with the top 10 highest number of COVID cases.")
            println("2: Countries with the bottom 10 highest number of COVID cases.")
            println("3: Log out")
            println("")
            var userChoice = scanner.next().toString()

            if (userChoice == "1") {
                println("")
                println("Option 1 Test Successful")
            }
            else if (userChoice == "2") {
                println("")
                println("Option 2 Test Successful")
            }
            else if (userChoice == "3") {
                continueUserMenu = false
                println("")
                println("Thanks for stopping by!")
            }
            else {
                println("")
                println("I'm sorry, that's not one of the available options. Please try again.")
            }
        }
    }

    def adminMenu(): Unit = {
        var continueAdminMenu = true
        while (continueAdminMenu) {
            println("")
            println("Good to see you! This is a COVID-19 data tracker. You can use it to get some basic information regarding the COVID-19 pandemic around the world.")
            println("")
            println("Please choose one of the options below.")
            println("1: View users table")
            println("2: Get COVID-19 data")
            println("3: Log out")
            println("")
            var adminChoice = scanner.next().toString()

            if (adminChoice == "1") {
                viewUsersTable()
            }
            else if (adminChoice == "2") {
                // covidDataMenu()
            }
            else if (adminChoice == "3") {
                continueAdminMenu = false
                println("")
                println("Thanks for stopping by!")
            }
            else {
                println("")
                println("I'm sorry, that's not one of the available options. Please try again.")
            }
        }
    }

    def getUsersTable(): Unit = {
        log.write("Executing 'SELECT * FROM users;'")
        usersTable = statement.executeQuery(s"SELECT * FROM users")
    }

    def getIndividualRecord(id: Int): Unit = {
        log.write("Executing 'SELECT * FROM users;'")
        individualRecord = statement.executeQuery(s"SELECT * FROM users WHERE id = $id;")
    }

    def viewUsersTable(): Unit = {
        var continueUsersTable = true
        while (continueUsersTable) {
            println("")
            getUsersTable()
            while (usersTable.next()) {
                var id = usersTable.getInt("user_id")
                var username = usersTable.getString("username")
                var password = usersTable.getString("password")
                var user_type = usersTable.getString("user_type")
                println(s"ID: $id ::: Username: $username ::: Password: $password ::: Account Type: $user_type")
            }

            println("")
            println("Please choose one of the options below.")
            println("1: Edit user information")
            println("2: Delete user")
            println("3: Back")
            println("")
            var choice = scanner.next().toString()

            if (choice == 1) {
                // editUser()
            }

            else if (choice == "2") {
                // deleteUser()
            }

            else if (choice == "3") {
                if (loggedInAccountType == "user") userMenu()
                else adminMenu()
                // adminMenu() || userMenu()
                // How to distinguish which one?
                // Instance variable for loggedInUser?
                // getLoggedInUser() method?
            }
        }
    }

    def editUser(): Unit = {
        var continueEditUser = true
        while (continueEditUser) {
            println("")
            println("Please select a user to edit by entering their ID number.")
            var whichUser = scanner.next().toString()
            getIndividualRecord(whichUser.toInt)

            if (!individualRecord.next()) {
                println("")
                println("That user does not exist. Please try again.")
            }

            else {
                var continueWhichInfo = true
                while (continueWhichInfo) {
                    println("")
                    println("What information do you want to edit?")
                    println("1: Username")
                    println("2: Password")
                    println("3: Account Type")
                    println("4: Back")
                    println("")
                    var whichInfo = scanner.next().toString()

                    if (whichInfo == "1") {
                        println("")
                        println("Okay. Please enter a new username.")
                        println("")
                        var newUsername = scanner.next().toString()
                        resultSet.updateString("username", newUsername)
                        println("Great! The new username has been saved.")
                    }

                    else if (whichInfo == "2") {
                        println("")
                        println("Okay. Please enter a new password.")
                        println("")
                        var newPassword = scanner.next().toString()
                        resultSet.updateString("password", newPassword)
                        println("Great! The new password has been saved.")
                    }

                    else if (whichInfo == "3") {
                        var continueWhichType = true
                        while (continueWhichType) {
                            println("")
                            println("Please choose which account type this user should be.")
                            println("1: USER")
                            println("2: ADMIN")
                            println("")
                            var newType = scanner.next().toString()
                            if (newType == "1" || newType == "2") {
                                var userType = if (newType == "1") "user" else "admin"
                                continueWhichInfo = false
                                resultSet.updateString("user_type", userType)
                                println("Great! This user's account type has been changed.")
                            }
                            else {
                                println("")
                                println("I'm sorry, that's not one of the available options.")
                            }
                        }
                    }

                    else if (whichInfo == "4") {
                        viewUsersTable()
                    }

                    else {
                        println("")
                        println("I'm sorry, that's not one of the available options.")
                    }
                }
            }
        }
    }

    def getCovidData(): Unit = {
        var continueCovidMenu = true
        while (continueCovidMenu) {
            println("")
            println("Please choose one of the options below.")
            println("1: Countries with the top 10 highest number of COVID cases.")
            println("2: Countries with the bottom 10 highest number of COVID cases.")
            println("3: Countries with the top 10 mortality rates.")
            println("4: Countries with the bottom 10 mortality rates.")
            println("5: Countries with the top 10 recovery rates.")
            println("6: Countries with the bottom 10 recovery rates.")
            println("7: Log out")
            println("")
            var userChoice = scanner.next().toString()
        }
    }
}