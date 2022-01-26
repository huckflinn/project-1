import java.util.Scanner
import java.sql.DriverManager
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement

object Project1 {

    private val scanner = new Scanner(System.in)
    private var statement: Statement = null

    def main(args: Array[String]): Unit = {

        val driver = "com.mysql.jdbc.Driver"
        val url = "jdbc:mysql://localhost:3306/project1"
        val username = "root"
        val password = "y6F*_03jgN2"

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
            }

            if (nextStep == "3") {
                continue = false
            }
        }

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

        // scanner.useDelimiter(System.lineSeparator())
                // var choice = scanner.next().toString().toUpperCase()
        return scanner.next().toString()
    }

    def createAccount(): Unit = {
        var resultSet = statement.executeQuery("SELECT username FROM users;")
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

        println("")
        print("Nice! You're all set up!")
        println("")
    }

    def login(): Unit = {    
        var loginAttempt = true
        while (loginAttempt) {
            loginAttempt = false
            println("")
            print("Username: ")
            var loginUsername = scanner.next().toString()
            var resultSet = statement.executeQuery(s"SELECT username, password, user_type FROM users WHERE username = \"$loginUsername\";")
            var invalidUsername = true
            if (!resultSet.next()) {
                loginAttempt = true
                println("")
                println("Sorry, that's not a valid username.")
            }
            else {
                var correctUsername = resultSet.getString("username")
                var correctPassword = resultSet.getString("password")
                var invalidPassword = true
                while (invalidPassword) {
                    invalidPassword = false
                    print("Password: ")
                    var loginPassword = scanner.next().toString()
                    // resultSet.next()
                    // var correctPassword = resultSet.getString("password")
                    if (loginPassword != correctPassword) {
                        invalidPassword = true
                        println("")
                        println("Sorry, that's not the correct password. Please try entering it again.")
                        println("")
                    }
                }
            }
        }

        println("")
        println("login() Test Successful")

        // userMenu()
    }

    // def userMenu(): Unit = {
    //     println("Good to see you! This is a COVID-19 data tracker. You can use it to get some basic information regarding the COVID-19 pandemic around the world.")
    //     println("Please choose one of the options below.")
    //     println("1: Countries with the top 10 highest number of COVID cases.")
    //     println("2: Countries with the bottom 10 highest number of COVID cases.")
    // }

    def adminLogin(): Unit = {
        /*
        var notValidUsername = true
        while (notValidUsername) {
            notValidUsername = false
            print("Username: ")
            var loginUsername = scanner.nextLine()
            /*
            if loginUsername IS NOT VALID {
                notValidUsername = true
                println("Sorry, that's not a valid username.")
            }
        */
        }

        var notValidPassword = true
        while (notValidPassword) {
            notValidPassword = false
            print("Password: ")
            var loginPassword = scanner.nextLine()
            /*
            if loginPassword IS NOT VALID {
                notValidPassword = true
                println("Sorry, that's not the correct password.")
            }
            */
        }
        */

        println("adminLogin() Test Successful")

        // adminMenu()
    }
}