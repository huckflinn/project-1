import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import java.util.Scanner
import java.sql.DriverManager
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement
import java.io.PrintWriter
import java.io.File

object Project1 {

    System.setSecurityManager(null)
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\") // change if winutils.exe is in a different bin folder
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
    private val conf = new SparkConf().setMaster("local").setAppName("Project1")
    private val sc = new SparkContext(conf)
    private val hiveCtx = new HiveContext(sc)

    def main(args: Array[String]): Unit = {

        // System.setSecurityManager(null)
        // System.setProperty("hadoop.home.dir", "C:\\hadoop\\") // change if winutils.exe is in a different bin folder
        // val conf = new SparkConf()
        //     .setMaster("local") 
        //     .setAppName("Project1")
        // val sc = new SparkContext(conf)
        sc.setLogLevel("ERROR")
        // hiveCtx = new HiveContext(sc)
        import hiveCtx.implicits._

        val driver = "com.mysql.jdbc.Driver"
        val url = "jdbc:mysql://localhost:3306/project1"
        val username = "root"
        val password = "y6F*_03jgN2"

        scanner.useDelimiter(System.lineSeparator())

        Class.forName(driver)
        val connection = DriverManager.getConnection(url, username, password)
        statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);

        // Run method to insert Covid data. Only needs to be ran initially, then table data1 will be persisted.
        // insertCovidData(hiveCtx)

        mainMenu()

        sc.stop()
        connection.close()
        log.close()
        println("You have exited the program. Bye!")

    }

    def mainMenu(): Unit = {
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
            println("1: Get COVID-19 data")
            println("2: Log out")
            println("")
            var userChoice = scanner.next().toString()

            if (userChoice == "1") {
                covidDataMenu()
            }
            else if (userChoice == "2") {
                continueUserMenu = false
                println("")
                println("Thanks for stopping by!")
                // mainMenu()
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
                covidDataMenu()
            }
            else if (adminChoice == "3") {
                continueAdminMenu = false
                println("")
                println("Thanks for stopping by!")
                // mainMenu()
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

            if (choice == "1") {
                continueUsersTable = false
                editUser()
            }

            else if (choice == "2") {
                deleteUser()
            }

            else if (choice == "3") {
                continueUsersTable = false
                if (loggedInAccountType == "user") userMenu()
                else adminMenu()
            }
        }
    }

    def getIndividualRecord(id: Int): Unit = {
        log.write("Executing 'SELECT * FROM users WHERE user_id = $id;'")
        individualRecord = statement.executeQuery(s"SELECT * FROM users WHERE user_id = $id;")
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
                        individualRecord.updateString("username", newUsername)
                        individualRecord.updateRow()
                        println("Great! The new username has been saved.")
                    }

                    else if (whichInfo == "2") {
                        println("")
                        println("Okay. Please enter a new password.")
                        println("")
                        var newPassword = scanner.next().toString()
                        individualRecord.updateString("password", newPassword)
                        individualRecord.updateRow()
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
                                continueWhichType = false
                                individualRecord.updateString("user_type", userType)
                                individualRecord.updateRow()
                                println("Great! This user's account type has been changed.")
                            }
                            else {
                                println("")
                                println("I'm sorry, that's not one of the available options.")
                            }
                        }
                    }

                    else if (whichInfo == "4") {
                        continueEditUser = false
                        continueWhichInfo = false
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

    def deleteUser(): Unit = {
        var continueDeleteUser = true
        while (continueDeleteUser) {
            println("")
            println("Please select a user to delete by entering their ID number, or type \"back\".")
            var whichUser = scanner.next().toString()
            if (whichUser == "back") {
                continueDeleteUser = false
            }
            else {
                getIndividualRecord(whichUser.toInt)

                if (!individualRecord.next()) {
                    println("")
                    println("That user does not exist. Please try again.")
                }

                else {
                    individualRecord.deleteRow()
                    continueDeleteUser = false
                    println("")
                    println("Okay, this user has been deleted.")
                }
            }
        }
    }

    def covidDataMenu(): Unit = {
        insertCovidData(hiveCtx)
        var continueCovidMenu = true
        while (continueCovidMenu) {
            println("")
            println("Please choose one of the options below.")
            println("1: US states with highest total vaccinations.")
            println("2: US states with lowest total vaccinations.")
            println("3: US states with the highest vaccination rates.")
            println("4: US states with the highest vaccination rates.")
            println("5: Fastest US states to reach 1 million people fully vaccinated.")
            println("6: Slowest US states to reach 1 million people fully vaccinated.")
            println("7: Back")
            println("")
            var userChoice = scanner.next().toString()

            if (userChoice == "1") {
                 highestTotalVaccinations()
            }
            else if (userChoice == "2") {
                lowestTotalVaccinations()
            }
            else if (userChoice == "3") {
                highestVaxRates()
            }
            else if (userChoice == "4") {
                lowestVaxRates()
            }
            else if (userChoice == "5") {
                fastestToMillion()
            }
            else if (userChoice == "6") {
                slowestToMillion()
            }
            else if (userChoice == "7") {
                continueCovidMenu = false

            }
            else {
                println("")
                println("I'm sorry, that's not one of the available options.")
            }
        }
    }

    def insertCovidData(hiveCtx: HiveContext): Unit = {
        val output = hiveCtx.read
        .format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .load("input/us_state_vaccinations.csv")
        output.limit(15).show() // Prints out the first 15 lines of the dataframe

        output.createOrReplaceTempView("temp_data")
        hiveCtx.sql("CREATE TABLE IF NOT EXISTS covid_vax_data (date STRING, location STRING, total_vaccinations DOUBLE, total_distributed DOUBLE, people_vaccinated DOUBLE, people_fully_vaccinated_per_hundred FLOAT, total_vaccinations_per_hundred FLOAT, people_fully_vaccinated DOUBLE, people_vaccinated_per_hundred FLOAT, distributed_per_hundred FLOAT, daily_vaccinations_raw DOUBLE, daily_vaccinations DOUBLE, daily_vaccinations_per_million DOUBLE, share_doses_used FLOAT, total_boosters INT, total_boosters_per_hundred FLOAT) row format delimited fields terminated by ',' stored as textfile")
        hiveCtx.sql("ALTER TABLE covid_vax_data SET TBLPROPERTIES (\"skip.header.line.count\"=\"1\")")
        hiveCtx.sql("INSERT INTO covid_vax_data SELECT * FROM temp_data")
        hiveCtx.sql("CREATE TABLE IF NOT EXISTS covid_vax_data_partitioned (date STRING, total_vaccinations DOUBLE, total_distributed DOUBLE, people_vaccinated DOUBLE, people_fully_vaccinated_per_hundred FLOAT, total_vaccinations_per_hundred FLOAT, people_fully_vaccinated DOUBLE, people_vaccinated_per_hundred FLOAT, distributed_per_hundred FLOAT, daily_vaccinations_raw DOUBLE, daily_vaccinations DOUBLE, daily_vaccinations_per_million DOUBLE, share_doses_used FLOAT, total_boosters INT, total_boosters_per_hundred FLOAT) PARTITIONED BY (location STRING) CLUSTERED BY (date) INTO 10 BUCKETS row format delimited fields terminated by ',' stored as textfile")
        hiveCtx.sql("INSERT INTO covid_vax_data_partitioned SELECT date, total_vaccinations, total_distributed, people_vaccinated, people_fully_vaccinated_per_hundred, total_vaccinations_per_hundred, people_fully_vaccinated, people_vaccinated_per_hundred, distributed_per_hundred, daily_vaccinations_raw, daily_vaccinations, daily_vaccinations_per_million, share_doses_used, total_boosters, total_boosters_per_hundred, location FROM covid_vax_data")

        val summary = hiveCtx.sql("SELECT * FROM covid_vax_data_partitioned LIMIT 10")
        summary.show()
    }

    def highestTotalVaccinations(): Unit = {
        val result = hiveCtx.sql("SELECT location, total_vaccinations FROM covid_vax_data_partitioned WHERE date = '2022-01-26' ORDER BY total_vaccinations DESC LIMIT 10;")
        result.show()
        result.write.csv("results/highestTotalVaccinations")
    }

    def lowestTotalVaccinations(): Unit = {
        val result = hiveCtx.sql("SELECT location, total_vaccinations FROM covid_vax_data_partitioned WHERE date = '2022-01-26' ORDER BY total_vaccinations ASC LIMIT 10;")
        result.show()
        result.write.csv("results/lowestTotalVaccinations")
    }

    def highestVaxRates(): Unit = {
        val result = hiveCtx.sql("SELECT location, people_fully_vaccinated_per_hundred FROM covid_vax_data_partitioned WHERE date = '2022-01-26' ORDER BY people_fully_vaccinated_per_hundred DESC LIMIT 10;")
        result.show()
        result.write.csv("results/highestVaxRates")
    }

    def lowestVaxRates(): Unit = {
        val result = hiveCtx.sql("SELECT location, people_fully_vaccinated_per_hundred FROM covid_vax_data_partitioned WHERE date = '2022-01-26' ORDER BY people_fully_vaccinated_per_hundred ASC LIMIT 10;")
        result.show()
        result.write.csv("results/lowestVaxRates")
    }

    def fastestToMillion(): Unit = {
        val result = hiveCtx.sql("SELECT date, location FROM table WHERE people_fully_vaccinated > 1000000 ORDER BY date ASC LIMIT 1;")
        result.show()
        result.write.csv("results/fastestToMillion")
    }

    def slowestToMillion(): Unit = {
        val result = hiveCtx.sql("SELECT date, location FROM table WHERE people_fully_vaccinated > 1000000 ORDER BY date DESC LIMIT 1;")
        result.show()
        result.write.csv("results/slowestToMillion")
    }
}