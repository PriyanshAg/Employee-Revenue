package org.EmployeeRevenueReport;

import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;

public class EmployeeRevenueReport {
    private static SparkSession spark = SparkSession.builder()
            .appName("Optimized Employee Revenue Report")
            .master("local") // Adjust for your cluster setup
            .getOrCreate();

    public static Dataset<Row> loadData(String filePath) {
        return spark.read()
                .option("header", "true")
                .csv(filePath);
    }

    public static void process(Dataset<Row> employees, Dataset<Row> transactions, Dataset<Row> departments) {
        // Filter employees and transactions based on selected types
        Dataset<Row> filteredEmployees = employees.filter(col("employeeType").equalTo("Sales"));
        Dataset<Row> filteredTransactions = transactions.filter(col("transactionType").equalTo("Sale"));

        // Select only necessary columns early to reduce the data size
        filteredEmployees = filteredEmployees.select("employeeId","employeeName", "departmentId");
        filteredTransactions = filteredTransactions.select("employeeId", "transactionAmount");

        // Repartition DataFrames for optimal performance
        filteredEmployees = filteredEmployees.repartition(10, col("departmentId"));
        filteredTransactions = filteredTransactions.repartition(10, col("employeeId"));


        departments.cache();

        // Join transactions with employees, and aggregate total revenue per employee per department
        Dataset<Row> employeeRevenue = filteredEmployees
                .join(filteredTransactions, "employeeId")
                .groupBy("employeeId","employeeName", "departmentId")
                .agg(sum("transactionAmount").as("totalRevenue"));


        // Join with departments DataFrame
        employeeRevenue = employeeRevenue
                .join(broadcast(departments), "departmentId", "left_outer")
                .select("employeeId", "employeeName", "departmentId", "totalRevenue");

        // Department-level revenue calculation
        Dataset<Row> departmentRevenue = employeeRevenue
                .groupBy("departmentId")
                .agg(avg("totalRevenue").as("avgRevenue"));

        // Join the employeeRevenue with departmentRevenue
        departmentRevenue = departmentRevenue
                .join(employeeRevenue, "departmentId", "left_outer")
                .select("departmentId", "employeeId", "employeeName", "totalRevenue", "avgRevenue");

        // If there are departments without employees (no transactions), fill those with 0 revenue
        departmentRevenue = departmentRevenue
                .join(departments, "departmentId", "left_outer")
                .na().fill(0, new String[]{"avgRevenue"});

        // Show the final report
        departmentRevenue.show();
    }

    public static void main(String[] args) {
        // Load the data
        Dataset<Row> employees = loadData("src/main/resources/exployees.csv");
        Dataset<Row> transactions = loadData("src/main/resources/transactions.csv");
        Dataset<Row> departments = loadData("src/main/resources/departments.csv");

        // Process the report
        process(employees, transactions, departments);
    }
}
