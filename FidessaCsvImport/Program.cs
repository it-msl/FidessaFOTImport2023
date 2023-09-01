using System;
using System.IO;
using LumenWorks.Framework.IO.Csv;
using System.Data.SqlClient;
using System.Data;
using System.Globalization;
using System.Threading;
using System.Text.RegularExpressions;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Runtime.Remoting.Messaging;

namespace FidessaCsvImport
{
    internal static class Program
    {
        private static void Main()
        {
            // ReSharper disable once UnusedVariable
            if (!DateTime.TryParse(ConfigurationManager.AppSettings["startDate"],out var dummyDate))
            {
                throw new Exception("startDate not specified");
            }
            if (!Directory.Exists(ConfigurationManager.AppSettings["csvDirectory"]))
            {
                throw new Exception($"csvDirectory does not exist: {ConfigurationManager.AppSettings["csvDirectory"]}");
            }
            if (!Directory.Exists(ConfigurationManager.AppSettings["importedDirectory"]))
            {
                throw new Exception($"importedDirectory does not exist: {ConfigurationManager.AppSettings["importedDirectory"]}");
            }

            Console.WriteLine("Importing OrderProgress");
            ImportOrderProgressCsv();

            Console.WriteLine("Importing Client");
            ImportClientCsv();

            Console.WriteLine("Done");
            Thread.Sleep(3000);
        }

        private static void ImportOrderProgressCsv()
        {
            var csvFiles = new List<KeyValuePair<string, DateTime>>();
            var unfilteredCsvFiles = Directory.GetFiles(ConfigurationManager.AppSettings["csvDirectory"], "ORDER_PROGRESS*.csv", SearchOption.TopDirectoryOnly);

            foreach (var csvFile in unfilteredCsvFiles)
            {
                if (csvFile.Contains("ORDER_PROGRESS_EOD"))
                {
                    continue;
                }

                var fileDate = ParseExactCsvDate(Regex.Match(csvFile, @"\d{8}").Value);

                //import if since startDate and not already imported
                if (fileDate != null && fileDate >= DateTime.Parse(ConfigurationManager.AppSettings["startDate"]) && !File.Exists(csvFile.Replace(ConfigurationManager.AppSettings["csvDirectory"], ConfigurationManager.AppSettings["importedDirectory"])))
                {
                    csvFiles.Add(new KeyValuePair<string, DateTime>(csvFile, (DateTime)fileDate));
                }
            }

            Console.WriteLine($"{csvFiles.Count} files found to import");

            //sort files by date (in filename)
            var orderedCsvFiles = csvFiles.OrderBy(x => x.Value).ToList();

            //import each file
            for (var i = 0; i < orderedCsvFiles.Count; i++)
            {
                var path = csvFiles[i].Key;
                Console.WriteLine($"Extracting csv file {i + 1} of {csvFiles.Count}, {csvFiles[i].Key}");

                var dataTable = new DataTable();
                dataTable.Columns.Add(new DataColumn("id", typeof(int)));
                dataTable.Columns.Add(new DataColumn("OrderState", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Routed_Order_Code", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Executing_Entity", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Executing_Book_Id", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Contracting_Entity", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Contracting_Book_Id", typeof(string)));
                dataTable.Columns.Add(new DataColumn("House_Book_Id", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Instrument_Code", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Instrument_Description", typeof(string)));
                dataTable.Columns.Add(new DataColumn("ISIN_Code", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Sedol_Code", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Epic_Code", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Market_Id", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Counterparty_Code", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Counterparty_Description", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Top_Level", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Parent_Order_Id", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Order_Id", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Version", typeof(int)));
                dataTable.Columns.Add(new DataColumn("Buy_Sell", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Total_Quantity", typeof(double)));
                dataTable.Columns.Add(new DataColumn("Quantity_Filled", typeof(double)));
                dataTable.Columns.Add(new DataColumn("Limit_Price", typeof(double)));
                dataTable.Columns.Add(new DataColumn("Gross_Fill_Price", typeof(double)));
                dataTable.Columns.Add(new DataColumn("Dealt_Ccy", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Settlement_Ccy", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Dealt_To_Settlement_Rate", typeof(double)));
                dataTable.Columns.Add(new DataColumn("Amended_Date", typeof(DateTime)));
                dataTable.Columns.Add(new DataColumn("Amended_Time", typeof(TimeSpan)));
                dataTable.Columns.Add(new DataColumn("Expiry_Date", typeof(DateTime)));
                dataTable.Columns.Add(new DataColumn("Expiry_Time", typeof(TimeSpan)));
                dataTable.Columns.Add(new DataColumn("Entered_By", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Representative_Id", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Num_Fills", typeof(int)));
                dataTable.Columns.Add(new DataColumn("Entered_Date", typeof(DateTime)));
                dataTable.Columns.Add(new DataColumn("Entered_Time", typeof(TimeSpan)));
                dataTable.Columns.Add(new DataColumn("Settlement_Date", typeof(DateTime)));
                dataTable.Columns.Add(new DataColumn("Commission_Type", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Commission_Value", typeof(double)));
                dataTable.Columns.Add(new DataColumn("Dealing_Capacity", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Executor", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Received_Date", typeof(DateTime)));
                dataTable.Columns.Add(new DataColumn("Received_Time", typeof(TimeSpan)));
                dataTable.Columns.Add(new DataColumn("Client_Classification", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Counterparty_Contact", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Default_Client_Classification", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Buy_Sell_Qualifier", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Commission_Booked", typeof(double)));
                dataTable.Columns.Add(new DataColumn("Group_Id", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Account_Alternative_code", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Origin_Code", typeof(string)));
                dataTable.Columns.Add(new DataColumn("CTI_Code", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Posting_Code", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Account_Code", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Member_Clearing_Id", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Trader_Clearing_Id", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Order_Class", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Last_Executor", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Order_Notes", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Cash_Leg_ID", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Cash_Bond_ISIN_Number", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Clean_Cash_Price", typeof(double)));
                dataTable.Columns.Add(new DataColumn("Hedge_Ratio_Methodology", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Coupon_Rate", typeof(double)));
                dataTable.Columns.Add(new DataColumn("Coupon_Payment_Frequency", typeof(int)));
                dataTable.Columns.Add(new DataColumn("Bond_Currency", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Bond_Nominal_Value", typeof(double)));
                dataTable.Columns.Add(new DataColumn("Fixed_Price_Formulae", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Floating_Price_Formulae", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Swap_Start_Date", typeof(DateTime)));
                dataTable.Columns.Add(new DataColumn("Maturity_Date", typeof(DateTime)));
                dataTable.Columns.Add(new DataColumn("Swap_Quantity", typeof(double)));
                dataTable.Columns.Add(new DataColumn("Futures_Expiry_Month", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Last_Execution_Datetime", typeof(DateTime)));
                dataTable.Columns.Add(new DataColumn("Originating_Counterparty", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Global_Order_Id", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Cash_Market_Counterparty", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Reference_Id", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Security_Name", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Settlement_Institution", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Coupon_Variable_Rate_Ref", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Coupon_Variable_Rate_Offset", typeof(double)));
                dataTable.Columns.Add(new DataColumn("Liquidation_Value", typeof(double)));
                dataTable.Columns.Add(new DataColumn("Swap_End_Date", typeof(DateTime)));
                dataTable.Columns.Add(new DataColumn("Fixed_Reset_Date", typeof(DateTime)));
                dataTable.Columns.Add(new DataColumn("Floating_Reset_Date", typeof(DateTime)));
                dataTable.Columns.Add(new DataColumn("Swap_Type", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Swap_Customer_1", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Swap_Customer_2", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Physical_Settlement_Date", typeof(DateTime)));
                dataTable.Columns.Add(new DataColumn("Expiry_Type", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Gross_Fill_Price_Today", typeof(double)));
                dataTable.Columns.Add(new DataColumn("Completion_Reason", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Quantity_Available", typeof(double)));
                dataTable.Columns.Add(new DataColumn("Quantity_Filled_Today", typeof(double)));
                dataTable.Columns.Add(new DataColumn("Immediate_Parent_Order_Id", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Order_Flags", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Composite_Order_Id", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Last_Complete_Datetime", typeof(DateTime)));
                dataTable.Columns.Add(new DataColumn("Gross_Booking_Price", typeof(double)));
                dataTable.Columns.Add(new DataColumn("Order_Price_Type", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Originator_Order_Id", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Option_Version", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Contract_Size", typeof(double)));
                dataTable.Columns.Add(new DataColumn("Option_Type", typeof(string)));
                dataTable.Columns.Add(new DataColumn("CFI_Code", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Retail_Account_Number", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Order_Entry_Type", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Entered_By_Group", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Default_Commission_Rule", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Default_Commission_Method", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Default_Commission_Argument", typeof(double)));
                dataTable.Columns.Add(new DataColumn("Amended_By", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Basket_Id", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Sub_Account", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Sub_Account_Description", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Order_Origin", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Intermediary_Code", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Dealing_Capacity_Qualifier", typeof(string)));

                //Added for MIFID2 26-FEB-2018
                dataTable.Columns.Add(new DataColumn("DEA_Individual", typeof(string)));
                dataTable.Columns.Add(new DataColumn("DEA_Flag", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Execution_Decision_Value", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Execution_Decision_Short_Code", typeof(int)));
                dataTable.Columns.Add(new DataColumn("Investment_Decision_Value", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Investment_Decision_Short_Code", typeof(int)));
                dataTable.Columns.Add(new DataColumn("Liq_Prov_Only", typeof(string)));
                dataTable.Columns.Add(new DataColumn("MiFID_Client_Id", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Comm_Deriv_Risk_Reduction", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Discretionary_Order", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Posted_By", typeof(int)));
                dataTable.Columns.Add(new DataColumn("Primary_Algo", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Client_LEI", typeof(string)));

                
                using (var csv = new CsvReader(new StreamReader(path), false))
                {
                    while (csv.ReadNextRecord())
                    {
                        var dataRow = dataTable.NewRow();
                        dataRow["id"] = 0;
                        dataRow["OrderState"] = DatabaseHelper.SafeParameter(csv[0]);
                        dataRow["Routed_Order_Code"] = DatabaseHelper.SafeParameter(csv[1]);
                        dataRow["Executing_Entity"] = DatabaseHelper.SafeParameter(csv[2]);
                        dataRow["Executing_Book_Id"] = DatabaseHelper.SafeParameter(csv[3]);
                        dataRow["Contracting_Entity"] = DatabaseHelper.SafeParameter(csv[4]);
                        dataRow["Contracting_Book_Id"] = DatabaseHelper.SafeParameter(csv[5]);
                        dataRow["House_Book_Id"] = DatabaseHelper.SafeParameter(csv[6]);
                        dataRow["Instrument_Code"] = DatabaseHelper.SafeParameter(csv[7]);
                        dataRow["Instrument_Description"] = DatabaseHelper.SafeParameter(csv[8]);
                        dataRow["ISIN_Code"] = DatabaseHelper.SafeParameter(csv[9]);
                        dataRow["Sedol_Code"] = DatabaseHelper.SafeParameter(csv[10]);
                        dataRow["Epic_Code"] = DatabaseHelper.SafeParameter(csv[11]);
                        dataRow["Market_Id"] = DatabaseHelper.SafeParameter(csv[12]);
                        dataRow["Counterparty_Code"] = DatabaseHelper.SafeParameter(csv[13]);
                        dataRow["Counterparty_Description"] = DatabaseHelper.SafeParameter(csv[14]);
                        dataRow["Top_Level"] = DatabaseHelper.SafeParameter(csv[15]);
                        dataRow["Parent_Order_Id"] = DatabaseHelper.SafeParameter(csv[16]);
                        dataRow["Order_Id"] = DatabaseHelper.SafeParameter(csv[17]);
                        dataRow["Version"] = int.Parse(csv[18]);
                        dataRow["Buy_Sell"] = DatabaseHelper.SafeParameter(csv[19]);
                        dataRow["Total_Quantity"] = float.Parse(csv[20]);
                        dataRow["Quantity_Filled"] = float.Parse(csv[21]);
                        dataRow["Limit_Price"] = float.Parse(csv[22]);
                        dataRow["Gross_Fill_Price"] = float.Parse(csv[23]);
                        dataRow["Dealt_Ccy"] = DatabaseHelper.SafeParameter(csv[24]);
                        dataRow["Settlement_Ccy"] = DatabaseHelper.SafeParameter(csv[25]);
                        dataRow["Dealt_To_Settlement_Rate"] = float.Parse(csv[26]);
                        dataRow["Amended_Date"] = DatabaseHelper.SafeParameter(ParseExactCsvDate(csv[27]));
                        dataRow["Amended_Time"] = TimeSpan.Parse(csv[28]);
                        dataRow["Expiry_Date"] = DatabaseHelper.SafeParameter(ParseExactCsvDate(csv[29]));
                        dataRow["Expiry_Time"] = ParseExactTimeSpan(csv[30]);
                        dataRow["Entered_By"] = DatabaseHelper.SafeParameter(csv[31]);
                        dataRow["Representative_Id"] = DatabaseHelper.SafeParameter(csv[32]);
                        dataRow["Num_Fills"] = int.Parse(csv[33]);
                        dataRow["Entered_Date"] = DatabaseHelper.SafeParameter(ParseExactCsvDate(csv[34]));
                        dataRow["Entered_Time"] = ParseExactTimeSpan(csv[35]);
                        dataRow["Settlement_Date"] = DatabaseHelper.SafeParameter(ParseExactCsvDate(csv[36]));
                        dataRow["Commission_Type"] = DatabaseHelper.SafeParameter(csv[37]);
                        dataRow["Commission_Value"] = float.Parse(csv[38]);
                        dataRow["Dealing_Capacity"] = DatabaseHelper.SafeParameter(csv[39]);
                        dataRow["Executor"] = DatabaseHelper.SafeParameter(csv[40]);
                        dataRow["Received_Date"] = DatabaseHelper.SafeParameter(ParseExactCsvDate(csv[41]));
                        dataRow["Received_Time"] = ParseExactTimeSpan(csv[42]);
                        dataRow["Client_Classification"] = DatabaseHelper.SafeParameter(csv[43]);
                        dataRow["Counterparty_Contact"] = DatabaseHelper.SafeParameter(csv[44]);
                        dataRow["Default_Client_Classification"] = DatabaseHelper.SafeParameter(csv[45]);
                        dataRow["Buy_Sell_Qualifier"] = DatabaseHelper.SafeParameter(csv[46]);
                        dataRow["Commission_Booked"] = float.Parse(csv[47]);
                        dataRow["Group_Id"] = DatabaseHelper.SafeParameter(csv[48]);
                        dataRow["Account_Alternative_code"] = DatabaseHelper.SafeParameter(csv[49]);
                        dataRow["Origin_Code"] = DatabaseHelper.SafeParameter(csv[50]);
                        dataRow["CTI_Code"] = DatabaseHelper.SafeParameter(csv[51]);
                        dataRow["Posting_Code"] = DatabaseHelper.SafeParameter(csv[52]);
                        dataRow["Account_Code"] = DatabaseHelper.SafeParameter(csv[53]);
                        dataRow["Member_Clearing_Id"] = DatabaseHelper.SafeParameter(csv[54]);
                        dataRow["Trader_Clearing_Id"] = DatabaseHelper.SafeParameter(csv[55]);
                        dataRow["Order_Class"] = DatabaseHelper.SafeParameter(csv[56]);
                        dataRow["Last_Executor"] = DatabaseHelper.SafeParameter(csv[57]);
                        dataRow["Order_Notes"] = DatabaseHelper.SafeParameter(csv[58]);
                        dataRow["Cash_Leg_ID"] = DatabaseHelper.SafeParameter(csv[59]);
                        dataRow["Cash_Bond_ISIN_Number"] = DatabaseHelper.SafeParameter(csv[60]);
                        dataRow["Clean_Cash_Price"] = float.Parse(csv[61]);
                        dataRow["Hedge_Ratio_Methodology"] = DatabaseHelper.SafeParameter(csv[62]);
                        dataRow["Coupon_Rate"] = float.Parse(csv[63]);
                        dataRow["Coupon_Payment_Frequency"] = int.Parse(csv[64]);
                        dataRow["Bond_Currency"] = DatabaseHelper.SafeParameter(csv[65]);
                        dataRow["Bond_Nominal_Value"] = float.Parse(csv[66]);
                        dataRow["Fixed_Price_Formulae"] = DatabaseHelper.SafeParameter(csv[67]);
                        dataRow["Floating_Price_Formulae"] = DatabaseHelper.SafeParameter(csv[68]);
                        dataRow["Swap_Start_Date"] = DatabaseHelper.SafeParameter(ParseExactCsvDate(csv[69]));
                        dataRow["Maturity_Date"] = DatabaseHelper.SafeParameter(ParseExactCsvDate(csv[70]));
                        dataRow["Swap_Quantity"] = float.Parse(csv[71]);
                        dataRow["Futures_Expiry_Month"] = DatabaseHelper.SafeParameter(csv[72]);
                        dataRow["Last_Execution_Datetime"] = DatabaseHelper.SafeParameter(ParseExactCsvDateTime(csv[73]));
                        dataRow["Originating_Counterparty"] = DatabaseHelper.SafeParameter(csv[74]);
                        dataRow["Global_Order_Id"] = DatabaseHelper.SafeParameter(csv[75]);
                        dataRow["Cash_Market_Counterparty"] = DatabaseHelper.SafeParameter(csv[76]);
                        dataRow["Reference_Id"] = DatabaseHelper.SafeParameter(csv[77]);
                        dataRow["Security_Name"] = DatabaseHelper.SafeParameter(csv[78]);
                        dataRow["Settlement_Institution"] = DatabaseHelper.SafeParameter(csv[79]);
                        dataRow["Coupon_Variable_Rate_Ref"] = DatabaseHelper.SafeParameter(csv[80]);
                        dataRow["Coupon_Variable_Rate_Offset"] = float.Parse(csv[81]);
                        dataRow["Liquidation_Value"] = float.Parse(csv[82]);
                        dataRow["Swap_End_Date"] = DatabaseHelper.SafeParameter(ParseExactCsvDate(csv[83]));
                        dataRow["Fixed_Reset_Date"] = DatabaseHelper.SafeParameter(ParseExactCsvDate(csv[84]));
                        dataRow["Floating_Reset_Date"] = DatabaseHelper.SafeParameter(ParseExactCsvDate(csv[85]));
                        dataRow["Swap_Type"] = DatabaseHelper.SafeParameter(csv[86]);
                        dataRow["Swap_Customer_1"] = DatabaseHelper.SafeParameter(csv[87]);
                        dataRow["Swap_Customer_2"] = DatabaseHelper.SafeParameter(csv[88]);
                        dataRow["Physical_Settlement_Date"] = DatabaseHelper.SafeParameter(ParseExactCsvDate(csv[89]));
                        dataRow["Expiry_Type"] = DatabaseHelper.SafeParameter(csv[90]);
                        dataRow["Gross_Fill_Price_Today"] = float.Parse(csv[91]);
                        dataRow["Completion_Reason"] = DatabaseHelper.SafeParameter(csv[92]);
                        dataRow["Quantity_Available"] = float.Parse(csv[93]);
                        dataRow["Quantity_Filled_Today"] = float.Parse(csv[94]);
                        dataRow["Immediate_Parent_Order_Id"] = DatabaseHelper.SafeParameter(csv[95]);
                        dataRow["Order_Flags"] = DatabaseHelper.SafeParameter(csv[96]);
                        dataRow["Composite_Order_Id"] = DatabaseHelper.SafeParameter(csv[97]);
                        dataRow["Last_Complete_Datetime"] = DatabaseHelper.SafeParameter(ParseExactCsvDateTime(csv[98]));
                        dataRow["Gross_Booking_Price"] = float.Parse(csv[99]);
                        dataRow["Order_Price_Type"] = DatabaseHelper.SafeParameter(csv[100]);
                        dataRow["Originator_Order_Id"] = DatabaseHelper.SafeParameter(csv[101]);
                        dataRow["Option_Version"] = DatabaseHelper.SafeParameter(csv[102]);
                        dataRow["Contract_Size"] = float.Parse(csv[103]);
                        dataRow["Option_Type"] = DatabaseHelper.SafeParameter(csv[104]);
                        dataRow["CFI_Code"] = DatabaseHelper.SafeParameter(csv[105]);
                        dataRow["Retail_Account_Number"] = DatabaseHelper.SafeParameter(csv[106]);
                        dataRow["Order_Entry_Type"] = DatabaseHelper.SafeParameter(csv[107]);
                        dataRow["Entered_By_Group"] = DatabaseHelper.SafeParameter(csv[108]);
                        dataRow["Default_Commission_Rule"] = DatabaseHelper.SafeParameter(csv[109]);
                        dataRow["Default_Commission_Method"] = DatabaseHelper.SafeParameter(csv[110]);
                        dataRow["Default_Commission_Argument"] = float.Parse(csv[111]);
                        dataRow["Amended_By"] = DatabaseHelper.SafeParameter(csv[112]);
                        dataRow["Basket_Id"] = DatabaseHelper.SafeParameter(csv[113]);
                        dataRow["Sub_Account"] = DatabaseHelper.SafeParameter(csv[114]);
                        dataRow["Sub_Account_Description"] = DatabaseHelper.SafeParameter(csv[115]);
                        dataRow["Order_Origin"] = DatabaseHelper.SafeParameter(csv[116]);
                        dataRow["Intermediary_Code"] = DatabaseHelper.SafeParameter(csv[117]);
                        dataRow["Dealing_Capacity_Qualifier"] = DatabaseHelper.SafeParameter(csv[118]);

                        //Added for MIFID2 26-FEB-2018
                        dataRow["DEA_Individual"] = DatabaseHelper.SafeParameter(csv[119]);
                        dataRow["DEA_Flag"] = DatabaseHelper.SafeParameter(csv[120]);
                        dataRow["Execution_Decision_Value"] = DatabaseHelper.SafeParameter(csv[121]);
                        dataRow["Execution_Decision_Short_Code"] = int.Parse(csv[122]);
                        dataRow["Investment_Decision_Value"] = DatabaseHelper.SafeParameter(csv[123]);
                        dataRow["Investment_Decision_Short_Code"] = int.Parse(csv[124]);
                        dataRow["Liq_Prov_Only"] = DatabaseHelper.SafeParameter(csv[125]);
                        dataRow["MiFID_Client_Id"] = DatabaseHelper.SafeParameter(csv[126]);
                        dataRow["Comm_Deriv_Risk_Reduction"] = DatabaseHelper.SafeParameter(csv[127]);
                        dataRow["Discretionary_Order"] = DatabaseHelper.SafeParameter(csv[128]);
                        dataRow["Posted_By"] = int.Parse(csv[129]);
                        dataRow["Primary_Algo"] = DatabaseHelper.SafeParameter(csv[130]);
                        dataRow["Client_LEI"] = DatabaseHelper.SafeParameter(csv[131]);

                        dataTable.Rows.Add(dataRow);
                    }
                }

                if (dataTable.Rows.Count > 0)
                {
                    using (var sqlConnection = new SqlConnection(ConfigurationManager.ConnectionStrings["connectionString"].ConnectionString))
                    {
                        sqlConnection.Open();
                        using (var sqlTransaction = sqlConnection.BeginTransaction())
                        {

                            using (var sqlBulkCopy = new SqlBulkCopy(sqlConnection, SqlBulkCopyOptions.Default, sqlTransaction))
                            {
                                sqlBulkCopy.DestinationTableName = "[dbo].[OrderProgress]";
                                sqlBulkCopy.BatchSize = 500; //http://stackoverflow.com/questions/779690/what-is-the-recommended-batch-size-for-sqlbulkcopy
                                sqlBulkCopy.NotifyAfter = 500;
                                sqlBulkCopy.SqlRowsCopied += OnSqlRowsCopied;
                                sqlBulkCopy.WriteToServer(dataTable);
                            }
                            sqlTransaction.Commit();
                        }
                    }
                }

                //copy files to imported directory after successful import
                File.Copy(path, path.Replace(ConfigurationManager.AppSettings["csvDirectory"], ConfigurationManager.AppSettings["importedDirectory"]));
            }
        }

        private static void OnSqlRowsCopied(
            object sender, SqlRowsCopiedEventArgs e)
        {
            Console.WriteLine("Copied {0} so far...", e.RowsCopied);
        }

        private static void ImportClientCsv()
        {
            var csvFiles = new List<KeyValuePair<string, DateTime>>();
            var unfilteredCsvFiles = Directory.GetFiles(ConfigurationManager.AppSettings["csvDirectory"], "CLIENTS*.csv", SearchOption.TopDirectoryOnly);

            foreach (var csvFile in unfilteredCsvFiles)
            {
                var fileInfo = new FileInfo(csvFile);
                var fileDate = ParseExactCsvDate(fileInfo.Name.Substring(8, 8));

                //import if since startDate and not already imported
                if (fileDate != null && fileDate >= DateTime.Parse(ConfigurationManager.AppSettings["startDate"]) && !File.Exists(csvFile.Replace(ConfigurationManager.AppSettings["csvDirectory"], ConfigurationManager.AppSettings["importedDirectory"])))
                {
                    csvFiles.Add(new KeyValuePair<string, DateTime>(fileInfo.FullName, (DateTime)fileDate));
                }
            }

            Console.WriteLine($"{csvFiles.Count} files found to import");

            //sort files by date (in filename)
            var orderedCsvFiles = csvFiles.OrderBy(x => x.Value).ToList();

            //import each file
            for (var i = 0; i < orderedCsvFiles.Count; i++)
            {
                var path = csvFiles[i].Key;
                Console.WriteLine($"Extracting csv file {i + 1} of {csvFiles.Count}, {csvFiles[i].Key}");

                var dataTable = new DataTable();
                dataTable.Columns.Add(new DataColumn("Client_Id", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Client_Mnemonic", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Client_Description", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Client_Group", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Client_Type", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Primary_Sales_Trader", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Representative", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Has_Accounts", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Country_Of_Domicile", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Gross_Price_Basis", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Agreed_Price_Basis", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Confirmation_Method", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Confirmation_Level", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Decimal_Places", typeof(int)));
                dataTable.Columns.Add(new DataColumn("Instrument_Symbiology", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Charge_Presentation", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Allow_Warehousing", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Warehouse_Duration_Buy", typeof(int)));
                dataTable.Columns.Add(new DataColumn("Warehouse_Duration_Sell", typeof(int)));
                dataTable.Columns.Add(new DataColumn("Warehouse_Limit_Buy", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Warehouse_Limit_Sell", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Oasys", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Alert", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Fund", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Pershing", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Customer_Code_1", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Customer_Code_2", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Status_Indicator", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Back_Office_Code", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Permissioning_Group", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Representative_Id", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Aggregation_model", typeof(string)));
                dataTable.Columns.Add(new DataColumn("Country_Of_Origin", typeof(string)));
                dataTable.Columns.Add(new DataColumn("recordDeleted", typeof(bool)));

                using (var csv = new CsvReader(new StreamReader(path), false))
                {
                    while (csv.ReadNextRecord())
                    {
                        var dataRow = dataTable.NewRow();
                        dataRow["Client_Id"] = DatabaseHelper.SafeParameter(csv[0]);
                        dataRow["Client_Mnemonic"] = DatabaseHelper.SafeParameter(csv[1]);
                        dataRow["Client_Description"] = DatabaseHelper.SafeParameter(csv[2]);
                        dataRow["Client_Group"] = DatabaseHelper.SafeParameter(csv[3]);
                        dataRow["Client_Type"] = DatabaseHelper.SafeParameter(csv[4]);
                        dataRow["Primary_Sales_Trader"] = DatabaseHelper.SafeParameter(csv[5]);
                        dataRow["Representative"] = DatabaseHelper.SafeParameter(csv[6]);
                        dataRow["Has_Accounts"] = DatabaseHelper.SafeParameter(csv[7]);
                        dataRow["Country_Of_Domicile"] = DatabaseHelper.SafeParameter(csv[8]);
                        dataRow["Gross_Price_Basis"] = DatabaseHelper.SafeParameter(csv[9]);
                        dataRow["Agreed_Price_Basis"] = DatabaseHelper.SafeParameter(csv[10]);
                        dataRow["Confirmation_Method"] = DatabaseHelper.SafeParameter(csv[11]);
                        dataRow["Confirmation_Level"] = DatabaseHelper.SafeParameter(csv[12]);
                        dataRow["Decimal_Places"] = int.Parse(csv[13]);
                        dataRow["Instrument_Symbiology"] = DatabaseHelper.SafeParameter(csv[14]);
                        dataRow["Charge_Presentation"] = DatabaseHelper.SafeParameter(csv[15]);
                        dataRow["Allow_Warehousing"] = DatabaseHelper.SafeParameter(csv[16]);
                        dataRow["Warehouse_Duration_Buy"] = int.Parse(csv[17]);
                        dataRow["Warehouse_Duration_Sell"] = int.Parse(csv[18]);
                        dataRow["Warehouse_Limit_Buy"] = DatabaseHelper.SafeParameter(csv[19]);
                        dataRow["Warehouse_Limit_Sell"] = DatabaseHelper.SafeParameter(csv[20]);
                        dataRow["Oasys"] = DatabaseHelper.SafeParameter(csv[21]);
                        dataRow["Alert"] = DatabaseHelper.SafeParameter(csv[22]);
                        dataRow["Fund"] = DatabaseHelper.SafeParameter(csv[23]);
                        dataRow["Pershing"] = DatabaseHelper.SafeParameter(csv[24]);
                        dataRow["Customer_Code_1"] = DatabaseHelper.SafeParameter(csv[25]);
                        dataRow["Customer_Code_2"] = DatabaseHelper.SafeParameter(csv[26]);
                        dataRow["Status_Indicator"] = DatabaseHelper.SafeParameter(csv[27]);
                        dataRow["Back_Office_Code"] = DatabaseHelper.SafeParameter(csv[28]);
                        dataRow["Permissioning_Group"] = DatabaseHelper.SafeParameter(csv[29]);
                        dataRow["Representative_Id"] = DatabaseHelper.SafeParameter(csv[30]);
                        dataRow["Aggregation_model"] = DatabaseHelper.SafeParameter(csv[31]);
                        dataRow["Country_Of_Origin"] = DatabaseHelper.SafeParameter(csv[32]);
                        dataRow["recordDeleted"] = false;
                        dataTable.Rows.Add(dataRow);
                    }

                }

                if (dataTable.Rows.Count > 0)
                {
                    Console.WriteLine($"Starting SQLBulkCopy, {dataTable.Rows.Count} rows");

                    using (var sqlConnection = new SqlConnection(ConfigurationManager.ConnectionStrings["connectionString"].ConnectionString))
                    {
                        sqlConnection.Open();
                        using (var sqlTransaction = sqlConnection.BeginTransaction())
                        {
                            //create temp table
                            using (var sqlCommand = new SqlCommand())
                            {
                                sqlCommand.Connection = sqlConnection;
                                sqlCommand.Transaction = sqlTransaction;
                                sqlCommand.CommandType = CommandType.Text;
                                sqlCommand.CommandText = @"
                                                    --drop first in case of prior error
                                                    IF OBJECT_ID('[FidessaImport].[dbo].[Client_Temp]', 'U') IS NOT NULL
                                                        DROP TABLE[FidessaImport].[dbo].[Client_Temp];

                                                    --create empty temp table
                                                    select top 0 * 
                                                    into [FidessaImport].[dbo].[Client_Temp] 
                                                    from [FidessaImport].[dbo].[Client]";
                                sqlCommand.ExecuteNonQuery();
                            }

                            //bulk copy all imported data to temp table
                            using (var sqlBulkCopy = new SqlBulkCopy(sqlConnection, SqlBulkCopyOptions.Default, sqlTransaction))
                            {
                                sqlBulkCopy.DestinationTableName = "[FidessaImport].[dbo].[Client_Temp]";
                                sqlBulkCopy.BatchSize = 500; //http://stackoverflow.com/questions/779690/what-is-the-recommended-batch-size-for-sqlbulkcopy
                                sqlBulkCopy.WriteToServer(dataTable);
                            }

                            //insert/update/delete Client table and drop temp table
                            using (var sqlCommand = new SqlCommand())
                            {
                                sqlCommand.Connection = sqlConnection;
                                sqlCommand.Transaction = sqlTransaction;
                                sqlCommand.CommandType = CommandType.Text;
                                sqlCommand.CommandText = @"
                                                    --delete
                                                    update [FidessaImport].[dbo].[Client]
                                                    set [recordDeleted] = 1
                                                    from [FidessaImport].[dbo].[Client]
                                                    C
                                                    left join [FidessaImport].[dbo].[Client_Temp] T on C.[Client_Id] = T.[Client_Id]
                                                    where T.[Client_Id] is null

                                                    --update (delete then insert)
                                                    delete from [FidessaImport].[dbo].[Client]
                                                    where [Client_Id] in (
                                                    select [Client_Id] from [FidessaImport].[dbo].[Client_Temp]
                                                    )

                                                    --insert
                                                    insert into [FidessaImport].[dbo].[Client]
                                                    select * from [FidessaImport].[dbo].[Client_Temp]
                                                    where [Client_Id] not in (
                                                    select [Client_Id] from [FidessaImport].[dbo].[Client]
                                                    )

                                                    --drop temp
                                                    drop table [FidessaImport].[dbo].[Client_Temp]
                                                ";

                                sqlCommand.ExecuteNonQuery();
                            }
                            sqlTransaction.Commit();
                        }
                    }
                }
                //copy files to imported directory after successful import
                File.Copy(path, path.Replace(ConfigurationManager.AppSettings["csvDirectory"], ConfigurationManager.AppSettings["importedDirectory"]));
            }
        }

        #region "helper methods"

        private static TimeSpan? ParseExactTimeSpan(string csvTime)
        {
            if (string.IsNullOrEmpty(csvTime) || csvTime == "0")
            {
                return null;
            }
            return TimeSpan.ParseExact(csvTime, "h\\:mm\\:ss", CultureInfo.InvariantCulture);
        }
        private static DateTime? ParseExactCsvDate(string csvDate)
        {
            if (string.IsNullOrEmpty(csvDate) || Regex.IsMatch(csvDate, "^[0]+$"))
            {
                return null;
            }
            return DateTime.ParseExact(csvDate, "yyyyMMdd", CultureInfo.InvariantCulture);
        }
        private static DateTime? ParseExactCsvDateTime(string csvDateTime)
        {
            if (string.IsNullOrEmpty(csvDateTime) || csvDateTime == "0")
            {
                return null;
            }
            return DateTime.ParseExact(csvDateTime, "yyyyMMdd HH:mm:ss", CultureInfo.InvariantCulture);
        }
        #endregion
    }
}