﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AnalysisServices.Tabular;
using System.Data.OleDb;
using System.Data;
using Microsoft.AnalysisServices;

namespace AWHandmade1._1
{

    class Program
    {
        public class Table 
        {
            public string TableSchema { get; set; }
            public string TableName { get; set; }
        }

        public class Column
        {
            public string TableSchema { get; set; }
            public string TableName { get; set; }
            public string ColumnName { get; set; }
            public ColumnDataType DataType;
        }
        public enum ColumnDataType
        {
            Automatic = 1,
            String = 2,
            Int64 = 6,
            Double = 8,
            DateTime = 9,
            Decimal = 10,
            Boolean = 11,
            Binary = 17,
            Unknown = 19,
            Variant = 20
        }

        public static string SelectedDatabase(string svr)
        {
            OleDbConnection myAccessConn = null;
            string dbSelection = "";
            string strConn = "Provider=SQLOLEDB;Data Source=" + svr + ";Integrated Security=SSPI;Persist Security Info=false";


            using (myAccessConn = new OleDbConnection(strConn))
            {
                myAccessConn.Open();

                var dt = myAccessConn.GetOleDbSchemaTable(OleDbSchemaGuid.Catalogs, null);
                // Xuất ra list database 
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    Console.WriteLine("{0}. " + dt.Rows[i].ItemArray[0].ToString(), i + 1);
                }
                Console.Write("Enter Selection: ");
                int selector = int.Parse(Console.ReadLine());
                for (int i = 1; i < dt.Rows.Count + 1; i++)
                {
                    if (selector == i) dbSelection = dt.Rows[i - 1]["CATALOG_Name"].ToString();
                }
                //Console.WriteLine("Your selection is:{0} ", dbSelection);
                myAccessConn.Close();
            }
            return dbSelection;
        }


        public static List<Table> SelectedTables(string svr, string db)
        {
            List<Table> tblSelection = new List<Table>();
            OleDbConnection myAccessConn = null;
            string strConn1 = "Provider = SQLOLEDB; Data Source = " + svr + ";Initial Catalog=" + db + "; Integrated Security = SSPI; Persist Security Info = false";
            using (myAccessConn = new OleDbConnection(strConn1))
            {

                myAccessConn.Open();
                var tbl = myAccessConn.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, new[] { null, null, null, "TABLE" });
                foreach (DataRow dr in tbl.Rows)
                {
                    Console.WriteLine(dr["TABLE_NAME"].ToString());
                }
                Console.Write("Enter Selection (Separated by comma):  ");
                string str = Console.ReadLine();
                List<int> tblSelect = new List<int>();

                Array.ForEach(str.Split(",".ToCharArray()), s =>
                 {
                     int currentInt;
                     if (Int32.TryParse(s, out currentInt))
                         tblSelect.Add(currentInt);
                 });
                for (int i = 0; i < tblSelect.Count; i++)
                {
                    for (int j = 0; j < tbl.Rows.Count; j++)
                    {
                        if (tblSelect[i] == j)
                        {
                            var tb = new Table()
                            {
                                TableName = tbl.Rows[j - 1]["TABLE_NAME"].ToString(),
                                TableSchema = tbl.Rows[j - 1]["TABLE_SCHEMA"].ToString()
                            };
                            tblSelection.Add(tb);
                        }
                    }
                }
            }
            return tblSelection;
        }

        public static List<Microsoft.AnalysisServices.Tabular.DataColumn> SelectedColumns(string svr, string db, Table table)
        {
            List<Microsoft.AnalysisServices.Tabular.DataColumn> colSelections = new List<Microsoft.AnalysisServices.Tabular.DataColumn>();
            OleDbConnection myAccessConn = null;

            string strConn1 = "Provider = SQLOLEDB; Data Source = " + svr + ";Initial Catalog=" + db + "; Integrated Security = SSPI; Persist Security Info = false";
            using (myAccessConn = new OleDbConnection(strConn1))
            {
                myAccessConn.Open();
                var col = myAccessConn.GetOleDbSchemaTable(OleDbSchemaGuid.Columns, new[] { null, table.TableSchema, table.TableName, null });
                foreach (DataRow dr in col.Rows)
                {
                    var column = new Microsoft.AnalysisServices.Tabular.DataColumn()
                    {
                        Name = dr["COLUMN_NAME"].ToString(),
                        DataType = (Microsoft.AnalysisServices.Tabular.DataType)(ChangeDataType(Convert.ToInt32(dr["DATA_TYPE"]))),
                    };
                    Console.WriteLine(dr["COLUMN_NAME"].ToString() + ": " + dr["DATA_TYPE"]);
                    Console.WriteLine(column.DataType);
                    
                    colSelections.Add(column);
                }
                Console.ReadLine();
            }
            return colSelections;
        }
        public static int ChangeDataType(int n)
        {
            int result = 0;
            switch (n)
            {
                case 12: //variant
                    result = 20;
                    break;
                //////case 13://Unknown
                //    result = 19;
                //    break;
                case 128: case 205: case 204://Binary
                    result =  17;
                    break;
                case 11://Boolean
                    return 11;
                    break;
                case 14://Decimal
                    result = 10;
                    break;
                case 133: case 134: case 135://DateTime
                    result = 9;
                    break;
                case 5: case 131://Double
                    result = 8;
                    break;
                case 2: case 3: case 16: case 17: case 18: case 19: case 20: case 21: //Int64
                    result = 6;
                    break;
                case 129: case 130: case 200: case 201: case 202: case 203: case 72://String
                    result = 2;
                    break;
                default: result = 1;break; //Automatic


            }
            return result;
        }
            
        public static void CreateAnalysisServiceDatabase(string svr, string db, List<Table> tblList)
        {
            string ConnectionString = "DataSource=.\\MSSQLAS";

            // 
            // The using syntax ensures the correct use of the 
            // Microsoft.AnalysisServices.Tabular.Server object. 
            // 
            using (Server server = new Server())
            {
                server.Connect(ConnectionString);

                // 
                // Generate a new database name and use GetNewName 
                // to ensure the database name is unique. 
                // 
                string newDatabaseName =
                    server.Databases.GetNewName("AW Handmade v1.1");

                // 
                // Instantiate a new  
                // Microsoft.AnalysisServices.Tabular.Database object. 
                // 
                var dbWithDataSource = new Database()
                {
                    Name = newDatabaseName,
                    ID = newDatabaseName,
                    CompatibilityLevel = 1200,
                    StorageEngineUsed = StorageEngineUsed.TabularMetadata,
                };

                // 
                // Add a Microsoft.AnalysisServices.Tabular.Model object to the 
                // database, which acts as a root for all other Tabular metadata objects. 
                // 
                dbWithDataSource.Model = new Model()
                {
                    Name = "AW Handmade Model",
                    Description = "A Tabular data model at the 1200 compatibility level."
                };

                // 
                // Add a Microsoft.AnalysisServices.Tabular.ProviderDataSource object 
                // to the data Model object created in the previous step. The connection 
                // string of the data source object in this example  
                // points to an instance of the AdventureWorks2014 SQL Server database. 
                // 
                dbWithDataSource.Model.DataSources.Add(new ProviderDataSource()
                {
                    Name = "SQL Server Data Source Example",
                    Description = "A data source definition that uses explicit Windows credentials for authentication against SQL Server.",
                    ConnectionString = "Provider=SQLNCLI11;Data Source=" + svr + ";Initial Catalog=" + db + ";Integrated Security=SSPI;Persist Security Info=false",
                    ImpersonationMode = Microsoft.AnalysisServices.Tabular.ImpersonationMode.ImpersonateAccount,
                    Account = @".\duy",
                    Password = "duyduy123",
                });

                // 
                // Add the new database object to the server's  
                // Databases connection and submit the changes 
                // with full expansion to the server. 
                // 

                foreach (Table tbl in tblList)
                {
                    Microsoft.AnalysisServices.Tabular.Table ttbl = new Microsoft.AnalysisServices.Tabular.Table();
                    ttbl.Name = tbl.TableName;
                    var sel = SelectedColumns(svr, db, tbl);

                    foreach (Microsoft.AnalysisServices.Tabular.DataColumn dc in sel)
                    {
                        ttbl.Columns.Add(dc);
                    }

                    ttbl.Partitions.Add(new Partition()
                    {
                        Name = "All",
                        Source = new QueryPartitionSource()
                        {
                            DataSource = dbWithDataSource.Model.DataSources["SQL Server Data Source Example"],
                            Query = "Select * from " + ttbl.Name
                        }

                    });
                    dbWithDataSource.Model.Tables.Add(ttbl);
                }



                server.Databases.Add(dbWithDataSource);
                dbWithDataSource.Update(UpdateOptions.ExpandFull);

                Console.Write("Database ");
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.Write(dbWithDataSource.Name);
                Console.ResetColor();
                Console.WriteLine(" created successfully.");
                Console.WriteLine("The data model includes the following data source definitions:");
                Console.ForegroundColor = ConsoleColor.Yellow;
                foreach (DataSource ds in dbWithDataSource.Model.DataSources)
                {
                    Console.WriteLine("\tData source name:\t\t{0}", ds.Name);
                    Console.WriteLine("\tData source description:\t{0}", ds.Description);
                }
                Console.ResetColor();
                Console.WriteLine();
            }
            Console.WriteLine("Press Enter to close this console window.");
            Console.ReadLine();

        }

        static void Main(string[] args)
        {

            string svr;
            string db;
            List<Table> tblList = new List<Table>();
            List<Column> colList = new List<Column>();
            Console.Write("Enter SQL Server: ");
            svr = Console.ReadLine().ToString();
            db = SelectedDatabase(svr);
            //Console.WriteLine(db);
            //Console.ReadLine();
            tblList = SelectedTables(svr, db);
            //for (int i = 0; i < tblList.Count; i++)
            //{
            //    Console.WriteLine(tblList[i].TableName);
            //}
            //Console.ReadLine();

            Console.WriteLine(tblList[0].TableName);
            //colList = SelectedColumns(svr, db, tblList[0]);
            //for (int i = 0; i < 10; i++)
            //{
            //    Console.WriteLine(colList)
            //}
            // Tùy vào bảng mà chỉ số có thể không chạy được
            //Console.WriteLine(colList[3].ColumnName);

            CreateAnalysisServiceDatabase(svr, db,tblList);
            Console.ReadLine();
        }
    }
}
