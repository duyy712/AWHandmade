using System;
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
            string connString = "DataSource=.\\MSSQLAS";
            using (Server server = new Server())
            {
                server.Connect(connString);

                string newDB = server.Databases.GetNewName("AW New Handmade");
                var db = new Database()
                {
                    Name = newDB,
                    ID = newDB,
                    CompatibilityLevel = 1200,
                    StorageEngineUsed = Microsoft.AnalysisServices.StorageEngineUsed.TabularMetadata
                };

                db.Model = new Model()
                {
                    Name = "AW New Handmade v1.0",
                    Description = "My new AW Handmade"
                };
                DataSet myDataSet = new DataSet();
                OleDbConnection myAccessConn = null;
                string dbSelection = "";
                //Lưu các bảng được chọn vào 1 list

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
        }

        public static List<Table> SelectedTables(string svr, string db)
        {
            List<Table> tblSelection = new List<Table>();
            OleDbConnection myAccessConn = null;
            string strConn1 = "Provider = SQLOLEDB; Data Source = " + svr + ";Initial Catalog=" + db+ "; Integrated Security = SSPI; Persist Security Info = false";
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

        public static List<Column> SelectedColumns (string svr,string db,Table table)
        {
            List<Column> colSelections = new List<Column>();
            OleDbConnection myAccessConn = null;
            
            string strConn1 = "Provider = SQLOLEDB; Data Source = " + svr + ";Initial Catalog=" + db + "; Integrated Security = SSPI; Persist Security Info = false";
            using (myAccessConn = new OleDbConnection(strConn1))
            {
                myAccessConn.Open();
                var col = myAccessConn.GetOleDbSchemaTable(OleDbSchemaGuid.Columns, new[] { null, table.TableSchema, table.TableName, null });
                foreach (DataRow dr in col.Rows)
                {
                    var column = new Column()
                    {
                        ColumnName = dr["COLUMN_NAME"].ToString(),
                        DataType = (ColumnDataType)Convert.ToInt32(dr["DATA_TYPE"]),
                        TableName = dr["TABLE_NAME"].ToString(),
                        TableSchema = dr["TABLE_SCHEMA"].ToString()

                    };
                    colSelections.Add(column);
                }

                
            }
            return colSelections;
        }
        public SingleColumnRelationship rel(Column fromColumn, Column toColumn, string name)
        {
            Microsoft.AnalysisServices.Tabular.DataColumn fc = new Microsoft.AnalysisServices.Tabular.DataColumn()
            {
                Name = fromColumn.ColumnName,
                DataType = (Microsoft.AnalysisServices.Tabular.DataType)fromColumn.DataType,
                SourceColumn = fromColumn.ColumnName
            };
            Microsoft.AnalysisServices.Tabular.DataColumn tc = new Microsoft.AnalysisServices.Tabular.DataColumn()
            {
                Name = toColumn.ColumnName,
                DataType = (Microsoft.AnalysisServices.Tabular.DataType)toColumn.DataType,
                SourceColumn = toColumn.ColumnName
            };

            SingleColumnRelationship SCR = new SingleColumnRelationship()
            {
                Name = name,
                FromColumn = fc,
                ToColumn = tc,
                FromCardinality = RelationshipEndCardinality.Many,
                ToCardinality = RelationshipEndCardinality.One

            };
            return SCR;
            
        }
        public static void createAnalysisServiceDatabase(string svr,string db)
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
                    ConnectionString = "Provider=SQLNCLI11;Data Source=" + svr +";Initial Catalog=" +db+";Integrated Security=SSPI;Persist Security Info=false",
                    ImpersonationMode = Microsoft.AnalysisServices.Tabular.ImpersonationMode.ImpersonateAccount,
                    Account = @".\duy",
                    Password = "duyduy123",
                });

                // 
                // Add the new database object to the server's  
                // Databases connection and submit the changes 
                // with full expansion to the server. 
                // 
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
            colList = SelectedColumns(svr, db, tblList[0]);
            //for (int i = 0; i < 10; i++)
            //{
            //    Console.WriteLine(colList)
            //}
            Console.WriteLine(colList[5].ColumnName);

            //createAnalysisServiceDatabase(svr, db);
            Console.ReadLine();
        }
    }
}
