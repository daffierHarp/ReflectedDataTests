
#region using

using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using ReflectedData;
// ReSharper disable InconsistentNaming
// ReSharper disable UnusedVariable

#endregion

namespace ReflectedDataTests
{
    internal class Program
    {
        static string StartupPath
        {
            get
            {
                var result = Path.GetDirectoryName(
                    Assembly.GetEntryAssembly()?.Location);
                if (result != null && result.StartsWith("file:\\"))
                    result = result.Substring(6);
                return result;
            }
        }

        // ReSharper disable once UnusedParameter.Local
        static void Main(string[] args)
        {
            // simplify connection creation by referring file path and immediately start using data
            // ReSharper disable once UnusedVariable
            // ReSharper disable once StringLiteralTypo
            var srcNew = new DataFileSource(StartupPath + @"\testDB.accdb");
            var dataSetNew = srcNew.GetDataSet("select * from customers");
            dataSetNew.Dispose();
            if (IntPtr.Size==8)
                DataFileSource.AllowOldEngine = false;

            var src = new DataFileSource(StartupPath + @"\testDB.mdb") {
                ReuseConnection = true,
            };

            // Run a query into a data-set, reflection not involved yet
            var dataSet = src.GetDataSet("select * from customers");
            dataSet.Dispose();

            // We can use linq to a data-set-wrapper and then assign anonymous types instead of defining reflect-able tables
            var wrappedSet = src.GetDataSetWrapper("select * from customers");
            var linqEnumFromDataSet = from line in wrappedSet
                select new {
                    CustomerName = (string) line["CustomerName"],
                    City = (string) line["City"]
                };
            foreach (var linqLine in linqEnumFromDataSet)
                Debug.WriteLine(linqLine.CustomerName + " from " + linqLine.City);

            // We can access tables by defining record classes
            var customers = src.Table<Customer>();
            var orders = src.Tables.Get<Order>();
            var orderLines = src.Table<OrderLine>();

            // Run a static function over the table class to get a list of records and join-lists populated with data
            var customersList = ReflectedTable<Customer>.ToListFillJoins(src);
            var wasSameTableInstanceUtilized = customersList[0].AtTable == customers; // should be true

            // We can utilize these instances for quick select queries, enumerations as well as joins
            var jerome = customers.LikeList("%jer%").FirstOrDefault();

            // the line instances can be used to modify the data
            if (jerome != null) {
                jerome.ZipCode++;
                jerome.Update();

                // test sql insert. 
                var newOrder = orders.Insert(new Order {customerID = jerome.ID, OrderShipped = true}); // insert
                var newOrderCopy = orders.Get(newOrder.ID); // query
                Debug.Assert(newOrder.customerID == newOrderCopy.customerID); // verify data
                Debug.Assert(newOrder.OrderShipped == newOrderCopy.OrderShipped);

                // Query into enumerable without table definition directly on data source. Reflection is performed to identify returned fields
                // this query uses a simple class definition without any attributes
                foreach (var detailAndPrice in src.SelectUnattributed<DetailAndPrice>("OrderLines", null))
                    Debug.WriteLine(
                        "detail: " + detailAndPrice.Detail + "\t ,price:" + detailAndPrice.Price);

                // Using an attributed reflected class - define the join relationship as a parameter to the function to return a list of results
                var selectJoinList = src.SelectList<CustomSelectJoinLine>(
                    "(Customers LEFT JOIN Orders ON Customers.ID = Orders.customerID)" +
                    "LEFT JOIN OrderLines ON Orders.ID = OrderLines.orderID", null);

                // Get a list from an sql query built by a tool. The line type of the returned list is defined by a class
                var customerAndSumList = src.SelectList<CustomerAndSum>(
                    @"SELECT DISTINCTROW Customers.CustomerName, Sum(OrderLines.Price) AS [Sum Of Price]
FROM (Customers LEFT JOIN Orders ON Customers.[ID] = Orders.[customerID]) LEFT JOIN OrderLines ON Orders.[ID] = OrderLines.[orderID]
GROUP BY Customers.CustomerName;");

                // test select-unattributed-list, no where
                var detailAndPriceList = src.SelectUnattributedList<DetailAndPrice>("OrderLines", null);

                // Run queries utilizing reflection and hierarchies defined by JoinSet classes
                // in the following test, data-readers are enumerated and not cached as memory lists
                foreach (var c in customers.ToEnumerable())
                foreach (var o in c.OrdersSet)
                foreach (var ol in o.OrderLinesSet)
                    Debug.WriteLine(
                        c.Name + " bought " + ol.Detail + ":" + ol.Price);

                // test table-join with different syntax
                foreach (var joinTableLine in src.Join<Customer, Order>("customerID"))
                    Debug.WriteLine(
                        joinTableLine.l.Name + " bought on " +
                        joinTableLine.r.OrderDate);
                foreach (var joinTableLine in src.Table<Customer>().JoinEn<Order>(-1, "customerID", null, null))
                    Debug.WriteLine(
                        joinTableLine.l.Name + " bought on " +
                        joinTableLine.r.OrderDate);

                // reuse join sets to perform group
                foreach (var joinedGroupedLine in src.Join<Customer, Order, OrderLine>(
                    "customerID", "orderID").Group(
                    "Customers.ID", "CustomerName", SqlFunction.Sum, "Price", null))
                    Debug.WriteLine(joinedGroupedLine.extraField + " owes: " +
                                    joinedGroupedLine.functionField);

                // run enumeration of hierarchy join-set relationships when several threads are involved
                var testThreads = new Thread[10];
                for (var i = 0; i < testThreads.Length; i++) {
                    testThreads[i] = new Thread(() => testHirarchySetQueries(src)) {Name = "thread " + i};
                }

                // ReSharper disable once ForCanBeConvertedToForeach
                for (var i = 0; i < testThreads.Length; i++)
                    testThreads[i].Start();
                // ReSharper disable once ForCanBeConvertedToForeach
                for (var i = 0; i < testThreads.Length; i++)
                    testThreads[i].Join();

                // the same test over several threads in a 2000 access data file
                testThreads = new Thread[10];
                for (var i = 0; i < testThreads.Length; i++) {
                    testThreads[i] = new Thread(() => testHirarchySetQueries(src)) {Name = "thread " + i};
                }

                // ReSharper disable once ForCanBeConvertedToForeach
                for (var i = 0; i < testThreads.Length; i++)
                    testThreads[i].Start();
                // ReSharper disable once ForCanBeConvertedToForeach
                for (var i = 0; i < testThreads.Length; i++)
                    testThreads[i].Join();

                // test source clone, and get data set over custom sql
                var srcClone = src.Clone();
                var dataSet2 = srcClone.GetDataSet("select * from customers");
                srcClone.Dispose();
                dataSet2.Dispose();

                // Use indexing to query for a record and update a record
                var someone = customers[customerAndSumList[0].CustomerName];
                someone.ZipCode++;
                customers[someone.Name] = someone;

                // easy code to insert new records into tables, doesn't keep an in memory reference
                orderLines.Insert(new OrderLine {
                    orderID = newOrder.ID,
                    Detail = "test123",
                    Price = 23.4m
                });
                orderLines.Insert(new OrderLine {
                    orderID = newOrder.ID,
                    Detail = "delete123",
                    Price = 23.4m
                });

                // utilize the ReflectedTableLine fill join mechanism to fill the empty list with data from storage
                newOrder.FillJoin("OrderLines");

                // delete new order and related order lines
                src.Delete("OrderLines", "orderID", newOrder.ID);
                newOrder.Delete();
            }

            // test fill joins to fill in-memory lists with data
            foreach (var c in customersList) {
                c.FillJoins();
                foreach (var o in c.Orders) o.FillJoins();
            }

            // at this point should have an in memory tree of all customers->orders->order-lines
            var editOrderLine = customersList.Last().Orders.Last().OrderLines.LastOrDefault();
            if (editOrderLine != null) {
                editOrderLine.Price = 44.21m;
                orderLines.Update(editOrderLine); // use table instance to call update
            } else {
                var lastOrder = customersList.Last().Orders.Last();
                orderLines.Insert(new OrderLine {
                    orderID = lastOrder.ID,
                    Detail = "stack of papers",
                    Price = 43.22m
                });
            }

            // test access to xlsx file, reuse the same reflected table classes
            var xlsxTest = new DataFileSource(StartupPath + @"\Customers.xlsx");
            var xlsxCustomersTable = xlsxTest.Tables.Get<Customer>();
            var xlsxCustomersTable2ndInstance = xlsxTest.Table<Customer>(); // alternative syntax
            Debug.Assert(xlsxCustomersTable == xlsxCustomersTable2ndInstance);
            var xlsxCustomersList = xlsxCustomersTable.ToList();
            xlsxCustomersList[0].ZipCode++;
            xlsxCustomersList[0].Update();

            // create table, drop table and copy records from one source to the other
            var dataTableNames = xlsxTest.GetDataTableNames();
            if (dataTableNames.Contains("Orders"))
                xlsxTest.Table<Order>().DropDataTable();

            xlsxTest.Table<Order>().CreateDataTable();
            foreach (var o in orders.ToEnumerable())
                processLock(()=> xlsxTest.Table<Order>().Insert(o));

            // test export to excel
            testExportToExcel();

            // test creating new MDB database
            testNewMdb();

            // test access to local sql server express database
            // testSqlServer(orders);
        }

        static void processLock(Action a, bool inLock = false, string mutexName = "update-db")
        {
            if (inLock) {
                a();
                return;
            }
            var mx = new Mutex(false, mutexName);
            mx.WaitOne();
            a();
            mx.ReleaseMutex();
        }
        static void processLock2(Action<bool> a, bool inLock = false, string mutexName = "update-db")
        {
            if (inLock) {
                a(true);
                return;
            }
            var mx = new Mutex(false, mutexName);
            mx.WaitOne();
            a(true);
            mx.ReleaseMutex();
        }
        static void processLock3(Action a, ref bool inLock , string mutexName = "update-db")
        {
            if (inLock) {
                a();
                return;
            }
            var mx = new Mutex(false, mutexName);
            mx.WaitOne();
            inLock = true;
            a();
            inLock = false;
            mx.ReleaseMutex();
        }

        static void testExportToExcel()
        {
            if (File.Exists(StartupPath + @"\Customers1.xlsx"))
                File.Delete(StartupPath + @"\Customers1.xlsx");
            var newXlsxTest = new DataFileSource(StartupPath + @"\Customers1.xlsx");
            var exportTable = newXlsxTest.Table<Customer>();
            exportTable.CreateDataTable();
            exportTable.Insert(new Customer
                { Name = "export dude", Address = "234 3rd st", City = "Detroit", ZipCode = 48880, State = "MI" });
            newXlsxTest.Dispose();


            var importFile = new DataFileSource(newXlsxTest.FilePath);
            importFile.Open();
            var importReader = importFile.ExecuteReader("select * from [Customers$]");
            var importTable = importFile.Table<Customer>();
            int row = 0;
            while (importReader.Read()) {
                row++;
                var l = importTable.ReaderToLine(importReader);
            }
        }

        static void testNewMdb()
        {
            var testNewMdbPath = StartupPath + @"\test_new.mdb";
            if (File.Exists(testNewMdbPath)) File.Delete(testNewMdbPath);
            File.Copy(StartupPath + @"\empty.mdb",
                testNewMdbPath); // either use an empty file (200k) or use ADOX or office COM objects
            var newMdbTest = new DataFileSource(testNewMdbPath);
            newMdbTest.Open();
            var newMdbTable = newMdbTest.Table<Customer>();
            newMdbTable.CreateDataTable();
            newMdbTable.Insert(new Customer
                { Name = "export dude", Address = "234 3rd st", City = "Detroit", ZipCode = 48880, State = "MI" });
            newMdbTest.Dispose();

            var readNewMdb = new DataFileSource(testNewMdbPath);
            var testReadList = readNewMdb.Table<Customer>().ToList();
            readNewMdb.Dispose();
        }
        // ReSharper disable UnusedMember.Global

#pragma warning disable 649
        [DataRecord(true)]
        class CustomSelectJoinLine
        {
            public string CustomerName;
            public DateTime? OrderDate;
            public decimal Price;
        }

        [DataRecord(true)]
        class CustomerAndSum
        {
            public string CustomerName;
            public decimal SumOfOrders;
        }

        class DetailAndPrice
        {
            public string Detail;
            public decimal Price;
        }
        // ReSharper restore UnusedMember.Global

        // ReSharper disable once UnusedMember.Local
        static void testSqlServer(ReflectedTable<Order> orders)
        {
            var sqlTest = new SqlServerSource("TestDb1");
            var sqlCustomersTable = sqlTest.Tables.Get<Customer>();
            var cl2 = sqlCustomersTable.ToList();
            cl2[0].ZipCode++;
            cl2[0].Update();
            var newCustomer = new Customer {
                Name = "Delete Me",
                Address = "123 You're dead",
                City = "Hell",
                State = "MI",
                ZipCode = 666
            };
            sqlCustomersTable.Insert(newCustomer);
            newCustomer.Delete();

            var sqlDataTableNames = sqlTest.GetDataTableNames();
            if (sqlDataTableNames.Contains("Orders"))
                sqlTest.Table<Order>().DropDataTable();
            sqlTest.Table<Order>().CreateDataTable();
            foreach (var o in orders.ToEnumerable())
                sqlTest.Table<Order>().Insert(o);
            sqlTest.Table<Order>().DropDataTable();

            // copying a hierarchy set from a one source to the other requires mapping new ID's!
        }

        static void testHirarchySetQueries(DataFileSource src)
        {
            foreach (var c in src.Table<Customer>().ToEnumerable())
            foreach (var o in c.OrdersSet)
            foreach (var ol in o.OrderLinesSet)
                Debug.WriteLine(
                    Thread.CurrentThread.Name + ">>" +
                    c.Name + " bought " + ol.Detail + ":" + ol.Price);
        }
    }
}