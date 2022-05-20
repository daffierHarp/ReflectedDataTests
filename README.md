# ReflectedDataTests

## ReflectedData CSharp Library

by *Sivan Segev*  

This library was initially created around 2008 to wrap around Microsoft's data libraries to quickly access and
manipulate data files.

### Sample code

In the following example, we run a simple query with no attempt to reflect. We use a wrapper to avoid
advanced manipulation of DataSets

```csharp
    var src = new DataFileSource(StartupPath + @"\testDB.mdb") {ReuseConnection = true};
    var wrappedSet = src.GetDataSetWrapper("select * from customers");
    var linqEnumFromDataSet = from line in wrappedSet
        select new {
            CustomerName = (string) line["CustomerName"], City = (string) line["City"]
        };
    foreach (var linqLine in linqEnumFromDataSet)
        Debug.WriteLine(linqLine.CustomerName + " from " + linqLine.City);
```

To connect reflection to data, we need to define classes. The name of the class would match a single line of a table, and the table would be the plural of that name, for example:

```csharp
    [DataRecord(true, IDField = "ID")]
    class Customer : ReflectedTableLine<Customer> {
        public string Address, City, State;
        public int ID = -1;
        public int ZipCode = -1;
        // map data field name to class field name
        [DataField(Rename = "CustomerName", IsIndex = true)] public string Name;
        // advanced join examples
        [DataJoinToMany(OtherTableField = "customerID")]     public List<Order> Orders;
        [DataJoinToMany(OtherTableField = "customerID")]     public JoinSet<Order> OrdersSet;
    }

    [DataRecord(true, IDField = "ID")]
    class Order : ReflectedTableLine<Order> {
        public int customerID = -1;
        public int ID = -1;
        public DateTime? OrderDate = DateTime.Now;
        public bool PaymentReceived = false, OrderShipped = false;
        public DateTime? PaymentReceiveDate = null;
        public DateTime? ShipDate = null;

        // advanced join examples
        [DataJoinToOne("customerID")] public Customer Customer = null;
        [DataJoinToMany(OtherTableField = "orderID")] public List<OrderLine> OrderLines;
        [DataJoinToMany(OtherTableField = "orderID")] public JoinSet<OrderLine> OrderLinesSet;
        public decimal OrderPrice => Convert.ToDecimal(Source.ExecuteScalarSqlToObject("select sum(price) from orderLines where orderId=" + ID))
    }
```

Now that we have some mapping setup, we can query the data:

```csharp
    var customersList = ReflectedTable<Customer>.ToListFillJoins(src);
```

We can access the tables at the source, and perform queries there too:

```csharp
    var customers = src.Table<Customer>();
    var jerome = customers.LikeList("%jer%").FirstOrDefault();
```

Data instances can be edited and updated back to the tables:

```csharp
    jerome.ZipCode++;
    jerome.Update();
    var newOrder = orders.Insert(new Order {customerID = jerome.ID, OrderShipped = true}); 
```

More examples are available through the Program.Main method.

### Prerequisites

For some usages, this library requires an installation of Microsoft's data
engines, as of 6/17/2020 these can be found at the following links:

Data Engine 2010  
https://www.microsoft.com/EN-US/DOWNLOAD/confirmation.aspx?id=13255  
or, Data Engine 2016  
https://www.microsoft.com/en-us/download/details.aspx?id=54920

It appears that to access MDB or XLS files, the base libraries come with latest versions of Windows.

A trick to install both x86 and x64 is to install 2010 for x86, then install the x64 for 2016 with /passive /quiet command line arguments
