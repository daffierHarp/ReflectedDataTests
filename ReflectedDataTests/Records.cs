
#region using

using System;
using System.Collections.Generic;
using ReflectedData;
// ReSharper disable UnusedMember.Global
// ReSharper disable InconsistentNaming

#endregion

namespace ReflectedDataTests
{
#pragma warning disable 649

    [DataRecord(true, IDField = "ID")]
    internal class Customer : ReflectedTableLine<Customer>
    {
        public string Address, City, State;
        public int ID = -1;

        [DataField(Rename = "CustomerName", IsIndex = true)]
        public string Name;

        [DataJoinToMany(OtherTableField = "customerID")]
        public List<Order> Orders;

        [DataJoinToMany(OtherTableField = "customerID")]
        public JoinSet<Order> OrdersSet;

        public int ZipCode = -1;
    }

    [DataRecord(true, IDField = "ID")]
    internal class Order : ReflectedTableLine<Order>
    {
        [DataJoinToOne("customerID")] public Customer Customer = null;

        public int customerID = -1;
        public int ID = -1;
        public DateTime? OrderDate = DateTime.Now;

        [DataJoinToMany(OtherTableField = "orderID")]
        public List<OrderLine> OrderLines;

        [DataJoinToMany(OtherTableField = "orderID")]
        public JoinSet<OrderLine> OrderLinesSet;

        public bool PaymentRecieved = false, OrderShipped = false;
        public DateTime? PaymentRecieveDate = null;
        public DateTime? ShipDate = null;

        public decimal OrderPrice =>
            Convert.ToDecimal(Source.ExecuteScalarSqlToObject(
                // ReSharper disable once StringLiteralTypo
                "select sum(price) from orderlines where orderid=" + ID));
    }

    internal class OrderLine
    {
        [DataField] public string Detail = null;

        [DataIdField] public int ID = -1;

        [DataField] public int orderID = -1;

        [DataField] public decimal Price = 0m;
    }
}