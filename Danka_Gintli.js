use geeksforgeeks;

// Beginners (B)

//Danka András B1-B10 + I1-I3

// B1. Retrieve all columns from the Sales table.
// SELECT * FROM Sales;
db.sales.find();

// B2. Retrieve the product_name and unit_price from the Products table.
// SELECT product_name, unit_price FROM Products;
db.products.find({}, { _id: 0, product_name: 1, unit_price: 1 });

// B3.Retrieve the sale_id and sale_date from the Sales table.
//SELECT sale_id, sale_date FROM Sales;
db.sales.find({}, { _id: 0, sale_id: 1, sale_date: 1 });

//B4. Filter the Sales table to show only sales with a total_price greater than $100.
//SELECT * FROM Sales WHERE total_price > 100;
db.sales.find({ total_price: { $gt: 100 } });

//B5. Filter the Products table to show only products in the 'Electronics' category.
//SELECT * FROM Products WHERE category = 'Electronics';
db.products.find({ category: 'Electronics' });

//B6. Retrieve the sale_id and total_price from the Sales table for sales made on January 3, 2024.
//SELECT sale_id, total_price 
//FROM Sales 
//WHERE sale_date = '2024-01-03';
db.sales.find(
    { sale_date: '2024-01-03' },
    { _id: 0, sale_id: 1, total_price: 1 }
);

//B7. Retrieve the product_id and product_name from the Products table for products with a unit_price greater than $100.
//SELECT product_id, product_name 
//FROM Products 
//WHERE unit_price > 100;
db.products.find(
    { unit_price: { $gt: 100 } },
    { _id: 0, product_id: 1, product_name: 1 }
);

//B8. Calculate the total revenue generated from all sales in the Sales table.
// SELECT SUM(total_price) AS total_revenue 
// FROM Sales;
db.sales.aggregate([
    {
        $group: {
            _id: null,
            total_revenue: { $sum: "$total_price" }
        }
    }
]);

//B9. Calculate the average unit_price of products in the Products table.
// SELECT AVG(unit_price) AS average_unit_price 
// FROM Products;
db.products.aggregate([
    {
        $group: {
            _id: null,
            average_unit_price: { $avg: "$unit_price" }
        }
    }
]);

//B10. Calculate the total quantity_sold from the Sales table.
// SELECT SUM(quantity_sold) AS total_quantity_sold 
// FROM Sales;
db.sales.aggregate([
    {
        $group: {
            _id: null,
            total_quantity_sold: { $sum: "$quantity_sold" }
        }
    }
]);

//I1. Calculate the total revenue generated from sales for each product category.
// SELECT p.category, SUM(s.total_price) AS total_revenue
// FROM Sales s
// JOIN Products p ON s.product_id = p.product_id
// GROUP BY p.category;
db.sales.aggregate([
    {
        $lookup: {
            from: "products",
            localField: "product_id",
            foreignField: "product_id",
            as: "product_info"
        }
    },
    {
        $unwind: "$product_info"
    },
    {
        $group: {
            _id: "$product_info.category",
            total_revenue: { $sum: "$total_price" }
        }
    },
    {
        $project: {
            _id: 0,
            category: "$_id",
            total_revenue: 1
        }
    }
]);

//I2. Find the product category with the highest average unit price.
// SELECT category
// FROM Products
// GROUP BY category
// ORDER BY AVG(unit_price) DESC
// LIMIT 1;
db.products.aggregate([
    {
        $group: {
            _id: "$category",
            average_unit_price: { $avg: "$unit_price" }
        }
    },
    {
        $sort: { average_unit_price: -1 }
    },
    {
        $limit: 1
    },
    {
        $project: {
            _id: 0,
            category: "$_id",
            average_unit_price: 1
        }
    }
]);

//I3. Identify products with total sales exceeding 30.
// SELECT p.product_name
// FROM Sales s
// JOIN Products p ON s.product_id = p.product_id
// GROUP BY p.product_name
// HAVING SUM(s.total_price) > 30;
db.sales.aggregate([
    {
        $lookup: {
            from: "products",
            localField: "product_id",
            foreignField: "product_id",
            as: "product_info"
        }
    },
    {
        $unwind: "$product_info"
    },
    {
        $group: {
            _id: "$product_info.product_name",
            total_price_sum: { $sum: "$total_price" }
        }
    },
    {
        $match: {
            total_price_sum: { $gt: 30 }
        }
    },
    {
        $project: {
            _id: 0,
            product_name: "$_id"
        }
    }
]);


//Gintli Máté B11-B20 + I4-I5

//B11. Retrieve the sale_id, product_id, and total_price from the Sales table for sales with a quantity_sold greater than 4.
// SELECT sale_id, product_id, total_price 
// FROM Sales 
// WHERE quantity_sold > 4;
db.sales.find({
    quantity_sold: { $gt: 4 }
  }, {
    sale_id: 1,
    product_id: 1,
    total_price: 1,
    _id: 0
  });

//B12. Retrieve the product_name and unit_price from the Products table, ordering the results by unit_price in descending order.
// SELECT product_name, unit_price 
// FROM Products 
// ORDER BY unit_price DESC;
db.products.find({}, {
    product_name: 1,
    unit_price: 1,
    _id: 0
  }).sort({
    unit_price: -1
  });

//B13. Retrieve the total_price of all sales, rounding the values to two decimal places.
// SELECT ROUND(SUM(total_price), 2) AS total_sales 
// FROM Sales;
db.sales.aggregate([
    {
      $group: {
        _id: null,
        total_sales: { $sum: "$total_price" }
      }
    },
    {
      $project: {
        _id: 0,
        total_sales: { $round: ["$total_sales", 2] }
      }
    }
  ]);

//B14. Calculate the average total_price of sales in the Sales table.
// SELECT AVG(total_price) AS average_total_price 
// FROM Sales;
db.sales.aggregate([
    {
      $group: {
        _id: null,
        average_total_price: { $avg: "$total_price" }
      }
    },
    {
      $project: {
        _id: 0,
        average_total_price: 1
      }
    }
  ]);

//B15. Retrieve the sale_id and sale_date from the Sales table, formatting the sale_date as 'YYYY-MM-DD'.
// SELECT sale_id, DATE_FORMAT(sale_date, '%Y-%m-%d') AS formatted_date 
// FROM Sales;
db.sales.aggregate([
    {
      $project: {
        sale_id: 1,
        formatted_date: {
          $dateToString: { format: "%Y-%m-%d", date: "$sale_date" }
        },
        _id: 0
      }
    }
  ]);

//B16. Calculate the total revenue generated from sales of products in the 'Electronics' category
// SELECT SUM(Sales.total_price) AS total_revenue 
// FROM Sales 
// JOIN Products ON Sales.product_id = Products.product_id 
// WHERE Products.category = 'Electronics';
db.sales.aggregate([
    {
      $lookup: {
        from: "products",
        localField: "product_id",
        foreignField: "product_id",
        as: "product_details"
      }
    },
    {
      $unwind: "$product_details"
    },
    {
      $match: {
        "product_details.category": "Electronics"
      }
    },
    {
      $group: {
        _id: null,
        total_revenue: { $sum: "$total_price" }
      }
    },
    {
      $project: {
        _id: 0,
        total_revenue: 1
      }
    }
  ]);

//B17. Retrieve the product_name and unit_price from the Products table, filtering the unit_price to show only values between $20 and $600.
db.products.find({
    unit_price: { $gte: 20, $lte: 600 }
  }, {
    product_name: 1,
    unit_price: 1,
    _id: 0
  });

//B18.Retrieve the product_name and category from the Products table, ordering the results by category in ascending order.
// SELECT product_name, category 
// FROM Products 
// ORDER BY category ASC;
db.products.find({}, {
    product_name: 1,
    category: 1,
    _id: 0
  }).sort({
    category: 1
  });

//19. Calculate the total quantity_sold of products in the 'Electronics' category.
// SELECT SUM(quantity_sold) AS total_quantity_sold 
// FROM Sales 
// JOIN Products ON Sales.product_id = Products.product_id 
// WHERE Products.category = 'Electronics';
db.sales.aggregate([
    {
      $lookup: {
        from: "products",
        localField: "product_id",
        foreignField: "product_id",
        as: "product_details"
      }
    },
    {
      $unwind: "$product_details"
    },
    {
      $match: {
        "product_details.category": "Electronics"
      }
    },
    {
      $group: {
        _id: null,
        total_quantity_sold: { $sum: "$quantity_sold" }
      }
    },
    {
      $project: {
        _id: 0,
        total_quantity_sold: 1
      }
    }
  ]);

//20. Retrieve the product_name and total_price from the Sales table, calculating the total_price as quantity_sold multiplied by unit_price.
// SELECT product_name, quantity_sold * unit_price AS total_price 
// FROM Sales 
// JOIN Products ON Sales.product_id = Products.product_id;
db.sales.aggregate([
    {
      $lookup: {
        from: "products",
        localField: "product_id",
        foreignField: "product_id",
        as: "product_details"
      }
    },
    {
      $unwind: "$product_details"
    },
    {
      $project: {
        product_name: "$product_details.product_name",
        total_price: { $multiply: ["$quantity_sold", "$product_details.unit_price"] },
        _id: 0
      }
    }
  ]);

//I4.Count the number of sales made in each month.
// SELECT DATE_FORMAT(s.sale_date, '%Y-%m') AS month, COUNT(*) AS sales_count
// FROM Sales s
// GROUP BY month;
db.sales.aggregate([
    {
      $group: {
        _id: { $dateToString: { format: "%Y-%m", date: "$sale_date" } },
        sales_count: { $sum: 1 }
      }
    },
    {
      $project: {
        month: "$_id",
        sales_count: 1,
        _id: 0
      }
    }
  ]);
  

//I5.Determine the average quantity sold for products with a unit price greater than $100.
// SELECT AVG(s.quantity_sold) AS average_quantity_sold
// FROM Sales s
// JOIN Products p ON s.product_id = p.product_id
// WHERE p.unit_price > 100;
db.sales.aggregate([
    {
      $lookup: {
        from: "products",
        localField: "product_id",
        foreignField: "product_id",
        as: "product_details"
      }
    },
    {
      $unwind: "$product_details"
    },
    {
      $match: {
        "product_details.unit_price": { $gt: 100 }
      }
    },
    {
      $group: {
        _id: null,
        average_quantity_sold: { $avg: "$quantity_sold" }
      }
    },
    {
      $project: {
        _id: 0,
        average_quantity_sold: 1
      }
    }
  ]);