### SQL Standards

#### Purpose

This document defines the SQL standards used in the PT Frozen Foods data platform.

It ensures consistency, readability, and maintainability across all SQL-based logic implemented in Databricks.

---

#### General Principles

- prioritize readability over complexity  
- write clear and explicit queries  
- avoid unnecessary nesting  
- keep queries modular and structured  

---

#### Query Structure

Recommended structure:

- SELECT  
- FROM  
- JOIN  
- WHERE  
- GROUP BY  
- ORDER BY  

Each clause should be clearly separated and properly formatted.

---

#### Formatting Rules

- use uppercase for SQL keywords  
- use lowercase for table and column names  
- align clauses vertically  
- use indentation for nested logic  

Example:

    SELECT
        customer_id,
        SUM(sales_amount) AS total_sales
    FROM ptfrozenfoods_dev.gold.fact_sales
    WHERE order_date >= '2024-01-01'
    GROUP BY customer_id

---

#### Naming in Queries

- use descriptive aliases  
- avoid single-letter aliases  
- prefer meaningful names over abbreviations  

Example:

    FROM fact_sales AS sales

---

#### Join Standards

- always use explicit JOIN types (INNER, LEFT, etc.)  
- avoid implicit joins  
- define join conditions clearly  

Example:

    SELECT
        f.order_id,
        d.customer_name
    FROM fact_sales AS f
    LEFT JOIN dim_customer AS d
        ON f.customer_id = d.customer_id

---

#### Filtering

- apply filters as early as possible  
- avoid unnecessary full table scans  
- use partition columns when available  

---

#### Aggregations

- always group by the correct grain  
- avoid mixing aggregated and non-aggregated columns  
- use clear aliasing for aggregated fields  

---

#### Performance Considerations

- avoid SELECT * in production queries  
- minimize data scanned  
- leverage Delta Lake optimizations  
- use clustering/partitioning when applicable  

---

#### Consistency

- follow the same structure across all queries  
- maintain uniform formatting  
- ensure queries are easy to read and review  

---

#### Notes

- these standards apply to all SQL used in Databricks notebooks  
- designed for scalability and team collaboration  
- aligned with best practices for analytical workloads  