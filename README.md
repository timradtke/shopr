
<!-- README.md is generated from README.Rmd. Please edit that file -->

# shopr

<!-- badges: start -->
<!-- badges: end -->

Use shopr as gym environment in which you can simulate a shop that faces
demand for its products and needs to make replenishment decisions based
on forecasts to keep the products in stock and turn a profit. Don’t
evaluate your forecasts by RMSE but by how much money you can make.

## Installation

You can install the development version of `shopr` from
[GitHub](https://github.com/timradtke/shopr) with:

``` r
# install.packages("devtools")
devtools::install_github("timradtke/shopr")
```

## Get Started

To start a simulation, first initialize the state. `shopr` does not
maintain state in memory via objects but writes changes to tables via
partitioned parquet files. Specify a path to a directory that `shopr`
should use to create the initial set of tables as well as for the
following updates.

``` r
path <- tempdir()
future_end_date <- as.Date("2025-12-31")

shopr::initialize(
  current_date = as.Date("2024-12-31"),
  history_start_date = as.Date("2024-12-01"),
  future_end_date = future_end_date,
  path = path
)
```

The simulation can be performed as a simple loop of daily iterations.
Each day starts with an `update()` which receives incoming shipments and
places them into inventory, to then serve demand from inventory and
observe sales.

Next, you should can decide how you’d like to forecast and decide which
orders to for replenishment of products to make. Submit orders as data
frame to `shopr::replenish()` which will ensure they arrive in future
iterations depending on products’ delivery times.

An iteration ends with recording the realized profit and cash
flow—mainly to ease later reporting of the overall simulation results.

``` r
for (day in 1:31) {
  # `update()` is the core of the simulation, updating inventory and sales
  current_date <- shopr::update(path = path)
  
  # While `shopr` provides internal `forecast()` and `make_order()` functions, 
  # they are intended for examples like the one here. You should create
  # forecasts and orders yourself, and pass them back to `shopr` via `replenish()`.
  df_forecast <- shopr:::forecast(
    current_date = current_date,
    future_end_date = future_end_date,
    path = path
  )
  
  df_order <- shopr:::make_order(
    forecast = df_forecast,
    current_date = current_date,
    path = path
  )
  
  # Submit your orders and they will appear in future iterations via `update()`
  shopr::replenish(order = df_order, current_date = current_date, path = path)
  shopr::record_profit_and_loss(current_date = current_date, path = path)
}
```

At the end of the simulation, gather the daily results into the overall
profit and loss and cash flow statement:

``` r
shopr::report_profit_and_loss(
  from_date = as.Date("2025-01-01"),
  to_date = current_date,
  path = path
)
#> # A tibble: 1 × 7
#>   Revenue  CoGS `Inventory CoGS` Profit `Order Cost` `Inventory Cost` Cashflow
#>     <dbl> <dbl>            <dbl>  <dbl>        <dbl>            <dbl>    <dbl>
#> 1    3800   380             249.  3171.          347             280.    3173.
```

As `shopr` maintains its state in parquet tables, you can close your
session, have lunch, come back later, and continue the simulation from
where you’ve left off:

``` r
for (day in 1:3) { # now simulating the first three days of February
  current_date <- shopr::update(path = path)
  # The shop will eventually run out-of-stock as we are not replenishing...
  shopr::record_profit_and_loss(current_date = current_date, path = path)
}

shopr::report_profit_and_loss(
  from_date = as.Date("2025-02-01"),
  to_date = current_date,
  path = path
)
#> # A tibble: 1 × 7
#>   Revenue  CoGS `Inventory CoGS` Profit `Order Cost` `Inventory Cost` Cashflow
#>     <dbl> <dbl>            <dbl>  <dbl>        <dbl>            <dbl>    <dbl>
#> 1     420    42             26.6   351.            0             19.1     401.
```

## Table Schema

To maintain state in tables, `shopr` comes with an internal definition
of tables and their (`arrow`) schemata.

Use `list_tables()` to list all tables that can be managed by `shopr`:

``` r
tables <- shopr::list_tables()
print(names(tables))
#>  [1] "settings"                        "demand"                         
#>  [3] "sales"                           "inventory"                      
#>  [5] "orders"                          "products"                       
#>  [7] "hierarchy"                       "profit_and_loss_by_sku_and_date"
#>  [9] "profit_and_loss_by_date"         "forecast"                       
#> [11] "out_of_stock"
```

Each of the tables comes with an `arrow` schema and partitioning
definition:

``` r
print(tables[["sales"]])
#> $name
#> [1] "sales"
#> 
#> $partitioning
#> [1] "date"
#> 
#> $schema
#> Schema
#> sku: string
#> date: date32[day]
#> sales: double
```

Access the tables directly using the `arrow` package, or via the wrapper
functions provided by `shopr`. For example, you can open any of the
tables as `arrow` data set by passing its name to `open_table()`:

``` r
ds_sales <- shopr::open_table(table = "sales", path = path)
print(ds_sales)
#> FileSystemDataset with 65 Parquet files
#> 3 columns
#> sku: string
#> date: date32[day]
#> sales: double
```

The data can then be queried and read using `dplyr` which can be more
efficient when data sets are larger and the table’s partitioning can be
exploited, for example by filtering for a specific date:

``` r
ds_sales |>
  dplyr::filter(date == current_date) |>
  dplyr::collect()
#> # A tibble: 2 × 3
#>   sku   date       sales
#>   <chr> <date>     <dbl>
#> 1 a     2025-02-03     5
#> 2 b     2025-02-03     7
```
