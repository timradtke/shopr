#' List tables managed by `shopr`
#' 
#' @return A list of lists, each of which represent one table managed by `shopr`
#'     to maintain the simulation's current state. Each table definition comes
#'     with `arrow` partitioning and schema.
#' 
#' @export
list_tables <- function() {
  list_of_tables <- list(
    settings = list(
      name = "settings",
      partitioning = NULL,
      schema = arrow::schema(
        arrow::field("seed", arrow::int32()),
        arrow::field("date", arrow::date32()),
        arrow::field("history_start_date", arrow::date32()),
        arrow::field("future_start_date", arrow::date32()),
        arrow::field("future_end_date", arrow::date32())
      )
    ),
    demand = list(
      name = "demand",
      partitioning = "date",
      schema = arrow::schema(
        arrow::field("sku", arrow::utf8()),
        arrow::field("date", arrow::date32()),
        arrow::field("demand", arrow::float64())
      )
    ),
    sales = list(
      name = "sales",
      partitioning = "date",
      schema = arrow::schema(
        arrow::field("sku", arrow::utf8()),
        arrow::field("date", arrow::date32()),
        arrow::field("sales", arrow::float64())
      )
    ),
    inventory = list(
      name = "inventory",
      # simplified assumption: only a single warehouse
      partitioning = "date",
      schema = arrow::schema(
        arrow::field("sku", arrow::utf8()),
        arrow::field("date", arrow::date32()),
        # multiple rows per sku-date
        arrow::field("batch", arrow::date32()),
        arrow::field("inventory", arrow::float64())
      )
    ),
    orders = list(
      name = "orders",
      # going to be a "big" table because it keeps snapshots by date
      # -> duplicates orders; save old orders into a "closed_orders" table
      partitioning = c("date_received"),
      schema = arrow::schema(
        # order ID is needed if we can have multiple orders open at the same
        # time for one material
        arrow::field("order", arrow::utf8()),
        arrow::field("sku", arrow::utf8()),
        #arrow::field("date", arrow::date32()),
        arrow::field("open", arrow::boolean()),
        arrow::field("date_ordered", arrow::date32()),
        arrow::field("date_received", arrow::date32()),
        arrow::field("quantity", arrow::float64()),
        arrow::field("purchase_price", arrow::float64()),
        arrow::field("fixed_order_cost", arrow::float64())
      )
    ),
    products = list(
      name = "products",
      partitioning = c("date"),
      schema = arrow::schema(
        arrow::field("sku", arrow::utf8()),
        arrow::field("date", arrow::date32()),
        arrow::field("stock_cost", arrow::float64()),
        arrow::field("purchase_price", arrow::float64()),
        arrow::field("sales_price", arrow::float64()),
        arrow::field("delivery_days", arrow::int32()),
        arrow::field("minimum_order_quantity", arrow::int32()),
        arrow::field("lot_size", arrow::int32()),
        arrow::field("fixed_order_cost", arrow::float64())
      )
    ),
    hierarchy = list(
      # TODO: This table doesn't make sense for anything but the worked example.
      # For the most part it's okay to have this here because `arrow` will
      # ignore when data frames don't have the specified columns,
      # but the hierarchy columns should not be hardcoded like this.
      # Depending on the use case, different hierarchy columns should be
      # defined by the user.
      name = "hierarchy",
      partitioning = NULL,
      schema = arrow::schema(
        arrow::field("sku", arrow::utf8()),
        arrow::field("emoji_image", arrow::utf8()),
        arrow::field("emoji_group", arrow::utf8()),
        arrow::field("emoji_version", arrow::float64()),
        arrow::field("has_skin_tone_support", arrow::boolean()),
        arrow::field("is_private_label", arrow::boolean())
      )
    ),
    profit_and_loss_by_sku_and_date = list(
      name = "profit_and_loss_by_sku_and_date",
      partitioning = "date",
      schema = arrow::schema(
        arrow::field("sku", arrow::utf8()),
        arrow::field("date", arrow::date32()),
        arrow::field("sales", arrow::float64()),
        arrow::field("revenue", arrow::float64()),
        arrow::field("cost_of_goods_sold", arrow::float64()),
        arrow::field("inventory_cost_of_goods_sold", arrow::float64()),
        arrow::field("profit", arrow::float64()),
        arrow::field("order_cost", arrow::float64()),
        arrow::field("inventory_cost", arrow::float64()),
        arrow::field("cashflow", arrow::float64())
      )
    ),
    profit_and_loss_by_date = list(
      name = "profit_and_loss_by_date",
      partitioning = "date",
      schema = arrow::schema(
        arrow::field("date", arrow::date32()),
        arrow::field("revenue", arrow::float64()),
        arrow::field("cost_of_goods_sold", arrow::float64()),
        arrow::field("inventory_cost_of_goods_sold", arrow::float64()),
        arrow::field("profit", arrow::float64()),
        arrow::field("order_cost", arrow::float64()),
        arrow::field("inventory_cost", arrow::float64()),
        arrow::field("cashflow", arrow::float64())
      )
    ),
    forecast = list(
      name = "forecast",
      partitioning = "train_end",
      schema = arrow::schema(
        arrow::field("train_end", arrow::date32()),
        arrow::field("path", arrow::int32()),
        arrow::field("date", arrow::date32()),
        arrow::field("sku", arrow::utf8()),
        arrow::field("prediction", arrow::float64())
      )
    ),
    out_of_stock = list(
      name = "out_of_stock",
      partitioning = "date",
      schema = arrow::schema(
        arrow::field("sku", arrow::utf8()),
        arrow::field("date", arrow::date32()),
        arrow::field("sales_price", arrow::float64()),
        arrow::field("purchase_price", arrow::float64()),
        arrow::field("stock_cost", arrow::float64())
      )
    )
  )
  
  return(list_of_tables)
}

#' Open a table managed by `shopr`
#' 
#' Wrapper around `arrow::open_dataset()` to open a table managed by `shopr` as
#' data set.
#' 
#' @param table Name of the table that should be opened, as defined in
#'     `shopr`'s internal schema (see `shopr::list_tables()`).
#' @param path Path to the directory in which the gym's state is stored
#' 
#' @export
open_table <- function(table, path) {
  ds <- arrow::open_dataset(
    sources = file.path(path, list_tables()[[table]][["name"]]),
    schema = list_tables()[[table]][["schema"]],
    partitioning = list_tables()[[table]][["partitioning"]],
    hive_style = TRUE,
    format = "parquet",
    unify_schemas = FALSE
  )
  
  return(ds)
}

#' Write a table managed by `shopr`
#' 
#' @param dataset A data frames or `arrow` data set
#' @param table Name of the table that should be written to, as defined in
#'     `shopr`'s internal schema (see `shopr::list_tables()`).
#' @param path Path to the directory in which the gym's state is stored
#'
#' @return TRUE, invisibly
#'
#' @export
write_table <- function(dataset, table, path) {
  arrow::write_dataset(
    dataset = dataset,
    path = file.path(path, list_tables()[[table]][["name"]]),
    format = "parquet",
    partitioning = list_tables()[[table]][["partitioning"]],
    hive_style = TRUE,
    existing_data_behavior = "delete_matching"
  )
  
  return(invisible(TRUE))
}

#' Write tables managed by `shopr`
#' 
#' Wrapper around `write_table` that writes a named list of tables by looking up
#' the table's names in `shopr`'s table schema.
#'
#' @param named_datasets A named list of data frames or `arrow` data sets
#' @param path Path to the directory in which the gym's state is stored
#'
#' @return TRUE, invisibly
#'
#' @export
write_tables <- function(named_datasets, path) {
  for (i in seq_along(named_datasets)) {
    write_table(
      dataset = named_datasets[[i]],
      table = names(named_datasets)[i],
      path = path
    )
  }
  
  return(invisible(TRUE))
}
