#' Report profit and loss for a range of dates
#' 
#' @export
report_profit_and_loss <- function(from_date, to_date, path) {
  pl <- open_table("profit_and_loss_by_date", path = path) |>
    dplyr::filter(date >= from_date, date <= to_date) |>
    summarize_profitability() |>
    dplyr::collect()
  
  return(pl)
}

#' Summarize profitability
#' 
#' @export
summarize_profitability <- function(df) {
  df |>
    dplyr::summarize(
      Revenue = sum(revenue),
      CoGS = sum(cost_of_goods_sold),
      `CoGS - Purchase` = sum(cogs_component_purchase),
      `CoGS - Order` = sum(cogs_component_order),
      `CoGS - Holding` = sum(cogs_component_holding),
      `Profit` = sum(profit),
      `Order Cost` = sum(order_cost),
      `Inventory Cost` = sum(inventory_cost),
      Cashflow = sum(cashflow),
      .groups = "drop"
    )
}

#' Generate a report of historic orders
#'
#' @export
report_orders <- function(current_date, path) {
  products <- open_table("products", path = path)
  date_28_days_ago <- current_date - lubridate::days(27)
  
  order_report <- open_table("orders", path = path) |>
    dplyr::filter(date_ordered >= date_28_days_ago) |>
    dplyr::select(sku, date_ordered, date_received, quantity) |>
    dplyr::inner_join(
      dplyr::select(
        products, sku, date, purchase_price, fixed_order_cost, delivery_days
      ),
      by = c(sku = "sku", date_received = "date")
    ) |>
    dplyr::group_by(sku) |>
    dplyr::summarize(
      n_orders = dplyr::n(),
      average_delivery_days = mean(delivery_days),
      average_order_size = mean(quantity),
      average_order_cost = mean(fixed_order_cost + purchase_price * quantity)
    ) |>
    dplyr::mutate(
      n_orders_expected = 28 / average_delivery_days,
      factor = n_orders / n_orders_expected
    ) |>
    dplyr::arrange(desc(factor)) |>
    dplyr::collect()
    
  return(order_report)
}

#' Generate a report of historic inventory
#'
#' @export
report_inventory <- function(current_date, path) {
  inventory <- open_table("inventory", path = path)
  
  batch_ages <- inventory |>
    dplyr::filter(date == current_date) |>
    dplyr::collect() |>
    dplyr::group_by(sku) |>
    dplyr::summarize(
      n_batches = length(unique(batch)),
      batch_age_oldest = min(batch),
      batch_age_youngest = max(batch)
    )
  
  out_of_stocks <- inventory |>
    dplyr::filter(inventory > 0) |>
    dplyr::group_by(sku) |>
    dplyr::summarize(latest_day_with_inventory = max(date), .groups = "drop") |>
    dplyr::filter(latest_day_with_inventory != current_date) |>
    dplyr::collect() |>
    dplyr::mutate(
      oos_for_n_days = as.numeric(current_date - latest_day_with_inventory)
    ) |>
    dplyr::arrange(desc(oos_for_n_days))
  
  inventory_report <- open_table("products", path = path) |>
    dplyr::filter(date == current_date) |>
    dplyr::select(sku) |>
    dplyr::left_join(out_of_stocks, by = "sku") |>
    dplyr::left_join(batch_ages, by = "sku") |>
    dplyr::arrange(sku) |>
    dplyr::collect()
  
  return(inventory_report)
}
