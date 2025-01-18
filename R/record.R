#' Write tables that record profit and loss for the current date
#' 
#' @export
record_profit_and_loss <- function(current_date, path, verbose = FALSE) {
  date_yesterday <- current_date - 1
  products <- open_table("products", path = path)
  
  order_costs <- open_table("orders", path = path) |>
    dplyr::filter(date_received == current_date) |>
    dplyr::select(sku, date_received, quantity) |>
    dplyr::inner_join(
      dplyr::select(products, sku, date, purchase_price, fixed_order_cost),
      by = c(sku = "sku", date_received = "date")
    ) |>
    dplyr::mutate(order_cost = fixed_order_cost + purchase_price * quantity) |>
    dplyr::group_by(sku, date = date_received) |>
    dplyr::summarize(
      order_cost = sum(order_cost),
      .groups = "drop"
    )
  
  sales_revenue <- open_table("sales", path = path) |>
    dplyr::filter(date == current_date) |>
    dplyr::select(sku, date, sales) |>
    dplyr::inner_join(
      dplyr::select(products, sku, date, sales_price, purchase_price),
      by = c("sku", "date")
    ) |>
    dplyr::mutate(
      revenue = sales_price * sales,
      # Assumes that the `purchase_price` does not vary over time, else we would
      # need to identify the price at which the material was ordered in the past
      cost_of_goods_sold = purchase_price * sales) |>
    dplyr::select(sku, date, sales, revenue, cost_of_goods_sold)
  
  inventory_yesterday <- open_table("inventory", path = path) |>
    dplyr::filter(date == date_yesterday) |>
    dplyr::select(-date) |>
    dplyr::rename(inventory_before = inventory)
  
  inventory_today <- open_table("inventory", path = path) |>
    dplyr::filter(date == current_date)
  
  inventory_cost_of_goods_sold <- inventory_yesterday |>
    dplyr::full_join(inventory_today, by = c("sku", "batch")) |>
    dplyr::mutate(inventory = dplyr::coalesce(inventory, 0)) |>
    # products sold today based on inventory that came in today didn't cost
    dplyr::filter(!is.na(inventory_before)) |>
    dplyr::inner_join(
      dplyr::select(products, sku, date, stock_cost),
      by = c(sku = "sku", batch = "date")
    ) |>
    dplyr::collect() |>
    dplyr::mutate(
      sold_from_inventory = inventory_before - inventory,
      days_on_stock = as.numeric(date - batch),
      inventory_cost_of_goods_sold =
        sold_from_inventory * days_on_stock * stock_cost
    ) |>
    dplyr::filter(sold_from_inventory > 0) |>
    dplyr::group_by(sku, date) |>
    dplyr::summarize(
      inventory_cost_of_goods_sold = sum(inventory_cost_of_goods_sold),
      .groups = "drop"
    )
  
  inventory_costs <- open_table("inventory", path = path) |>
    dplyr::filter(date == current_date) |>
    dplyr::select(sku, date, batch, inventory) |>
    dplyr::inner_join(
      dplyr::select(products, sku, date, stock_cost),
      by = c(sku = "sku", batch = "date")
    ) |>
    dplyr::mutate(inventory_cost = stock_cost * inventory) |>
    dplyr::group_by(sku, date) |>
    dplyr::summarize(
      inventory_cost = sum(inventory_cost),
      .groups = "drop"
    )
  
  profit_and_loss_by_sku_and_date <- products |>
    dplyr::filter(date == current_date) |>
    dplyr::select(sku, date) |>
    dplyr::left_join(sales_revenue, by = c("sku", "date")) |>
    dplyr::left_join(inventory_cost_of_goods_sold, by = c("sku", "date")) |>
    dplyr::left_join(inventory_costs, by = c("sku", "date")) |>
    dplyr::left_join(order_costs, by = c("sku", "date")) |>
    dplyr::mutate(
      sales = dplyr::coalesce(sales, 0),
      revenue = dplyr::coalesce(revenue, 0),
      cost_of_goods_sold = dplyr::coalesce(cost_of_goods_sold, 0),
      inventory_cost_of_goods_sold =
        dplyr::coalesce(inventory_cost_of_goods_sold, 0),
      profit = revenue - cost_of_goods_sold - inventory_cost_of_goods_sold,
      order_cost = dplyr::coalesce(order_cost, 0),
      inventory_cost = dplyr::coalesce(inventory_cost, 0),
      cashflow = revenue - order_cost - inventory_cost,
    )
  
  write_table(
    dataset = profit_and_loss_by_sku_and_date,
    table = "profit_and_loss_by_sku_and_date",
    path = path
  )
  
  profit_and_loss_by_date <- profit_and_loss_by_sku_and_date |>
    dplyr::group_by(date) |>
    dplyr::collect() |>
    dplyr::summarize(
      revenue = sum(revenue),
      cost_of_goods_sold = sum(cost_of_goods_sold),
      inventory_cost_of_goods_sold = sum(inventory_cost_of_goods_sold),
      profit = sum(profit),
      order_cost = sum(order_cost),
      inventory_cost = sum(inventory_cost),
      cashflow = revenue - order_cost - inventory_cost,
      .groups = "drop"
    )
  
  write_table(
    dataset = profit_and_loss_by_date,
    table = "profit_and_loss_by_date",
    path = path
  )
  
  if (!verbose) {
    return(invisible(profit_and_loss_by_date))
  }
  
  return(profit_and_loss_by_date)
}