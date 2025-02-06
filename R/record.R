#' Write tables that record profit and loss for the current date
#' 
#' @export
record_profit_and_loss <- function(current_date, path, verbose = FALSE) {
  date_yesterday <- current_date - 1
  products <- open_table("products", path = path)
  
  sales_revenue <- open_table("sales", path = path) |>
    dplyr::filter(date == current_date) |>
    dplyr::select(sku, date, sales) |>
    dplyr::inner_join(
      dplyr::select(products, sku, date, sales_price, purchase_price),
      by = c("sku", "date")
    ) |>
    dplyr::mutate(
      revenue = sales_price * sales
    ) |>
    dplyr::select(sku, date, sales, revenue)
  
  # For a first-in-first-out accounting of the profit and loss, identify for
  # each product from which batch it was sold. Based on the batch date, ...
  # 1) Calculate how long the product was on stock and thus how large the total
  #    holding costs over the period were.
  # 2) Track back to the order through which the material was added to the
  #    inventory to identify the order costs of the product including the
  #    purchase price at which it was ordered.
  
  inventory <- open_table("inventory", path = path)
  
  inventory_yesterday <- inventory |>
    dplyr::filter(date == date_yesterday) |>
    dplyr::select(-date) |>
    dplyr::rename(inventory_before = inventory)
  
  inventory_today <- inventory |>
    dplyr::filter(date == current_date)
  
  inventory_sold_today <- inventory_yesterday |>
    dplyr::full_join(inventory_today, by = c("sku", "batch")) |>
    dplyr::mutate(inventory = dplyr::coalesce(inventory, 0)) |>
    # products sold today based on inventory that came in today didn't cost
    dplyr::filter(!is.na(inventory_before)) |>
    dplyr::mutate(sold_from_inventory = inventory_before - inventory) |>
    dplyr::filter(sold_from_inventory > 0) |>
    dplyr::select(sku, batch, date, sold_from_inventory)
  
  cost_of_goods_sold <- inventory_sold_today |>
    dplyr::inner_join(
      dplyr::select(products, sku, date, stock_cost),
      by = c(sku = "sku", batch = "date")
    ) |>
    dplyr::inner_join(
      dplyr::select(
        open_table("orders", path = path),
        sku, date_received, quantity, fixed_order_cost, purchase_price),
      by = c(sku = "sku", batch = "date_received")
    ) |>
    dplyr::collect() |>
    dplyr::mutate(
      days_on_stock = as.numeric(date - batch),
      cogs_component_holding = sold_from_inventory * days_on_stock * stock_cost,
      cogs_component_order = fixed_order_cost / quantity * sold_from_inventory,
      cogs_component_purchase = purchase_price * sold_from_inventory
    ) |>
    dplyr::group_by(sku, date) |>
    dplyr::summarize(
      cogs_component_holding = sum(cogs_component_holding),
      cogs_component_order = sum(cogs_component_order),
      cogs_component_purchase = sum(cogs_component_purchase),
      .groups = "drop"
    ) |>
    dplyr::mutate(
      cost_of_goods_sold = cogs_component_purchase +
        cogs_component_order +
        cogs_component_holding
    )
  
  cashflow_inventory <- open_table("inventory", path = path) |>
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
  
  cashflow_orders <- open_table("orders", path = path) |>
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
  
  profit_and_loss_by_sku_and_date <- products |>
    dplyr::filter(date == current_date) |>
    dplyr::select(sku, date) |>
    dplyr::left_join(sales_revenue, by = c("sku", "date")) |>
    dplyr::left_join(cost_of_goods_sold, by = c("sku", "date")) |>
    dplyr::left_join(cashflow_inventory, by = c("sku", "date")) |>
    dplyr::left_join(cashflow_orders, by = c("sku", "date")) |>
    dplyr::mutate(
      sales = dplyr::coalesce(sales, 0),
      revenue = dplyr::coalesce(revenue, 0),
      cost_of_goods_sold = dplyr::coalesce(cost_of_goods_sold, 0),
      cogs_component_purchase = dplyr::coalesce(cogs_component_purchase, 0),
      cogs_component_order = dplyr::coalesce(cogs_component_order, 0),
      cogs_component_holding = dplyr::coalesce(cogs_component_holding, 0),
      profit = revenue - cost_of_goods_sold,
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
      cogs_component_purchase = sum(cogs_component_purchase),
      cogs_component_order = sum(cogs_component_order),
      cogs_component_holding = sum(cogs_component_holding),
      profit = sum(profit),
      order_cost = sum(order_cost),
      inventory_cost = sum(inventory_cost),
      cashflow = sum(cashflow),
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