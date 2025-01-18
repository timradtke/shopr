forecast <- function(current_date, future_end_date, path) {
  if (current_date == future_end_date) {
    return(data.frame(
      path = integer(),
      date = as.Date(numeric()),
      sku = character(),
      prediction = numeric()
    ))
  }
  
  sales_train <- open_table("sales", path = path) |>
    dplyr::filter(date <= current_date) |>
    dplyr::collect() |>
    dplyr::group_by(sku) |>
    dplyr::arrange(date) |>
    dplyr::mutate(sales_lag7 = dplyr::lag(sales, n = 7))
  
  sales_train_sd <- sales_train |>
    dplyr::group_by(sku) |>
    dplyr::summarize(
      sd_fitted = stats::sd(sales_lag7, na.rm = TRUE),
      .groups = "drop"
    )
  
  model <- sales_train |>
    dplyr::group_by(sku) |>
    dplyr::slice_tail(n = 7) |>
    dplyr::ungroup() |>
    dplyr::mutate(weekday = lubridate::wday(date, week_start = 1)) |>
    dplyr::left_join(sales_train_sd, by = "sku") |>
    dplyr::select(sku, weekday, mean_fitted = sales, sd_fitted)
  
  forecast <- data.frame(
    date = seq(current_date + lubridate::days(1), future_end_date, by = "day")
  ) |>
    dplyr::mutate(weekday = lubridate::wday(date, week_start = 1)) |>
    dplyr::inner_join(model, by = "weekday", relationship = "many-to-many") |>
    dplyr::group_by(sku) |>
    dplyr::mutate(
      mean_fitted_cumulative = cumsum(mean_fitted),
      prediction = mean_fitted_cumulative,
      path = 1
    ) |>
    dplyr::ungroup() |>
    dplyr::select(path, date, sku, prediction)
  
  return(forecast)
}

record_forecast <- function(forecast, current_date, path) {
  forecast |>
    dplyr::mutate(train_end = current_date) |>
    dplyr::select(train_end, path, date, sku, prediction) |>
    write_table(table = "forecast", path = path)
  
  return(invisible(TRUE))
}

make_order <- function(forecast, current_date, path) {
  if (NROW(forecast) == 0L) {
    return(data.frame(
      sku = character(),
      quantity = numeric()
    ))
  }
  
  outstanding_orders <- open_table("orders", path = path) |>
    dplyr::filter(date_received > current_date) |>
    # to ignore already created orders during replay
    dplyr::filter(date_ordered < current_date) |>
    dplyr::group_by(sku, date_received) |>
    dplyr::summarize(inventory_incoming = sum(quantity), .groups = "drop") |>
    dplyr::collect()
  
  current_inventory <- open_table("inventory", path = path) |>
    dplyr::filter(date == current_date) |>
    dplyr::group_by(sku) |>
    dplyr::summarize(inventory = sum(inventory)) |>
    dplyr::collect()
  
  predictions_with_delivery_information <- forecast |>
    dplyr::mutate(n_days_ahead = as.integer(date - current_date)) |>
    dplyr::inner_join(
      open_table("products", path = path) |>
        dplyr::filter(date == current_date) |>
        dplyr::mutate(
          order_threshold_quantile = 
            # 0.001 -> at least 1 in 1000 paths predicts out-of-stock
            pmax(0.001, stock_cost / (sales_price - purchase_price)),
          order_threshold_quantile = ifelse(
            is.infinite(order_threshold_quantile) |
              is.na(order_threshold_quantile),
            1,
            order_threshold_quantile
          )
        ) |>
        dplyr::select(
          sku, delivery_days, minimum_order_quantity, order_threshold_quantile
        ) |>
        dplyr::collect(),
      by = "sku"
    ) |>
    dplyr::left_join(current_inventory, by = "sku") |>
    dplyr::left_join(
      outstanding_orders, by = c("sku", date = "date_received")
    ) |>
    dplyr::group_by(path, sku) |>
    dplyr::mutate(
      inventory_incoming = dplyr::coalesce(inventory_incoming, 0),
      inventory = dplyr::coalesce(inventory, 0),
      inventory = inventory + cumsum(inventory_incoming),
      inventory_over_time = inventory - prediction
    ) |>
    dplyr::ungroup()
  
  materials_to_replenish <- predictions_with_delivery_information |>
    dplyr::filter(n_days_ahead == delivery_days) |>
    dplyr::mutate(would_be_oos_on_delivery_day = inventory_over_time <= 0) |>
    dplyr::group_by(sku, date) |>
    dplyr::summarize(
      needs_to_be_replenished =
        mean(would_be_oos_on_delivery_day) > order_threshold_quantile,
      .groups = "drop"
    ) |>
    dplyr::filter(needs_to_be_replenished) |>
    dplyr::select(sku, delivery_arrival = date)
  
  order <- predictions_with_delivery_information |>
    dplyr::inner_join(materials_to_replenish, by = "sku") |>
    # order the expected quantity needed for the following two weeks
    dplyr::filter(
      date >= delivery_arrival,
      date < delivery_arrival + lubridate::days(14)
    ) |>
    dplyr::group_by(path, sku) |>
    # if the material has to be ordered, this date should have a negative
    # cumulative inventory value in `inventory_over_time` that indicates
    # how much inventory is needed if ordered today
    dplyr::slice_max(order_by = date) |>
    dplyr::mutate(missed_inventory = -1 * pmin(inventory_over_time, 0)) |>
    dplyr::group_by(sku, minimum_order_quantity) |> # aggregate out `path`
    dplyr::summarize(quantity = mean(missed_inventory), .groups = "drop") |>
    # avoid to order SKUs for which the expected quantity is too small (or zero)
    # As long as MOQ is equal to 1, this is harmless; could lead to OOS if MOQ
    # was higher and expected quantity just slightly below it.
    dplyr::filter(quantity >= minimum_order_quantity) |>
    dplyr::select(sku, quantity)
  
  return(order)
}
