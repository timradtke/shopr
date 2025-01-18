#' Update the gym's state by one day
#' 
#' This function updates several tables stored at `path` as side effect,
#' including the `settings`, `inventory` and `sales` tables.
#' 
#' @param path Path to the directory in which the gym's state is stored
#' 
#' @return Returns the date of the current iteration invisibly
#' 
#' @export
update <- function(path) {
  updated <- update_current_date(path = path)
  if (!updated) {
    stop("Can't update further as future-end-date was reached.")
  }
  
  current_date <- lookup_current_date(path = path)
  
  update_inventory(current_date = current_date, path = path)
  receive_shipments(current_date = current_date, path = path)
  register_sales(current_date = current_date, path = path)
  
  return(invisible(current_date))
}

update_current_date <- function(path) {
  settings <- open_table(table = "settings", path = path) |>
    dplyr::collect()
  
  if (settings[["date"]] == settings[["future_end_date"]]) {
    return(invisible(FALSE))
  }
  
  settings |>
    dplyr::mutate(date = date + lubridate::days(1)) |>
    write_table(table = "settings", path = path)
  
  return(invisible(TRUE))
}

lookup_current_date <- function(path) {
  open_table(table = "settings", path = path) |>
    dplyr::select(date) |>
    dplyr::collect() |>
    dplyr::pull(date)
}

update_inventory <- function(current_date, path) {
  previous_date <- current_date - lubridate::days(1)

  open_table(table = "inventory", path = path) |>
    dplyr::filter(date == previous_date) |>
    # consider filtering out old batches that have dropped to 0 inventory
    # to not duplicate data too much
    dplyr::filter(inventory != 0) |>
    dplyr::mutate(date = current_date) |>
    write_table(table = "inventory", path = path)
  
  return(invisible(TRUE))
}

receive_shipments <- function(current_date, path) {
  store_inventory(current_date = current_date, path = path)
  close_outstanding_orders(current_date = current_date, path = path)
}

store_inventory <- function(current_date, path) {
  new_inventory <- open_table(table = "orders", path = path) |>
    dplyr::filter(
      date_received == current_date
    ) |>
    dplyr::select(
      sku, date = date_received, batch = date_received, inventory = quantity
    ) |>
    dplyr::collect()
  
  open_table(table = "inventory", path = path) |>
    dplyr::filter(date == current_date) |>
    dplyr::collect() |>
    dplyr::bind_rows(new_inventory) |>
    write_table(table = "inventory", path = path)
  
  return(invisible(TRUE))
}

close_outstanding_orders <- function(current_date, path) {
  open_table(table = "orders", path = path) |>
    dplyr::filter(date_received == current_date) |>
    dplyr::mutate(open = FALSE) |>
    write_table(table = "orders", path = path)
  
  return(invisible(TRUE))
}

register_sales <- function(current_date, path) {
  # Compare demand against inventory; the demand that can be served with the
  # current inventory becomes the observed sales.
  
  demand <- open_table("demand", path = path) |>
    dplyr::filter(date == current_date)
  
  inventory <- open_table("inventory", path = path) |>
    dplyr::filter(date == current_date)
  
  inventory_and_sales <- inventory |>
    # do a full outer join in case no inventory exists
    dplyr::full_join(demand, by = c("sku", "date")) |>
    dplyr::mutate(
      batch = dplyr::coalesce(batch, as.Date(NA_real_)),
      inventory = dplyr::coalesce(inventory, 0),
      demand = dplyr::coalesce(demand, 0)
    ) |>
    # implement first-in-first-out
    dplyr::group_by(sku) |>
    dplyr::arrange(batch) |>
    dplyr::collect() |>
    dplyr::mutate(
      inventory_cumulative = cumsum(inventory),
      inventory_less_demand = pmax(inventory_cumulative - demand, 0),
      inventory_less_demand_lagged =
        dplyr::coalesce(dplyr::lag(inventory_less_demand, n = 1), 0),
      inventory_updated = inventory_less_demand - inventory_less_demand_lagged,
      sales = inventory - inventory_updated,
      inventory = inventory_updated
    ) |>
    dplyr::ungroup()
  
  inventory_and_sales |>
    dplyr::filter(!is.na(batch)) |>
    dplyr::select(tidyselect::all_of(names(inventory))) |>
    write_table(table = "inventory", path = path)
  
  inventory_and_sales |>
    dplyr::group_by(sku, date) |>
    dplyr::summarize(sales = sum(sales), .groups = "drop") |>
    write_table(table = "sales", path = path)
  
  return(invisible(TRUE))
}
