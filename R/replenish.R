#' Submit orders to replenish materials
#' 
#' This function updates the `orders` table as a side effect.
#' 
#' @param order A data frame with columns `sku` and `quantity`, one row for each
#'     product that should be ordered If no products should be ordered, provide
#'     a zero-row data frame.
#' @param current_date The current date of the simulation's state
#' @param path Path to the directory in which the gym's state is stored
#' 
#' @return TRUE, invisibly
#' 
#' @export
replenish <- function(order,
                      current_date,
                      path) {
  check_order()
  
  create_outstanding_orders(
    order = order,
    current_date = current_date,
    path = path
  )
  
  return(invisible(TRUE))
}

check_order <- function() {
  # TODO
}

create_outstanding_orders <- function(order, current_date, path) {
  orders_to_append <- open_table("products", path = path) |>
    dplyr::filter(date == current_date) |>
    dplyr::select(sku, date, delivery_days) |>
    dplyr::filter(sku %in% order[["sku"]]) |>
    dplyr::collect() |>
    dplyr::mutate(
      order = paste0(format(current_date, "%Y%m%d"), "/", sku),
      open = TRUE,
      date_ordered = current_date,
      date_received = date_ordered + lubridate::days(delivery_days)
    ) |>
    dplyr::inner_join(order[c("sku", "quantity")], by = "sku") |>
    dplyr::select(tidyselect::all_of(names(open_table("orders", path = path))))
  
  open_table("orders", path = path) |>
    dplyr::filter(
      date_received %in% unique(orders_to_append[["date_received"]])
    ) |>
    dplyr::collect() |>
    dplyr::bind_rows(orders_to_append) |>
    write_table("orders", path = path)
  
  return(invisible(TRUE))
}