#' Initialize tables used to store a simulation's state
#' 
#' A side effect of this function is the creation of most of the tables listed
#' in `list_tables()` in the directory specified with `path`.
#' 
#' @param path Path to the directory in which the gym's state is stored
#' @param current_date The date from which the simulation will pick up
#' @param history_start_date The first date in the historical data, used to
#'     create a sales history so that there is training data for forecast models
#' @param future_end_date The final date in the future for which data will be
#'     created; the simulation can't update further into the future.
#' 
#' @export
initialize <- function(path,
                       current_date,
                       history_start_date,
                       future_end_date) {
  
  dates <- seq(from = history_start_date, to = future_end_date, by = "day")
  
  named_datasets <- list(
    settings = data.frame(
      seed = seed,
      date = current_date,
      history_start_date = history_start_date,
      future_start_date = current_date,
      future_end_date = future_end_date
    ),
    demand = data.frame(
      sku = rep(letters[1:2], each = length(dates)),
      date = rep(dates, times = 2),
      demand = stats::rpois(n = length(dates) * 2, lambda = 6)
    ),
    sales = data.frame(
      sku = rep(letters[1:2], each = length(dates)),
      date = rep(dates, times = 2),
      sales = stats::rpois(n = length(dates) * 2, lambda = 6)
    ) |> dplyr::filter(date <= current_date),
    inventory = data.frame(
      sku = letters[1:2],
      date = current_date,
      batch = current_date,
      inventory = 1.5*7*6
    ),
    orders = data.frame(
      order = "dummy",
      sku = letters[1],
      open = FALSE,
      date_ordered = history_start_date,
      date_received = history_start_date,
      quantity = 0
    ),
    products = data.frame(
      sku = letters[1:2],
      date = rep(dates, each = 2),
      stock_cost = 0.1,
      purchase_price = 1,
      sales_price = 10,
      delivery_days = 7,
      minimum_order_quantity = 1,
      lot_size = 1,
      fixed_order_cost = 0
    ),
    hierarchy = data.frame(
      sku = letters[1:2],
      category_id = LETTERS[1:2]
    )
  )
  
  write_tables(
    named_datasets = named_datasets,
    path = path
  )
  
  return(invisible(TRUE))
}

#' Initialize tables used to store a simulation's state from files
#' 
#' In contrast to `initialize()`, this function creates the tables by taking
#' the required information from three parquet files that provide the necessary
#' information for demand, inventory, and products.
#' 
#' A side effect of this function is the creation of most of the tables listed
#' in `list_tables()` in the directory specified with `path`.
#' 
#' @param path Path to the directory in which the gym's state is stored
#' @param current_date The date from which the simulation will pick up
#' @param history_start_date The first date in the historical data, used to
#'     create a sales history so that there is training data for forecast models
#' @param future_end_date The final date in the future for which data will be
#'     created; the simulation can't update further into the future
#' @param path_to_demand File path to the parquet file used as basis to derive
#'     demand and sales from
#' @param path_to_inventory File path to the parquet file used as basis to
#'     derive inventory from
#' @param path_to_products File path to the parquet file used as basis to derive
#'     product information including prices from
#' 
#' @export
initialize_from_file <- function(path,
                                 current_date,
                                 history_start_date,
                                 future_end_date,
                                 path_to_demand,
                                 path_to_inventory,
                                 path_to_products,
                                 seed = 512) {
  demand <- arrow::read_parquet(file = path_to_demand)
  history_start_date <- pmax(min(demand$date), history_start_date)
  future_end_date <- pmin(max(demand$date), future_end_date)
  
  inventory <- arrow::read_parquet(file = path_to_inventory)
  products <- arrow::read_parquet(file = path_to_products)
  
  named_datasets <- list(
    settings = data.frame(
      seed = seed,
      date = current_date,
      history_start_date = history_start_date,
      future_start_date = current_date,
      future_end_date = future_end_date
    ),
    demand = demand |>
      dplyr::filter(date >= history_start_date, date <= future_end_date),
    sales = demand |>
      dplyr::filter(date <= current_date) |>
      dplyr::rename(sales = demand),
    inventory = inventory |>
      dplyr::filter(
        first_obs_date <= current_date, final_obs_date > current_date
      ) |>
      dplyr::mutate(date = current_date, batch = current_date) |>
      dplyr::select(sku, date, batch, inventory),
    orders = inventory |>
      dplyr::mutate(
        open = first_obs_date > current_date,
        date_ordered = history_start_date,
        date_received = dplyr::if_else(
          first_obs_date > current_date, first_obs_date, history_start_date
        ),
        order = paste0(format(date_ordered, "%Y%m%d"), "/", sku)
      ) |>
      dplyr::select(
        order, sku, open, date_ordered, date_received, quantity = inventory
      ),
    products = products |>
      dplyr::select(
        sku, date,
        stock_cost, purchase_price, sales_price, 
        delivery_days, minimum_order_quantity, lot_size, fixed_order_cost
      ),
    hierarchy = products |>
      dplyr::select(sku, emoji_image, emoji_group, emoji_version,
                    has_skin_tone_support, is_private_label) |>
      dplyr::distinct()
  )
  
  write_tables(
    named_datasets = named_datasets,
    path = path
  )
  
  return(invisible(TRUE))
}
