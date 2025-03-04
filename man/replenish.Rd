% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/replenish.R
\name{replenish}
\alias{replenish}
\title{Submit orders to replenish materials}
\usage{
replenish(order, current_date, path)
}
\arguments{
\item{order}{A data frame with columns \code{sku} and \code{quantity}, one row for each
product that should be ordered If no products should be ordered, provide
a zero-row data frame.}

\item{current_date}{The current date of the simulation's state}

\item{path}{Path to the directory in which the gym's state is stored}
}
\value{
TRUE, invisibly
}
\description{
This function updates the \code{orders} table as a side effect.
}
