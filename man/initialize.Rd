% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/initialize.R
\name{initialize}
\alias{initialize}
\title{Initialize tables used to store a simulation's state}
\usage{
initialize(path, current_date, history_start_date, future_end_date)
}
\arguments{
\item{path}{Path to the directory in which the gym's state is stored}

\item{current_date}{The date from which the simulation will pick up}

\item{history_start_date}{The first date in the historical data, used to
create a sales history so that there is training data for forecast models}

\item{future_end_date}{The final date in the future for which data will be
created; the simulation can't update further into the future.}
}
\description{
A side effect of this function is the creation of most of the tables listed
in \code{list_tables()} in the directory specified with \code{path}.
}
