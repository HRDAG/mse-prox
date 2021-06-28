#
# Authors:     MG
# Maintainers: MG, PB
# Copyright:   2021, HRDAG, GPL v2 or later
# =========================================
# co-mse-prox/src/estimate.R

pacman::p_load(argparse, readr, dplyr, LCMCR, logger, tibble, stringr, fs, rjson)

parser <- ArgumentParser()
parser$add_argument("sha", default = "868266fd9664fe2820e4507ea6d119d93a64202c", nargs = '*')
args <- parser$parse_args()
args$input <- paste0("input/", args$sha, ".json")
args$output <- paste0("output/", args$sha, ".json")


# --- functions -----

do_lcmcr <- function(args) {

    summary_table <- fromJSON(file = args$input) %>%
        as_tibble(.) %>%
        mutate(across(everything(), as.integer))

    # NB: LCMCR fails if any of the in_* columns have only one factor level. a
    #     column of all 1s is a valid input, but a column of all 0s is not;
    #     test that there are no columns with 0 records in them, then set _all_
    #     of the lists to be factors with 2 levels.
    stopifnot(all(colSums(summary_table %>% select(starts_with("in_"))) > 0))

    summary_table <- summary_table %>%
        mutate(across(starts_with("in_"), ~factor(.x, levels = c(0, 1)))) %>%
        mutate(Freq = as.integer(Freq)) %>%
        as.data.frame(.)

    # parameters
    K <- min((2 ** (ncol(summary_table) - 1)) - 1, 15)
    n_samples <- 10000

    options(warn = -1)
    sampler <- lcmCR(captures = summary_table,
                     K = K,
                     tabular = TRUE,
                     seed = 19481210,
                     buffer_size = 10000,
                     thinning = 1000,
                     in_list_label = "1",
                     not_in_list_label = "0",
                     verbose = FALSE)

    N <- lcmCR_PostSampl(sampler,
                         burnin = 10000,
                         samples = n_samples,
                         thinning = 500,
                         output = FALSE)
    options(warn = 0)

    N <- N[seq(1, length(N), n_samples / 1000)] # thin again

    write(toJSON(N), args$output)

}


# ----- main
do_lcmcr(args)

# done.

