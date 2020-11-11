# Title     : To run s1_gather_tile in terminal
# Objective : To make multiprocess easier
# Created by: Lei Song
# Created on: 10/29/20
PkgNames <- c("optparse", "here")
invisible(suppressMessages(suppressWarnings(
  lapply(PkgNames, require, character.only = T))))

# Read command inputs
option_list <- list(
  make_option(c("-i", "--tile_id"),
              type = "integer", default = NULL,
              help = "tile id", metavar="number"),
  make_option(c("-d", "--dirname"),
              type = "character", default = '.',
              help = "the dirname of project"),
  make_option(c("-n", "--num_threads"),
              type = "integer", default = 1,
              help = "the number of threads"),
  make_option(c("-c", "--config_path"),
              type = "character", default = "cfgs/config_main.yaml",
              help = "the path of config file [default= %default]"))

parms <- parse_args(OptionParser(option_list = option_list))
source(file.path(parms$dirname, 's1_gather_tile.R'))
gather_image(parms$tile_id, parms$config_path, parms$num_threads)
