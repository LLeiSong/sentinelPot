##############HEADER####################
# This is a script to parse and clip all
# imagery for each tile.
# CAUTION: since terra package is updating
# dramatically, some functions might stop
# working with a newer terra package
# Current one is: 0.8.6
# Author: Lei Song
########################################
split_catalog <- function(config_dir){
        # Load packages
        PkgNames <- c("sf", "dplyr","yaml")
        invisible(suppressMessages(suppressWarnings(
                lapply(PkgNames, require, character.only = T))))
        options(warn = -1)
        
        config <- yaml.load_file(config_dir)
        s1_footprints <- st_read(config$sentinel$catalog_json, quiet = T)
        s1_footprints_unique <- s1_footprints %>% 
                dplyr::select(-no_geom) %>% unique()
        s1_look_table <- s1_footprints %>% dplyr::select(c(id, no_geom)) %>% 
                st_drop_geometry() %>% na.omit() %>% 
                mutate(no_geom = no_geom + 1)
        file.rename(config$sentinel$catalog_json, 
                    file.path(dirname(config$sentinel$catalog_json),
                              paste0('raw_', basename(config$sentinel$catalog_json))))
        st_write(s1_footprints_unique, config$sentinel$catalog_json, quiet = T)
        write.csv(s1_look_table, config$sentinel$search_table, 
                  row.names = F)
}

gather_image <- function(tile_no,
                         config_dir,
                         num_threads = 1){
        # Load packages
        PkgNames <- c("sf", "dplyr","yaml", "log4r", "glue", "terra", 
                      "parallel", "stringr", "rmapshaper")
        invisible(suppressMessages(suppressWarnings(
                lapply(PkgNames, require, character.only = T))))
        options(warn = -1)
        
        # Read config
        config <- yaml.load_file(config_dir)
        dst_dir <- config$dirs$dst_dir
        log_dir <- file.path(dst_dir, 'logs')
        if (!dir.exists(log_dir)) dir.create(log_dir, showWarnings = F)
        local_path <- file.path(dst_dir, config$dirs$processed_path)
        
        tiles <- st_read(config$sentinel$geojson, quiet = T) %>% 
                dplyr::select(tile)
        s1_footprints <- st_read(config$sentinel$catalog_json,
                                 quiet = T)
        s1_search_table <- read.csv(config$sentinel$search_table,
                                    stringsAsFactors = F)
        gcs_res <- config$harmonic$gcs_rec
        
        # logging
        dt <- format(Sys.time(), '%d%m%Y_%H%M')
        logger <- create.logger(logfile = file.path(log_dir, 
                                                    glue('s1_gather_tile_{dt}.log')), 
                                level = "DEBUG")
        
        # Hard setting
        ids <- s1_search_table %>% 
                filter(no_geom == which(tiles$tile == tile_no))
        ftps_sub <- s1_footprints %>% 
                filter(id %in% ids$id)
        dates <- sapply(ftps_sub$title, function(title_each){
                gsub("T", "", str_extract(title_each, "[0-9]+T"))})
        tile_id <- tiles %>% slice(which(tiles$tile == tile_no)) %>% pull(tile)
        vv_name <- "Sigma0_VV_db.img"
        vv_hdr <- gsub("img", "hdr", vv_name)
        vh_name <- "Sigma0_VH_db.img"
        vh_hdr <- gsub("img", "hdr", vh_name)
        temp_path <- config$dirs$dir_clip
        temp_path <- glue("{temp_path}_{tile_id}")
        if (!dir.exists(temp_path)) dir.create(temp_path, showWarnings = F)
        
        # Collect garbage
        rm(ids, s1_footprints, s1_search_table); gc()
        
        # Process
        if (length(unique(ftps_sub$title)) == length(unique(dates))){
                info(logger, "No imagery come from the same day.")
                info(logger, "Start to clip the imagery.")
                title_list <- unique(ftps_sub$title)
                suppressMessages(bry_raw <- tiles %>% filter(tile == tile_id) %>%
                        st_buffer(gcs_res * 100) %>% vect())
                
                cores <- min(length(title_list), num_threads)
                clip <- mclapply(title_list, function(title_each){
                        if (is.null(local_path)){
                                stop("No path for local imagery!")}
                        # VV
                        rst <- rast(glue("{local_path}/{title_each}.data/{vv_name}"))
                        rst_crop <- extend(crop(rst, bry_raw), ext(bry_raw))
                        rst_crop[is.na(rst_crop)] <- 0
                        
                        ######## Remove artifacts across boundary ########
                        vals <- as.matrix(rst_crop)
                        bg_ratio <- sum(vals == 0) / length(vals)
                        if (bg_ratio < 0.99) {
                                if (bg_ratio > 0.01){ # condition that image has bg
                                        info(logger, glue("Remove artifacts on boundary for {title_each}."))
                                        msk <- (rst_crop == 0 | rst_crop < (-30))
                                        msk <- as.polygons(msk); names(msk) <- 'val'
                                        tmp <- tempfile()
                                        writeVector(msk, tmp)
                                        # Move the accident islands from real 0 values
                                        pol <- st_read(tmp, quiet = T) %>% 
                                                filter(val == 0) %>% st_set_crs(4326)
                                        # pol <- st_cast(pol, 'POINT') %>%
                                        #         st_coordinates() %>% data.frame() %>%
                                        #         filter(X == st_bbox(pol)[1] | X == st_bbox(pol)[3] |
                                        #                        Y == st_bbox(pol)[2] | Y == st_bbox(pol)[4]) %>%
                                        #         st_as_sf(coords = c('X', 'Y'), crs = 4326) %>%
                                        #         st_combine() %>% st_cast("POLYGON") %>%
                                        #         st_buffer(-gcs_res * 30) %>% st_as_sf()
                                        pol <- st_cast(pol, 'POLYGON') %>% 
                                                mutate(area = st_area(.)) %>% 
                                                arrange(-area) %>% slice(1)  
                                        suppressMessages(pol <- st_multipolygon(lapply(st_geometry(pol), 
                                                                                       function(x) x[1])) %>% 
                                                                 st_buffer(-gcs_res * 60) %>% 
                                                                 st_geometry() %>% st_sf(pol) %>% vect())
                                        crs(pol) <- crs(bry_raw)
                                        try(rst_crop <- mask(rst_crop, pol), silent = T)
                                        suppressMessages(bry <- tiles %>% filter(tile == tile_id) %>% 
                                                                 st_buffer(gcs_res * 12) %>% vect())
                                        try(rst_crop <- crop(rst_crop, bry), silent = T)
                                        
                                        # Collect garbage
                                        rm(pol, bry, msk, tmp); gc()
                                } else{
                                        suppressMessages(bry <- tiles %>% filter(tile == tile_id) %>% 
                                                                 st_buffer(gcs_res * 12) %>% vect())
                                        try(rst_crop <- crop(rst, bry), silent = T); rm(bry)
                                }; rm(vals, bg_ratio, rst); gc()
                                
                                ##################################################
                                writeRaster(rst_crop, glue("{temp_path}/{title_each}_{vv_name}"), 
                                            wopt = list(filetype = "ENVI"), 
                                            overwrite = TRUE)
                        }
                        
                        
                        # VH
                        rst <- rast(glue("{local_path}/{title_each}.data/{vh_name}"))
                        rst_crop <- extend(crop(rst, bry_raw), ext(bry_raw))
                        rst_crop[is.na(rst_crop)] <- 0
                        
                        ######## Remove artifacts across boundary ########
                        vals <- as.matrix(rst_crop)
                        bg_ratio <- sum(vals == 0)/length(vals)
                        if (bg_ratio < 0.99) {
                                if (bg_ratio > 0.01){ # condition that image has bg
                                        info(logger, glue("Remove artifacts on boundary for {title_each}."))
                                        msk <- (rst_crop == 0 | rst_crop < (-30))
                                        msk <- as.polygons(msk); names(msk) <- 'val'
                                        tmp <- tempfile()
                                        writeVector(msk, tmp)
                                        pol <- st_read(tmp, quiet = T) %>% 
                                                filter(val == 0) %>% st_set_crs(4326)
                                        # pol <- st_cast(pol, 'POINT') %>%
                                        #         st_coordinates() %>% data.frame() %>%
                                        #         filter(X == st_bbox(pol)[1] | X == st_bbox(pol)[3] |
                                        #                        Y == st_bbox(pol)[2] | Y == st_bbox(pol)[4]) %>%
                                        #         st_as_sf(coords = c('X', 'Y'), crs = 4326) %>%
                                        #         st_combine() %>% st_cast("POLYGON") %>%
                                        #         st_buffer(-gcs_res * 30) %>% st_as_sf()
                                        pol <- st_cast(pol, 'POLYGON') %>% 
                                                mutate(area = st_area(.)) %>% 
                                                arrange(-area) %>% slice(1)  
                                        suppressMessages(pol <- st_multipolygon(lapply(st_geometry(pol), 
                                                                                       function(x) x[1])) %>% 
                                                                 st_buffer(-gcs_res * 60) %>% 
                                                                 st_geometry() %>% st_sf(pol) %>% vect())
                                        crs(pol) <- crs(bry_raw)
                                        try(rst_crop <- mask(rst_crop, pol), silent = T)
                                        suppressMessages(bry <- tiles %>% filter(tile == tile_id) %>% 
                                                                 st_buffer(gcs_res * 12) %>% vect())
                                        try(rst_crop <- crop(rst_crop, bry), silent = T)
                                        
                                        # Collect garbage
                                        rm(pol, bry, msk, tmp); gc()
                                } else{
                                        suppressMessages(bry <- tiles %>% filter(tile == tile_id) %>% 
                                                                 st_buffer(gcs_res * 12) %>% vect())
                                        try(rst_crop <- crop(rst, bry), silent = T); rm(bry)
                                }; rm(vals, bg_ratio, rst); gc()
                                ##################################################
                                writeRaster(rst_crop, glue("{temp_path}/{title_each}_{vh_name}"), 
                                            wopt = list(filetype = "ENVI"), 
                                            overwrite = TRUE)
                        }
                        info(logger, sprintf("Finish imagery %s.", title_each))
                }, mc.cores = cores) %>% unlist(); rm(bry_raw)
        } else {
                info(logger, "There are imagery from the same day.")
                info(logger, "Switch to use date to mosaic imagery first.")
                title_list <- unique(ftps_sub$title)
                dates <- unique(dates)
                suppressMessages(bry_terra <- tiles %>% filter(tile == tile_id) %>%
                        st_buffer(gcs_res * 100) %>% vect())
                
                cores <- min(length(dates), num_threads)
                clip <- mclapply(dates, function(date_each){
                        titles <- title_list[grep(date_each, title_list)]
                        # VV
                        if (is.null(local_path)){
                                stop("No path for local imagery!")
                        }
                        rst <- lapply(titles, function(title_each){
                                rst <- rast(glue("{local_path}/{title_each}.data/{vv_name}"))
                                rst <- extend(crop(rst, bry_terra), ext(bry_terra))
                                rst[is.na(rst)] <- 0
                                rst
                        })
                        rst_crop_stack <- do.call(c, rst)
                        rst_crop <- app(rst_crop_stack, fun = min, na.rm = T)
                        rst_fill <- focal(rst_crop, w = 3, fun = min)
                        rst_crop <- cover(rst_crop, rst_fill, value = 0)
                        rm(rst_crop_stack, rst, rst_fill); gc()
                        
                        ######## Remove artifacts across boundary ########
                        vals <- as.matrix(rst_crop)
                        bg_ratio <- sum(vals==0)/length(vals)
                        if (bg_ratio < 0.99) {
                                if (bg_ratio > 0.01){ # condition that image has bg
                                        info(logger, glue("Remove artifacts on boundary for {date_each}."))
                                        msk <- (rst_crop == 0 | rst_crop < (-30))
                                        msk <- as.polygons(msk)
                                        tmp <- tempfile()
                                        writeVector(msk, tmp)
                                        # Move the accident island from real 0 values
                                        pol <- st_read(tmp, quiet = T) %>% 
                                                filter(min == 0) %>% st_set_crs(4326)
                                        # pol <- st_cast(pol, 'POINT') %>%
                                        #         st_coordinates() %>% data.frame() %>%
                                        #         filter(X == st_bbox(pol)[1] | X == st_bbox(pol)[3] |
                                        #                        Y == st_bbox(pol)[2] | Y == st_bbox(pol)[4]) %>%
                                        #         st_as_sf(coords = c('X', 'Y'), crs = 4326) %>%
                                        #         st_combine() %>% st_cast("POLYGON") %>%
                                        #         st_buffer(-gcs_res * 30) %>% st_as_sf()
                                        pol <- st_cast(pol, 'POLYGON') %>% 
                                                mutate(area = st_area(.)) %>% 
                                                arrange(-area) %>% slice(1)  
                                        suppressMessages(pol <- st_multipolygon(lapply(st_geometry(pol), 
                                                                                       function(x) x[1])) %>% 
                                                                 st_buffer(-gcs_res * 60) %>% 
                                                                 st_geometry() %>% st_sf(pol) %>% vect())
                                        crs(pol) <- crs(bry_terra)
                                        try(rst_crop <- mask(rst_crop, pol), silent = T)
                                        suppressMessages(bry <- tiles %>% filter(tile == tile_id) %>% 
                                                                 st_buffer(gcs_res * 12) %>% vect())
                                        try(rst_crop <- crop(rst_crop, bry), silent = T)
                                        rm(msk, tmp, pol, bry); gc()
                                } else{
                                        suppressMessages(bry <- tiles %>% filter(tile == tile_id) %>% 
                                                                 st_buffer(gcs_res * 12) %>% vect())
                                        try(rst_crop <- crop(rst_crop, bry), silent = T); rm(bry)
                                }; rm(vals, bg_ratio)
                                ##################################################
                                opt_name <- titles[1]
                                writeRaster(rst_crop, glue("{temp_path}/{opt_name}_{vv_name}"), 
                                            wopt = list(filetype = "ENVI"), 
                                            overwrite = TRUE)
                        }
                        
                        # VH
                        if (is.null(local_path)){
                                stop("No path for local imagery!")
                        }
                        rst <- lapply(titles, function(title_each){
                                rst <- rast(glue("{local_path}/{title_each}.data/{vh_name}"))
                                rst <- extend(crop(rst, bry_terra), ext(bry_terra))
                                rst[is.na(rst)] <- 0
                                rst
                        })
                        rst_crop_stack <- do.call(c, rst)
                        rst_crop <- app(rst_crop_stack, fun = min, na.rm = T)
                        rst_fill <- focal(rst_crop, w = 3, fun = min)
                        rst_crop <- cover(rst_crop, rst_fill, value = 0)
                        rm(rst_crop_stack, rst, rst_fill); gc()
                        
                        ######## Remove artifacts across boundary ########
                        vals <- as.matrix(rst_crop)
                        bg_ratio <- sum(vals==0)/length(vals)
                        if (bg_ratio < 0.99) {
                                if (bg_ratio > 0.01){ # condition that image has bg
                                        info(logger, glue("Remove artifacts on boundary for {date_each}."))
                                        msk <- (rst_crop == 0 | rst_crop < (-30))
                                        msk <- as.polygons(msk)
                                        tmp <- tempfile()
                                        writeVector(msk, tmp)
                                        pol <- st_read(tmp, quiet = T) %>% 
                                                filter(min == 0) %>% st_set_crs(4326)
                                        # pol <- st_cast(pol, 'POINT') %>%
                                        #         st_coordinates() %>% data.frame() %>%
                                        #         filter(X == st_bbox(pol)[1] | X == st_bbox(pol)[3] |
                                        #                        Y == st_bbox(pol)[2] | Y == st_bbox(pol)[4]) %>%
                                        #         st_as_sf(coords = c('X', 'Y'), crs = 4326) %>%
                                        #         st_combine() %>% st_cast("POLYGON") %>%
                                        #         st_buffer(-gcs_res * 30) %>% st_as_sf()
                                        pol <- st_cast(pol, 'POLYGON') %>% 
                                                mutate(area = st_area(.)) %>% 
                                                arrange(-area) %>% slice(1)  
                                        suppressMessages(pol <- st_multipolygon(lapply(st_geometry(pol), 
                                                                                       function(x) x[1])) %>% 
                                                                 st_buffer(-gcs_res * 60) %>% 
                                                                 st_geometry() %>% st_sf(pol) %>% vect())
                                        crs(pol) <- crs(bry_terra)
                                        try(rst_crop <- mask(rst_crop, pol), silent = T)
                                        suppressMessages(bry <- tiles %>% filter(tile == tile_id) %>% 
                                                                 st_buffer(gcs_res * 12) %>% vect())
                                        rst_crop <- crop(rst_crop, bry)
                                        rm(msk, tmp, pol, bry); gc()
                                } else{
                                        suppressMessages(bry <- tiles %>% filter(tile == tile_id) %>% 
                                                                 st_buffer(gcs_res * 12) %>% vect())
                                        try(rst_crop <- crop(rst_crop, bry), silent = T); rm(bry)
                                }; rm(vals, bg_ratio); gc()
                                ##################################################
                                opt_name <- titles[1]
                                writeRaster(rst_crop, glue("{temp_path}/{opt_name}_{vh_name}"), 
                                            wopt = list(filetype = "ENVI"), 
                                            overwrite = TRUE)
                        }
                        info(logger, sprintf("Finish imagery for %s.", date_each))
                }, mc.cores = cores) %>% unlist()
        }
}

