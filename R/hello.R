library(dplyr)
library(jsonlite)
library(rvest)
library(sf)
library(httr)
library(lubridate)
library(purrr)

sf_use_s2(F)

curr_time <- Sys.time()

datestamp <- curr_time %>% with_tz("US/Central") %>%
  format("%Y-%m-%d_%H_%M_%S")

all_dates <- readRDS("all_dates.rds")
all_dates <- c(all_dates, curr_time)
saveRDS(all_dates, file = "all_dates.rds")


old_shark <- readRDS("alive_sharks.rds")

alive_sharks <- st_read("https://www.mapotic.com/api/v1/maps/3413/pois.geojson/?format=json&h=10") %>%
  mutate(time = curr_time)

new_shark <- alive_sharks %>%
  as.data.frame(stringsAsFactors = F) %>%
  select(id, new_date = last_update)


saveRDS(alive_sharks, file = paste0("./data/allsharks_", datestamp, ".rds"))
saveRDS(alive_sharks, file = paste0("alive_sharks.rds"))

ids_new_data <- old_shark %>%
  left_join(new_shark, by = c("id" = "id")) %>%
  filter(last_update != new_date) %>%
  pull(id)

shark_motion_func <- function(record) {
  df <- record %>% as.data.frame(stringsAsFactors = F)
  colnames(df) <- c("dt_move", "point.type", "coord.x", "coord.y", "import_id")
  return(df)
}

#only do this stuff if some ids appear to have changed locations
if(length(ids_new_data) != 0) {
  new_sharks_motion <- map_dfr(.x = ids_new_data, .f = function(x) {
    beg <- Sys.time()
    url <- paste0("https://www.mapotic.com/api/v1/maps/3413/pois/", x , "/motion/with-meta/?format=json&h=30")

    res <- GET(url)

    if(status_code(res) != 200L) {
      return(NULL)
    }


    sing_shark_df <- content(res, "parsed") %>%
      #read_json(url)
      .$motion %>%
      map_dfr(.f = function(y){
        #func_beg <- Sys.time()
        df <- y %>% as.data.frame(stringsAsFactors = F)
        colnames(df) <- c("dt_move", "point.type", "coord.x", "coord.y", "import_id")
        #func_end <- Sys.time()

        #print(paste0(y, " --------- ", as.numeric(func_end - func_beg)))

        return(df)
      }) %>%
      mutate(id = x)
    end <- Sys.time()

    print(paste0(x, " ----------- ", as.numeric(end - beg), " ----- motion"))

    return(sing_shark_df)
  })

  saveRDS(new_sharks_motion, file = paste0("./data/new_shark_motion_", datestamp, ".rds"))

  live_sharks_motion <- readRDS("live_sharks_motion.rds")

  old_motions <- paste0(live_sharks_motion$dt_move, live_sharks_motion$coord.x, live_sharks_motion$coord.y, live_sharks_motion$id)

  only_new_points <- new_sharks_motion %>%
    mutate(identifier = paste0(dt_move, coord.x, coord.y, id)) %>%
    filter(!identifier %in% old_motions) %>%
    select(-identifier)
  ######only do this stuff if there are ACTUALLY new points
  if(nrow(only_new_points) != 0) {
    live_sharks_motion <- bind_rows(live_sharks_motion, only_new_points) %>%
      mutate(time = case_when(
        is.na(time) ~ curr_time,
        TRUE ~ time
      ))

    saveRDS(live_sharks_motion, "live_sharks_motion.rds")


    live_sharks_motion_line <- live_sharks_motion %>%
      st_as_sf(coords = c(lat = "coord.x", long = "coord.y")) %>%
      group_by(id) %>%
      mutate(geometry_lagged = lag(geometry, order_by = dt_move),
             time_lag = Sys.time() - ymd_hms(dt_move),
             time_lag_num = as.numeric(time_lag)/60/60/24)%>%
      slice(-1) %>%
      mutate(
        line = st_sfc(purrr::map2(
          .x = geometry,
          .y = geometry_lagged,
          .f = ~{st_union(c(.x, .y)) %>% st_cast("LINESTRING")}
        )), time = curr_time)

    st_geometry(live_sharks_motion_line) <- 'line'
    st_crs(live_sharks_motion_line) <- 4326

    saveRDS(live_sharks_motion_line, file = "live_sharks_motion_line.rds")
  }


}
#live_sharks_motion





new_shark_comments <- map_dfr(.x = alive_sharks$id, .f = function(x) {

  beg <- Sys.time()
  url <- paste0("https://www.mapotic.com/api/v1/maps/3413/public-pois/", x , "/?format=json&h=30")
  res <- GET(url)

  if(status_code(res) != 200L) {
    return(NULL)
  }

  sing_shark_df <- content(res, "parsed") %>%
    #read_json(url)
    .$comments %>%
    map_dfr(.f = unlist) %>%
    mutate(id = x)
  end <- Sys.time()

  print(paste0(x, " ----------- ", as.numeric(end - beg), " ----- comments"))

  return(sing_shark_df)

})
shark_comments <- readRDS("shark_comments.rds")



old_comments <- paste0(shark_comments$id, shark_comments$text, shark_comments$created)

only_new_comments <- new_shark_comments %>%
  mutate(identifier = paste0(id, text, created)) %>%
  filter(!identifier %in% old_comments) %>%
  select(-identifier)


if(nrow(only_new_comments != 0)) {
  saveRDS(only_new_comments, file = paste0("./data/newcomments_", datestamp, ".rds"))

  shark_comments <- bind_rows(shark_comments, only_new_comments) %>%
    mutate(time = case_when(
      is.na(time) ~ curr_time,
      TRUE ~ time
    ))


  saveRDS(shark_comments, "shark_comments.rds")
}

#
# moving_sharks_most_recent <- live_sharks_motion %>%
#   filter(id %in% moving_sharks) %>%
#   group_by(id) %>%
#   filter(dt_move == max(dt_move)) %>%
#   mutate(time_lag = Sys.time() - ymd_hms(dt_move),
#          time_lag_num = as.numeric(time_lag)/60/60/24) %>%
#   st_as_sf(coords = c(lat = "coord.x", long = "coord.y"), crs = 4326)
