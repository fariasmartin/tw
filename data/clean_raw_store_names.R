library(tidyverse)
library(writexl)

data <- readxl::read_xlsx("C:/Users/faria/tw/data/raw_store_names.xlsx", col_names = F)
names(data) <- "name"

data <- data %>% arrange(name)

exact_names_to_exclude <- c(
  "Pastries",
  "Park",
  "Plaza",  # optional — see notes below
  "Argentine",
  "Caterer"
  # Add more exact names here
)

data <- data %>%
  filter(
    !is.na(name),                                   # remove NA
    !name %in% exact_names_to_exclude,              # exact match exclusion
    !str_detect(name, "^(\\d+\\.\\d+)"),
    !str_detect(name, "^(\\d+°)"),
    !str_detect(name, "Hola"),
    !str_detect(name, "¡Hola"),
    !str_detect(name, "🔴Envíos"),
    !str_detect(name, "Punto de venta "),
    !str_detect(name, "Pin colocado"),
    !str_detect(name, "Punto"),
    !str_detect(name, "Carrer"),
    !str_detect(name, "Constelaciones"),
    !str_detect(name, "Psicolo"),
    !str_detect(name, "Argentinian"),
    !str_detect(name, "Holiday apartment"),
    !str_detect(name, "Grocery store"),
    !str_detect(name, "E-commerce"),
    !str_detect(name, "South American"),
    !str_detect(name, "Marcador"),
    !str_detect(name, "Butcher shop"),
    !str_detect(name, "Temporarily closed"),
    !str_detect(name, "Copy shop"),
    !str_detect(name, "Hairdresser"),
    !str_detect(name, "Las empanadas lo más parecido"),
    !str_detect(name, "Prácticas de"),
    !str_detect(name, "REMAX"),
    !str_detect(name, "Sweden"),
    !str_detect(name, "Cerca de"),
    !str_detect(name, "Soy "),
    !str_detect(name, "Punto"),
    !str_detect(name, "Punto"),
    !str_detect(name, "Punto"),
    !str_detect(name, "Punto"),
    !str_detect(name, "Punto"),
    !str_detect(name, "Punto"),
    !str_detect(name, "Punto"),
    !str_detect(name, "Punto"),
    !str_detect(name, "^(\\d{4}|£|€|\\$|-)")         # remove if starts with €, $, or -
  )

data <- data %>% group_by(name) %>% slice(1) %>% ungroup()

write_xlsx(data, "C:/Users/faria/tw/data/store_names.xlsx")
