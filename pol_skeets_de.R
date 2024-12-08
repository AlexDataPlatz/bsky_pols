# set up connection to Bluesky and atrrr package
# for developer version
# install.packages("pak") 
# pak::pak("JBGruber/atrrr")
# or from CRAN
# install.packages("atrrr") 
library(atrrr)
library(tidyverse)

# set up app password in browser, then add to R studio to authenticate
auth("*add_your_handle_here.bsky.social*")

# load other useful packages
library(rvest)
library(httr)

# Create dataframe of MdBs and their Bluesky handles
# Import list of MdBs
# URL of the Wikipedia page
url <- "https://en.wikipedia.org/wiki/List_of_members_of_the_20th_Bundestag#List_of_current_members"

# Read the HTML content of the page
webpage <- read_html(url)

# Extract all tables from the page
tables <- html_table(webpage, fill = TRUE)

# Inspect the list of tables (use View if running in RStudio)
# View(tables)

# Assuming the table of interest is the first one (adjust index as necessary)
bundestag_table <- tables[[7]]

# Remove duplicated 'Party' pictures, 4th column
bundestag_table <- bundestag_table[, -4]


# Import names with account handles from GitHub

# Or download and import most recent *bsky_de.csv* file from Teams folder

# URL of the raw CSV file
gh_url <- "https://raw.githubusercontent.com/AlexDataPlatz/bsky_pols/main/bsky_de_2811.csv"

# Download the file using httr
response <- GET(gh_url)

# Check if the request was successful
if (status_code(response) == 200) {
  # Save the content to a temporary file
  temp_file <- tempfile(fileext = ".csv")
  writeBin(content(response, "raw"), temp_file)
  
  # Read the CSV file
  bsky_de <- read.csv(temp_file, stringsAsFactors = FALSE)
  head(bsky_de)
  
  # Optionally clean up
  unlink(temp_file)
} else {
  stop("Failed to download the file.")
}


# Make sure names and variables match for merging
bsky_de <- bsky_de %>%
  # Replace specific names
  dplyr::mutate(name = case_when(
    name == "Boris Mijatovic" ~ "Boris Mijatović",
    name == "Catarina dos Santos" ~ "Catarina dos Santos Firnhaber",
    name == "Cornelia Moehring" ~ "Cornelia Möhring",
    name == "KonstantinNotz" ~ "Konstantin von Notz",
    name == "MdB Sören Pellmann" ~ "Sören Pellmann",
    name == "Soeren Bartol" ~ "Sören Bartol",
    name == "Sven Kindler" ~ "Sven-Christian Kindler",
    name == "Tobias B. Bacherle" ~ "Tobias Bacherle",
    TRUE ~ name # Keep all other names unchanged
  )) %>%
  # Remove specific names
  filter(!name %in% c(
    "Burkhard Lischka", "Chris Kühn", "Jakob Maria Mierscheid", 
    "Landesgruppensprecherin NRW", "Marie-Agnes Strack-Zimmermann", 
    "Stephan Bischoff", "Uli Grötsch", "Valerie Wilms", "houbenreinhard.bsky.social"
  ))

bundestag_table <- bundestag_table %>%
  rename(name = Name)

# Merge bluesky handles with bundestag table
bsky_de <- merge(bundestag_table, bsky_de, by = 'name', all = TRUE)

# # Check for any misses
# miss <- bsky_de %>%
#   filter(is.na(Party))
# 
# # Check for any duplicates
# duplicates <- bsky_de2 %>%
#   group_by(name) %>%
#   filter(n() > 1)

post <- search_post("from:@alexhartland.bsky.social")

post <- get_skeets_authored_by(actor = "alexhartland.bsky.social", parse = TRUE) 

# Remove rows with NA values in bsky_handle
bsky_de_nao <- bsky_de %>%
  dplyr::filter(!is.na(bsky_handle))

# When did they join? Uses indexed_at, not a reliable indicator!
# # Define a function to fetch 'indexed_at' for a given handle
# fetch_user_info <- function(handle) {
#   # Remove the leading '@' and any extra spaces from the handle
#   clean_handle <- stringr::str_trim(stringr::str_remove(handle, "^@"))
#   
#   # Check if the handle is valid (must not contain invalid characters like spaces or multiple @ symbols)
#   if (!stringr::str_detect(clean_handle, "^[a-zA-Z0-9._-]+$")) {
#     warning(paste("Invalid handle:", handle))
#     return(NA_character_)  # Return NA for invalid handles
#   }
#   
#   # Fetch user info using the cleaned handle
#   user_info <- tryCatch(
#     atrrr::get_user_info(actor = clean_handle),
#     error = function(e) {
#       warning(paste("Error fetching info for handle:", handle, "Error:", e$message))
#       return(NULL)
#     }
#   )
#   
#   # Extract 'indexed_at' field; ensure it returns a single value
#   if (!is.null(user_info) && !is.null(user_info$indexed_at)) {
#     return(as.character(user_info$indexed_at))
#   }
#   
#   return(NA_character_)  # Return NA if 'indexed_at' is missing or on error
# }
# 
# # Use purrr::map_chr to iterate over the bsky_handle column
# indexed_at_list <- purrr::map_chr(bsky_de_nao$bsky_handle, fetch_user_info)
# 
# # Add the new column 'indexed_at' to the original data frame
# bsky_de_nao <- bsky_de_nao %>%
#   dplyr::mutate(indexed_at = indexed_at_list)


# Fetch skeets authored by each handle
fetch_skeets <- function(handle) {
  # Remove the leading '@' from the handle
  clean_handle <- stringr::str_remove(handle, "^@")
  
  # Fetch skeets using the cleaned handle
  skeets <- tryCatch(
    atrrr::get_skeets_authored_by(actor = clean_handle, parse = TRUE, limit = 5000L),
    error = function(e) {
      warning(paste("Error fetching skeets for handle:", handle, "Error:", e$message))
      return(NULL)
    }
  )
  
  return(skeets)
}

# Import skeets for all handles in bsky_de_nao$bsky_handle
# Could take some time!
skeets_list <- purrr::map(bsky_de_nao$bsky_handle, fetch_skeets)

# Combine skeets into a single dataframe
skeets_de <- bind_rows(skeets_list)

# Clean the 'indexed_at' variable in skeets_de
# Convert 'indexed_at' to Date format and filter by date
skeets_de <- skeets_de %>%
  dplyr::mutate(indexed_at = as.Date(indexed_at, format = "%Y-%m-%d %H:%M:%S"))

# Subset bsky_de_nao and rename 'indexed_at' to 'joined'
bsky_de_nao_subset <- bsky_de_nao %>%
  dplyr::select(bsky_handle, Party)

# create handle variable for merging
skeets_de$bsky_handle <- paste0("@", skeets_de$author_handle)

# Merge the subset data frame with skeets_de
# Merge by 'bsky_handle', keeping all values from skeets_de
skeets_de <- skeets_de %>%
  left_join(bsky_de_nao_subset, by = "bsky_handle")

# Remove rows where is_reskeet is TRUE
# Somehow can't find the handle for who is reskeeting, only the original author
# Should be able to add this in future iteration
skeets_de <- skeets_de %>%
  filter(!is_reskeet)

# Combine "CDU" and "CSU" into a single "CDU" category in Party column
skeets_de <- skeets_de %>%
  mutate(Party = if_else(Party %in% c("CDU", "CSU"), "CDU", Party))

# Rename Party values to match the desired names
# Define a mapping for the Party values
party_mapping <- c(
  "CDU" = "CDU/CSU",
  "FDP" = "FDP",
  "GRÜNE" = "Grüne",
  "LINKE" = "Die Linke",
  "SPD" = "SPD",
  "SSW" = "SSW",  # Assuming no change for SSW
  "AfD" = "AfD"   # Add AfD if relevant to the data
)

# Apply the mapping to rename Party values
skeets_de <- skeets_de %>%
  dplyr::mutate(Party = recode(Party, !!!party_mapping))


# Define the color mapping for the parties
party_colors <- c(
  "AfD" = "#00A2DE",
  "CDU/CSU" = "#151518",
  "FDP" = "#FFED00",
  "Grüne" = "#409A3C",
  "Die Linke" = "#BE3075",
  "SPD" = "#E3000F",
  "SSW" = "#003C8F"
)

# Step 1: Aggregate the data to count skeets per day per Party
skeets_daily <- skeets_de %>%
  dplyr::group_by(indexed_at, Party) %>%
  dplyr::summarize(skeets_count = n(), .groups = "drop")

# Step 2: Create the geom_line plot
ggplot(skeets_daily, aes(x = indexed_at, y = skeets_count, color = Party)) +
  geom_line(size = 1) +  # Set line thickness
  scale_color_manual(values = party_colors) +  # Apply the custom color mapping
  labs(
    title = "Daily Post Count by Party",
    x = "Date",
    y = "Number of Posts",
    color = "Party"
  ) +
  theme_minimal() +  # Clean, minimal theme
  theme(
    legend.position = "bottom",  # Move legend to the bottom
    axis.text.x = element_text(angle = 45, hjust = 1)  # Rotate x-axis labels
  )


# Filter the data for dates from 2024-10-01 onwards
skeets_daily_filtered <- skeets_daily %>%
  filter(indexed_at >= as.Date("2024-10-01")) %>%
  dplyr::filter(indexed_at < max(indexed_at))

# Create the geom_line plot
ggplot(skeets_daily_filtered, aes(x = indexed_at, y = skeets_count, color = Party)) +
  geom_line(size = 1) +  # Set line thickness
  scale_color_manual(values = party_colors) +  # Apply the custom color mapping
  labs(
    title = "Daily Posts per Party",
    x = "",
    y = "Posts per Day",
    color = "Party"
  ) + 
  theme_light() + 
  theme(
    legend.position = "right",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 45, hjust = 1)  # Rotate x-axis labels
  )
