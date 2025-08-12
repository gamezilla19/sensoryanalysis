# ===== APPLICATION SHINY - GESTIONNAIRE MÉTADONNÉES AVEC POSTGRESQL =====
# Auteur: Interface utilisateur pour saisie manuelle avec base PostgreSQL
# Date: 2025-08-12 - VERSION POSTGRESQL INTÉGRÉE AVEC CONTRAINTES MÉTIER

# ===== CHARGEMENT DES BIBLIOTHÈQUES =====
suppressPackageStartupMessages({
  library(shiny)
  library(shinydashboard)
  library(DT)
  library(readxl)
  library(writexl)
  library(dplyr)
  library(stringr)
  library(fs)
  library(shinyWidgets)
  library(shinycssloaders)
  library(RPostgres)
  library(DBI)
  library(shinyjs)
})

# ===== LISTES DE VALEURS MÉTIER =====

# Liste GMPS TYPE
GMPS_TYPE_CHOICES <- c("", "INFO", "SUB")

# Liste MASTER CUSTOMER NAME
MASTER_CUSTOMER_CHOICES <- c(
  "",
  "AMKA", "ANJAC HEALTH & BEAUTY/ROVAL", "ARCHEM", "ARMA GROUP", "ASPIRA NIGERIA",
  "BASEL", "BEIERSDORF", "BEYAZ KAGIT", "BOLSIUS", "BOLTON GROUP", "BRIOCHIN ST BRANDAN",
  "BUCK HOLDING AG", "CAR FRESHNER CORPORATION", "CARREFOUR", "CEMP", "CYBELE COSMETIC LTD",
  "DANYA", "DELTAPRONATURA", "DENIS ET FILS", "DIAMOND OVERSEAS", "DR KURT WOLFF",
  "DRAMERS", "ECOLAB", "EMOSIA", "ESSITY", "EUROSYN", "EVYAP", "EXPANSCIENCE",
  "FABRE", "FATER", "FCG COLLECTION", "HARRIS LE BRIOCHIN", "HAYAT", "HELIOS MILESTONE TRADING",
  "HENKEL", "HERITAGE", "INDIGO", "INNOVA", "ITASILVA", "JOHNSON CLEAN", "KNEIPP",
  "LAB INDUSTRIES", "LABOCOS", "LA RIVE", "LEA NATURE", "LECLERC", "LION SPIRIT",
  "LITEGO AB", "LITTLE TREES LTD", "LOCCITANE", "LORCO", "LOREAL", "M & L LABORATOIRES",
  "MADAR CHEMICAL", "MAJ INTERNATIONAL", "MANE", "MANETTI & ROBERTS", "MANITOBA",
  "MARC AND SPENCER", "MAVERICK ULLDECONA", "MAXIM KOSMETIK GMBH", "MC BRIDE",
  "MERCADONA", "MIELE", "NICOLS", "NOVAMEX", "NUXE", "PAGLIERI", "PANAMEX PACIFIC",
  "PERSAN POLSKA", "PERSAN SPAIN", "PHOCEENNE DE COSMETIQUE", "PONS QUIMICAS",
  "PROCTER FRAGRANCE", "POREX", "PZ CUSSONS", "REVLON", "RNM", "SAINSBURY",
  "SANO/COSMOPHARM", "SPRING COLLECTIVE", "STERLING PERFUMES INDUSTR", "SWANIA",
  "SYSTEME U", "TESCO PLC", "THE SPB GLOBAL CORPO.", "TRADE KINGS", "UBESOL",
  "UNILEVER FRAGRANCE", "UNIVERS DETERGENT", "VANDEPUTTE BELGIQUE", "VIOKOX",
  "VIOLETA", "ZOBELE", "ZOHAR DALIA"
)

# Liste COUNTRY CLIENT
COUNTRY_CLIENT_CHOICES <- c(
  "",
  "ALGERIA", "ARGENTINA", "AUSTRALIA", "AUSTRIA", "BRAZIL", "CHILE", "CHINA",
  "COLOMBIA", "EGYPT", "FRANCE", "GERMANY", "GHANA", "HONG KONG", "INDIA",
  "INDONESIA", "ISRAEL", "ITALY", "JAPAN", "JORDANIA", "KAZAKHSTAN", "KENYA",
  "KOREA SOUTH", "MEXICO", "MOLDAVIA", "TURKEY", "NIGERIA", "PAKISTAN",
  "PHILIPPINES", "POLAND", "RUSSIA", "SINGAPOUR", "SOUTH AFRICA", "SPAIN",
  "SWITZERLAND", "THAILAND", "TURKEY", "UKRAINE", "UNITED ARAB EMIRATES",
  "UNITED KINGDOM", "USA", "VIETNAM", "BELGIQUE", "TUNISIA"
)

# Liste TYPE OF TEST
TYPE_OF_TEST_CHOICES <- c("", "PROACTIVE", "BRIEF")

# Liste CATEGORY
CATEGORY_CHOICES <- c(
  "",
  "CROSS CATEGORY", "HOME CARE (HC)", "FABRIC CARE (FC)", "PERSONAL CARE (PC)",
  "HAIR CARE (HAC)", "FRAGRANCE DEO PERFUME (FDP)", "FINE (FF)"
)

# Liste SUBSEGMENT
SUBSEGMENT_CHOICES <- c(
  "",
  "CROSS CATEGORY", "AIR / SPRAYS", "AIR / ELECTRICS", "AIR / BURNING SYSTEMS",
  "AIR / LIQUIDS", "AIR / SOLIDS", "AIR / CARS", "HOU / DISH CARE",
  "HOU / ALL PURPOSE CLEANER", "HOU / TOILET BLOCK", "HOU / TOILET PAPER",
  "HOU / BLEACH", "FAB / LIQUID DETERGENT", "FAB / POWDER DETERGENT",
  "FAB / FABRIC SOFTENER", "FAB / AUXILIAR", "FAB / SOAP", "DEO / ANTI PERSPIRANT",
  "DEO / BACTERICIDE", "HAI / BUNDEL", "HAI / COLOR CARE", "HAI / CONDITIONER",
  "HAI / HAIR TREATMENT", "SHO / SHOWER", "HAI / SHAMPOO", "HAI / STYLING",
  "CHI / BODY CARE", "CHI / FACE CARE", "CHI / HAIR & BODY WASH", "CHI / LIP CARE",
  "CHI / SHAMPOO", "CHI / SHOWER", "CHI / SUN CARE", "MEN / BODY CARE",
  "MEN / DEPILATORIES", "MEN / EYE CARE", "MEN / FACE CARE", "MEN / HAND CARE",
  "MEN / LIP CARE", "MEN / SUN CARE", "SHO / BATH", "SHO / HAIR & BODY WASH",
  "SHO / INTIMATE HYGENE", "SHO / SHOWER", "SHO / SOAP", "SKI / BODY CARE",
  "SKI / DEPILATORIES", "SKI / EYE CARE", "SKI / FACE CARE", "SKI / HAND CARE",
  "SKI / LIP CARE", "SKI / SUN CARE", "FFG / FINE FRAGRANCE", "FFG / FINE FRAGRANCE MASS"
)

# Liste METHODOLOGY
METHODOLOGY_CHOICES <- c(
  "",
  "Type of test", "Sniff Test", "Home Use Test", "Consumer Napping", "Omnibus",
  "Use & Habits", "Concept testing", "Consumer behaviour", "Ingredient tracking",
  "Ethnographic surveys", "Focus Group Discussion", "In Home Evaluation",
  "Quali home use test", "Online bulletin board", "Co-creation workshop",
  "Preference test with tasting", "Individual in-depth interview",
  "Internal Focus Group Discussion", "Development Sniff Test", "Internal Sniff Test",
  "Internal Preference test with tasting", "Internal Quali home use test",
  "Flash Profile", "Strength test", "Malodor Counteractancy", "QDA Monadic Profile",
  "DoD (degree of difference)", "CATA", "Comparative profiling", "Time-Intensity",
  "Free sorting task", "Quick Flavour Description", "Descriptors training",
  "Temporal Dominance of Sensation", "Descriptive Napping", "Triangular Test",
  "Proximity test", "2-AFC / pair test", "Duo-Trío", "Ranking", "2 out of 5",
  "A or not A", "Tetrad"
)

# Liste PANEL
PANEL_CHOICES <- c(
  "",
  "EXPERT PANEL", "TRAINED PANEL", "EXPERT & TRAINED PANEL", "CONSUMER PANEL"
)

# Liste TEST FACILITIES
TEST_FACILITIES_CHOICES <- c(
  "",
  "Sensory booth", "Shooting star", "Sensory lab", "CLT", "Home", "Hotel room"
)

# Liste REF (Product Info)
REF_CHOICES <- c("", "Y", "N")

# ===== CONFIGURATION ADAPTÉE AVEC POSTGRESQL =====
RAW_DATA_DIR <- "C:/testManon"
RESULTS_DIR <- "C:/ResultatsAnalyseSenso/Fichiers_Individuels"
METADATA_DB_PATH <- file.path(RESULTS_DIR, "METADATA_PRODUITS_DB.xlsx")
CONSOLIDATED_REPORT_PATH <- file.path("C:/ResultatsAnalyseSenso/RAPPORT_CONSOLIDE", "RAPPORT_CONSOLIDE.xlsx")

# ===== FONCTIONS DE CONNEXION POSTGRESQL =====
create_postgres_connection <- function() {
  tryCatch({
    con <- dbConnect(RPostgres::Postgres(),
                     dbname = "Metadata_SA",
                     host = "emfrndsunx574.emea.sesam.mane.com",
                     port = 5432,
                     user = "dbadmin",
                     password = "Azerty06*")
    message("Connexion PostgreSQL établie avec succès")
    return(con)
  }, error = function(e) {
    message("ERREUR connexion PostgreSQL: ", e$message)
    return(NULL)
  })
}

safe_disconnect <- function(con) {
  if(!is.null(con) && dbIsValid(con)) {
    dbDisconnect(con)
    message("Connexion PostgreSQL fermée")
  }
}

# ===== FONCTIONS DE VALIDATION =====

# Validation format date DD/MM/YYYY
validate_date_format <- function(date_string) {
  if(is.null(date_string) || date_string == "") return(TRUE)
  
  pattern <- "^\\d{2}/\\d{2}/\\d{4}$"
  if(!grepl(pattern, date_string)) return(FALSE)
  
  tryCatch({
    parsed_date <- as.Date(date_string, format = "%d/%m/%Y")
    return(!is.na(parsed_date))
  }, error = function(e) {
    return(FALSE)
  })
}

# Validation format pourcentage
validate_percentage_format <- function(percentage_string) {
  if(is.null(percentage_string) || percentage_string == "") return(TRUE)
  
  # Accepter les formats: "1.5%", "1,5%", "1.5", "1,5"
  pattern <- "^\\d+([.,]\\d+)?%?$"
  return(grepl(pattern, percentage_string))
}

# Validation numérique pour S&C REQUEST
validate_numeric_format <- function(numeric_string) {
  if(is.null(numeric_string) || numeric_string == "") return(TRUE)
  
  return(!is.na(suppressWarnings(as.numeric(numeric_string))))
}

# ===== FONCTIONS DE GESTION DES TABLES POSTGRESQL (INCHANGÉES) =====
load_test_info_from_postgres <- function(con) {
  if(is.null(con) || !dbIsValid(con)) {
    return(data.frame(
      gmps_type = character(0),
      gpms_code = character(0),
      sc_request = character(0),
      test_name = character(0),
      test_date = character(0),
      master_customer_name = character(0),
      country_client = character(0),
      type_of_test = character(0),
      category = character(0),
      subsegment = character(0),
      methodology = character(0),
      panel = character(0),
      test_facilities = character(0),
      stringsAsFactors = FALSE
    ))
  }
  
  tryCatch({
    if(dbExistsTable(con, "testinfo")) {
      test_info <- dbReadTable(con, "testinfo")
      message("Test Info chargé depuis PostgreSQL: ", nrow(test_info), " lignes")
      return(test_info)
    } else {
      message("Table 'testinfo' non trouvée dans PostgreSQL")
      return(data.frame())
    }
  }, error = function(e) {
    message("Erreur lecture Test Info: ", e$message)
    return(data.frame())
  })
}

load_product_info_from_postgres <- function(con) {
  if(is.null(con) || !dbIsValid(con)) {
    return(data.frame(
      code_prod = character(0),
      nomprod = character(0),
      base = character(0),
      ref = character(0),
      dosage = character(0),
      stringsAsFactors = FALSE
    ))
  }
  
  tryCatch({
    if(dbExistsTable(con, "productinfo")) {
      product_info <- dbReadTable(con, "productinfo")
      message("Product Info chargé depuis PostgreSQL: ", nrow(product_info), " lignes")
      return(product_info)
    } else {
      message("Table 'productinfo' non trouvée dans PostgreSQL")
      return(data.frame())
    }
  }, error = function(e) {
    message("Erreur lecture Product Info: ", e$message)
    return(data.frame())
  })
}

# Fonction pour obtenir les noms de produits uniques depuis databrute
get_unique_product_names <- function(con) {
  if(is.null(con) || !dbIsValid(con)) return(c(""))
  
  tryCatch({
    if(dbExistsTable(con, "databrute")) {
      raw_data <- dbReadTable(con, "databrute")
      if("productname" %in% names(raw_data)) {
        unique_products <- unique(raw_data$productname)
        unique_products <- unique_products[!is.na(unique_products) & unique_products != ""]
        return(c("", sort(unique_products)))
      }
    }
    return(c(""))
  }, error = function(e) {
    message("Erreur récupération noms produits: ", e$message)
    return(c(""))
  })
}

# Fonction pour obtenir les bases uniques depuis productinfo
get_unique_bases <- function(con) {
  if(is.null(con) || !dbIsValid(con)) return(c(""))
  
  tryCatch({
    if(dbExistsTable(con, "productinfo")) {
      product_info <- dbReadTable(con, "productinfo")
      if("base" %in% names(product_info)) {
        unique_bases <- unique(product_info$base)
        unique_bases <- unique_bases[!is.na(unique_bases) & unique_bases != ""]
        return(c("", sort(unique_bases)))
      }
    }
    return(c(""))
  }, error = function(e) {
    message("Erreur récupération bases: ", e$message)
    return(c(""))
  })
}

save_test_info_to_postgres <- function(con, test_info_data) {
  if(is.null(con) || !dbIsValid(con)) return(FALSE)
  
  tryCatch({
    test_info_clean <- test_info_data %>%
      rename_with(~str_to_lower(str_replace_all(.x, "[^[:alnum:]_]", "_")))
    
    dbWriteTable(con, "testinfo", test_info_clean, 
                 append = TRUE, row.names = FALSE)
    
    message("Test Info sauvegardé vers PostgreSQL: ", nrow(test_info_clean), " lignes")
    return(TRUE)
  }, error = function(e) {
    message("Erreur sauvegarde Test Info: ", e$message)
    return(FALSE)
  })
}

save_product_info_to_postgres <- function(con, product_info_data) {
  if(is.null(con) || !dbIsValid(con)) return(FALSE)
  
  tryCatch({
    product_info_clean <- product_info_data %>%
      rename_with(~str_to_lower(str_replace_all(.x, "[^[:alnum:]_]", "_")))
    
    dbWriteTable(con, "productinfo", product_info_clean, 
                 append = TRUE, row.names = FALSE)
    
    message("Product Info sauvegardé vers PostgreSQL: ", nrow(product_info_clean), " lignes")
    return(TRUE)
  }, error = function(e) {
    message("Erreur sauvegarde Product Info: ", e$message)
    return(FALSE)
  })
}

# ===== FONCTIONS DE DÉTECTION DES DONNÉES MANQUANTES (INCHANGÉES) =====
detect_missing_test_info <- function(con) {
  if(is.null(con) || !dbIsValid(con)) return(data.frame())
  
  tryCatch({
    existing_tests <- data.frame()
    tables_to_scan <- c("strengthtest", "proximity", "triangulaire", "databrute")
    
    for(table_name in tables_to_scan) {
      if(dbExistsTable(con, table_name)) {
        table_data <- dbReadTable(con, table_name)
        
        if("idtest" %in% names(table_data)) {
          test_ids <- table_data %>%
            select(idtest) %>%
            distinct() %>%
            mutate(source_table = table_name)
          
          existing_tests <- bind_rows(existing_tests, test_ids)
        }
      }
    }
    
    if(nrow(existing_tests) == 0) return(data.frame())
    
    existing_test_info <- load_test_info_from_postgres(con)
    
    missing_tests <- existing_tests %>%
      distinct(idtest, .keep_all = TRUE) %>%
      filter(!idtest %in% existing_test_info$test_name) %>%
      mutate(
        gmps_type = "",
        gpms_code = "",
        sc_request = "",
        test_name = idtest,
        test_date = "",
        master_customer_name = "",
        country_client = "",
        type_of_test = "",
        category = "",
        subsegment = "",
        methodology = "",
        panel = "",
        test_facilities = "",
        statut = "À compléter"
      )
    
    return(missing_tests)
    
  }, error = function(e) {
    message("Erreur détection Test Info manquants: ", e$message)
    return(data.frame())
  })
}

detect_missing_product_info <- function(con) {
  if(is.null(con) || !dbIsValid(con)) return(data.frame())
  
  tryCatch({
    existing_products <- data.frame()
    
    if(dbExistsTable(con, "databrute")) {
      raw_data <- dbReadTable(con, "databrute")
      
      if("productname" %in% names(raw_data)) {
        existing_products <- raw_data %>%
          select(productname) %>%
          distinct() %>%
          rename(product_name = productname)
      }
    }
    
    if(nrow(existing_products) == 0) return(data.frame())
    
    existing_product_info <- load_product_info_from_postgres(con)
    
    missing_products <- existing_products %>%
      filter(!product_name %in% existing_product_info$nomprod) %>%
      mutate(
        code_prod = "",
        nomprod = product_name,
        base = "",
        ref = "",
        dosage = "",
        statut = "À compléter"
      )
    
    return(missing_products)
    
  }, error = function(e) {
    message("Erreur détection Product Info manquants: ", e$message)
    return(data.frame())
  })
}

# ===== INTERFACE UTILISATEUR ÉTENDUE AVEC CONTRAINTES =====
ui <- dashboardPage(
  dashboardHeader(title = "Gestionnaire Métadonnées PostgreSQL - Script Analyse"),
  
  dashboardSidebar(
    sidebarMenu(
      menuItem("Connexion PostgreSQL", tabName = "connection", icon = icon("database")),
      menuItem("Scanner Test Info", tabName = "scan_tests", icon = icon("search")),
      menuItem("Scanner Product Info", tabName = "scan_products", icon = icon("search")),
      menuItem("Saisie Test Info", tabName = "manual_test", icon = icon("edit")),
      menuItem("Saisie Product Info", tabName = "manual_product", icon = icon("edit")),
      menuItem("Tables PostgreSQL", tabName = "postgres_tables", icon = icon("table")),
      menuItem("Import/Export", tabName = "import", icon = icon("file-excel")),
      menuItem("Debug", tabName = "debug", icon = icon("bug"))
    )
  ),
  
  dashboardBody(
    useShinyjs(),
    
    tags$head(
      tags$style(HTML("
        .content-wrapper, .right-side {
          background-color: #f4f4f4;
        }
        .box {
          border-top-color: #3c8dbc;
        }
        .btn-primary {
          background-color: #3c8dbc;
          border-color: #3c8dbc;
        }
        .alert-info {
          background-color: #d9edf7;
          border-color: #bce8f1;
          color: #31708f;
        }
        .alert-success {
          background-color: #dff0d8;
          border-color: #d6e9c6;
          color: #3c763d;
        }
        .alert-warning {
          background-color: #fcf8e3;
          border-color: #faebcc;
          color: #8a6d3b;
        }
        .alert-danger {
          background-color: #f2dede;
          border-color: #ebccd1;
          color: #a94442;
        }
        .status-connected {
          color: #28a745;
          font-weight: bold;
        }
        .status-disconnected {
          color: #dc3545;
          font-weight: bold;
        }
        .validation-error {
          border: 2px solid #d9534f !important;
        }
        .validation-success {
          border: 2px solid #5cb85c !important;
        }
      "))
    ),
    
    tabItems(
      # ===== ONGLET CONNEXION POSTGRESQL (INCHANGÉ) =====
      tabItem(
        tabName = "connection",
        fluidRow(
          box(
            title = "Connexion PostgreSQL", 
            status = "primary", 
            solidHeader = TRUE,
            width = 12,
            
            h4("État de la connexion:"),
            div(id = "connection_status", class = "alert alert-warning",
                icon("exclamation-triangle"),
                " Connexion non établie"
            ),
            
            br(),
            
            fluidRow(
              column(6,
                     actionButton(
                       "connect_btn",
                       "Se Connecter",
                       icon = icon("plug"),
                       class = "btn-success btn-lg"
                     )
              ),
              column(6,
                     actionButton(
                       "disconnect_btn",
                       "Se Déconnecter",
                       icon = icon("unlink"),
                       class = "btn-danger btn-lg"
                     )
              )
            ),
            
            br(),
            
            h4("Informations de connexion:"),
            verbatimTextOutput("connection_info"),
            
            br(),
            
            h4("Tables disponibles:"),
            verbatimTextOutput("available_tables")
          )
        )
      ),
      
      # ===== ONGLETS SCANNER (INCHANGÉS) =====
      tabItem(
        tabName = "scan_tests",
        fluidRow(
          box(
            title = "Scanner Test Info Manquants", 
            status = "primary", 
            solidHeader = TRUE,
            width = 12,
            
            p("Cet outil identifie les tests qui existent dans les données d'analyse mais qui n'ont pas d'informations complètes dans la table Test Info."),
            
            div(class = "alert alert-info",
                icon("info-circle"),
                " Double-cliquez sur un test de la liste pour le compléter directement !"
            ),
            
            actionButton(
              "scan_test_info_btn",
              "Scanner Test Info Manquants",
              icon = icon("search"),
              class = "btn-primary btn-lg"
            ),
            
            br(), br(),
            
            conditionalPanel(
              condition = "output.test_scan_completed",
              div(class = "alert alert-warning",
                  icon("mouse-pointer"),
                  " Double-cliquez sur une ligne pour compléter le test directement !"
              ),
              withSpinner(
                DT::dataTableOutput("missing_test_info_table"),
                type = 4
              )
            )
          )
        )
      ),
      
      tabItem(
        tabName = "scan_products",
        fluidRow(
          box(
            title = "Scanner Product Info Manquants", 
            status = "primary", 
            solidHeader = TRUE,
            width = 12,
            
            p("Cet outil identifie les produits qui existent dans les données brutes mais qui n'ont pas d'informations complètes dans la table Product Info."),
            
            div(class = "alert alert-info",
                icon("info-circle"),
                " Double-cliquez sur un produit de la liste pour le compléter directement !"
            ),
            
            actionButton(
              "scan_product_info_btn",
              "Scanner Product Info Manquants",
              icon = icon("search"),
              class = "btn-primary btn-lg"
            ),
            
            br(), br(),
            
            conditionalPanel(
              condition = "output.product_scan_completed",
              div(class = "alert alert-warning",
                  icon("mouse-pointer"),
                  " Double-cliquez sur une ligne pour compléter le produit directement !"
              ),
              withSpinner(
                DT::dataTableOutput("missing_product_info_table"),
                type = 4
              )
            )
          )
        )
      ),
      
      # ===== ONGLET SAISIE TEST INFO AVEC CONTRAINTES =====
      tabItem(
        tabName = "manual_test",
        fluidRow(
          box(
            title = "Saisie Test Info", 
            status = "primary", 
            solidHeader = TRUE,
            width = 8,
            
            div(id = "test_indicator", class = "alert alert-info", style = "display: none;",
                icon("info-circle"),
                "Saisie en cours pour le test: ",
                textOutput("current_test_name", inline = TRUE)
            ),
            
            # Messages de validation
            div(id = "test_validation_messages"),
            
            textInput("test_name_input", "Test Name", 
                      placeholder = "Ex: TEST_2025_001"),
            
            fluidRow(
              column(6,
                     selectInput("gmps_type", "GMPS Type", 
                                 choices = GMPS_TYPE_CHOICES,
                                 selected = "")
              ),
              column(6,
                     textInput("gpms_code", "GPMS Code", 
                               placeholder = "Ex: GPMS001")
              )
            ),
            
            fluidRow(
              column(6,
                     textInput("sc_request", "S&C Request # (Numérique)", 
                               placeholder = "Ex: 123456")
              ),
              column(6,
                     textInput("test_date", "Test Date (DD/MM/YYYY)", 
                               placeholder = "Ex: 15/01/2025")
              )
            ),
            
            fluidRow(
              column(6,
                     selectInput("master_customer_name", "Master Customer Name", 
                                 choices = MASTER_CUSTOMER_CHOICES,
                                 selected = "")
              ),
              column(6,
                     selectInput("country_client", "Country Client", 
                                 choices = COUNTRY_CLIENT_CHOICES,
                                 selected = "")
              )
            ),
            
            fluidRow(
              column(6,
                     selectInput("type_of_test", "Type of Test",
                                 choices = TYPE_OF_TEST_CHOICES,
                                 selected = "")
              ),
              column(6,
                     selectInput("category", "Category", 
                                 choices = CATEGORY_CHOICES,
                                 selected = "")
              )
            ),
            
            fluidRow(
              column(6,
                     selectInput("subsegment", "Subsegment", 
                                 choices = SUBSEGMENT_CHOICES,
                                 selected = "")
              ),
              column(6,
                     selectInput("methodology", "Methodology", 
                                 choices = METHODOLOGY_CHOICES,
                                 selected = "")
              )
            ),
            
            fluidRow(
              column(6,
                     selectInput("panel", "Panel", 
                                 choices = PANEL_CHOICES,
                                 selected = "")
              ),
              column(6,
                     selectInput("test_facilities", "Test Facilities", 
                                 choices = TEST_FACILITIES_CHOICES,
                                 selected = "")
              )
            ),
            
            br(),
            
            fluidRow(
              column(6,
                     actionButton(
                       "save_test_info_btn",
                       "Enregistrer Test Info",
                       icon = icon("save"),
                       class = "btn-success btn-lg"
                     )
              ),
              column(6,
                     actionButton(
                       "save_test_and_next_btn",
                       "Enregistrer et Suivant",
                       icon = icon("arrow-right"),
                       class = "btn-primary btn-lg"
                     )
              )
            ),
            
            br(),
            
            actionButton(
              "clear_test_form_btn",
              "Vider le Formulaire",
              icon = icon("eraser"),
              class = "btn-secondary"
            )
          ),
          
          box(
            title = "Aide Test Info", 
            status = "info", 
            solidHeader = TRUE,
            width = 4,
            
            h4("Champs Test Info:"),
            tags$ul(
              tags$li(strong("GMPS TYPE:"), " Liste déroulante (INFO ou SUB)"),
              tags$li(strong("GPMS CODE:"), " Texte libre"),
              tags$li(strong("S&C REQUEST #:"), " Numérique uniquement"),
              tags$li(strong("TEST NAME:"), " Nom unique du test"),
              tags$li(strong("TEST DATE:"), " Format DD/MM/YYYY obligatoire"),
              tags$li(strong("MASTER CUSTOMER NAME:"), " Liste déroulante"),
              tags$li(strong("COUNTRY CLIENT:"), " Liste déroulante"),
              tags$li(strong("TYPE OF TEST:"), " Liste déroulante"),
              tags$li(strong("CATEGORY:"), " Liste déroulante"),
              tags$li(strong("SUBSEGMENT:"), " Liste déroulante"),
              tags$li(strong("METHODOLOGY:"), " Liste déroulante"),
              tags$li(strong("PANEL:"), " Liste déroulante"),
              tags$li(strong("TEST FACILITIES:"), " Liste déroulante")
            ),
            
            br(),
            
            h4("Tests restants:"),
            textOutput("remaining_tests_count"),
            
            br(),
            
            div(class = "alert alert-info",
                icon("info-circle"),
                strong("Validation automatique:"),
                br(),
                "• Date: format DD/MM/YYYY",
                br(),
                "• S&C Request: numérique uniquement",
                br(),
                "• Listes: sélection obligatoire"
            )
          )
        )
      ),
      
      # ===== ONGLET SAISIE PRODUCT INFO AVEC CONTRAINTES =====
      tabItem(
        tabName = "manual_product",
        fluidRow(
          box(
            title = "Saisie Product Info", 
            status = "primary", 
            solidHeader = TRUE,
            width = 8,
            
            div(id = "product_indicator", class = "alert alert-info", style = "display: none;",
                icon("info-circle"),
                "Saisie en cours pour le produit: ",
                textOutput("current_product_name", inline = TRUE)
            ),
            
            # Messages de validation
            div(id = "product_validation_messages"),
            
            textInput("code_prod_input", "Code Produit", 
                      placeholder = "Ex: MGOB714A04, I, trial, Smell-It..."),
            
            # NOMPROD avec liste déroulante dynamique
            selectizeInput("nomprod_input", "Nom Produit", 
                           choices = NULL,
                           options = list(
                             placeholder = "Sélectionnez ou tapez le nom du produit",
                             create = FALSE
                           )),
            
            # BASE avec liste déroulante + texte libre
            selectizeInput("base_input", "Base", 
                           choices = NULL,
                           options = list(
                             placeholder = "Sélectionnez une base existante ou tapez une nouvelle",
                             create = TRUE
                           )),
            
            selectInput("ref_input", "Référence (Y/N)",
                        choices = REF_CHOICES,
                        selected = ""),
            
            textInput("dosage_input", "Dosage (Pourcentage)", 
                      placeholder = "Ex: 1,50% ou 2.5%"),
            
            br(),
            
            fluidRow(
              column(6,
                     actionButton(
                       "save_product_info_btn",
                       "Enregistrer Product Info",
                       icon = icon("save"),
                       class = "btn-success btn-lg"
                     )
              ),
              column(6,
                     actionButton(
                       "save_product_and_next_btn",
                       "Enregistrer et Suivant",
                       icon = icon("arrow-right"),
                       class = "btn-primary btn-lg"
                     )
              )
            ),
            
            br(),
            
            actionButton(
              "clear_product_form_btn",
              "Vider le Formulaire",
              icon = icon("eraser"),
              class = "btn-secondary"
            )
          ),
          
          box(
            title = "Aide Product Info", 
            status = "info", 
            solidHeader = TRUE,
            width = 4,
            
            h4("Champs Product Info:"),
            tags$ul(
              tags$li(strong("CODE_PROD:"), " Texte libre (I, trial, Smell-It, code officiel...)"),
              tags$li(strong("NOMPROD:"), " Liste déroulante basée sur les données existantes"),
              tags$li(strong("BASE:"), " Liste + texte libre (bases existantes + nouvelles)"),
              tags$li(strong("REF:"), " Y (Oui) ou N (Non) uniquement"),
              tags$li(strong("DOSAGE:"), " Format pourcentage (1,5% ou 2.5%)")
            ),
            
            br(),
            
            h4("Produits restants:"),
            textOutput("remaining_products_count"),
            
            br(),
            
            div(class = "alert alert-info",
                icon("info-circle"),
                strong("Validation automatique:"),
                br(),
                "• Dosage: format pourcentage",
                br(),
                "• Référence: Y ou N uniquement",
                br(),
                "• Nom Produit: depuis données existantes"
            )
          )
        )
      ),
      
      # ===== ONGLET TABLES POSTGRESQL =====
      tabItem(
        tabName = "postgres_tables",
        fluidRow(
          box(
            title = "Tables PostgreSQL", 
            status = "primary", 
            solidHeader = TRUE,
            width = 12,
            
            tabsetPanel(
              tabPanel("Test Info",
                       br(),
                       actionButton("refresh_test_info_btn", "Actualiser", 
                                    icon = icon("refresh"), class = "btn-info"),
                       br(), br(),
                       withSpinner(DT::dataTableOutput("test_info_table"), type = 4)
              ),
              tabPanel("Product Info",
                       br(),
                       actionButton("refresh_product_info_btn", "Actualiser", 
                                    icon = icon("refresh"), class = "btn-info"),
                       br(), br(),
                       withSpinner(DT::dataTableOutput("product_info_table"), type = 4)
              )
            )
          )
        )
      ),
      
      # ===== ONGLET IMPORT/EXPORT =====
      tabItem(
        tabName = "import",
        fluidRow(
          box(
            title = "Import/Export Données", 
            status = "primary", 
            solidHeader = TRUE,
            width = 12,
            
            h4("Fonctionnalités d'import/export"),
            p("Cette section sera développée pour l'import/export de données Excel."),
            
            div(class = "alert alert-warning",
                icon("construction"),
                " Section en développement"
            )
          )
        )
      ),
      
      # ===== ONGLET DEBUG =====
      tabItem(
        tabName = "debug",
        fluidRow(
          box(
            title = "Informations de Debug PostgreSQL", 
            status = "warning", 
            solidHeader = TRUE,
            width = 12,
            
            h4("État connexion PostgreSQL:"),
            verbatimTextOutput("debug_postgres_status"),
            
            br(),
            
            h4("Tables PostgreSQL disponibles:"),
            verbatimTextOutput("debug_postgres_tables"),
            
            br(),
            
            h4("Statistiques des données:"),
            verbatimTextOutput("debug_data_stats"),
            
            br(),
            
            actionButton(
              "test_postgres_btn",
              "Test Connexion PostgreSQL",
              icon = icon("database"),
              class = "btn-warning"
            )
          )
        )
      )
    )
  )
)

# ===== SERVEUR AVEC CONTRAINTES MÉTIER =====
server <- function(input, output, session) {
  
  # Variables réactives
  postgres_con <- reactiveVal(NULL)
  connection_status <- reactiveVal(FALSE)
  missing_test_info <- reactiveVal(data.frame())
  missing_product_info <- reactiveVal(data.frame())
  test_scan_completed <- reactiveVal(FALSE)
  product_scan_completed <- reactiveVal(FALSE)
  
  # ===== GESTION CONNEXION POSTGRESQL =====
  observeEvent(input$connect_btn, {
    con <- create_postgres_connection()
    if(!is.null(con)) {
      postgres_con(con)
      connection_status(TRUE)
      
      # Charger les listes dynamiques
      updateSelectizeInput(session, "nomprod_input", 
                           choices = get_unique_product_names(con))
      updateSelectizeInput(session, "base_input", 
                           choices = get_unique_bases(con))
      
      # Mettre à jour l'interface
      shinyjs::html("connection_status", 
                    HTML('<i class="fa fa-check-circle status-connected"></i> Connexion établie avec succès'))
      shinyjs::removeClass("connection_status", "alert-warning")
      shinyjs::addClass("connection_status", "alert-success")
      
      showNotification("Connexion PostgreSQL établie", type = "message")
    } else {
      connection_status(FALSE)
      showNotification("Échec de la connexion PostgreSQL", type = "error")
    }
  })
  
  observeEvent(input$disconnect_btn, {
    con <- postgres_con()
    if(!is.null(con)) {
      safe_disconnect(con)
      postgres_con(NULL)
      connection_status(FALSE)
      
      # Mettre à jour l'interface
      shinyjs::html("connection_status", 
                    HTML('<i class="fa fa-exclamation-triangle status-disconnected"></i> Connexion fermée'))
      shinyjs::removeClass("connection_status", "alert-success")
      shinyjs::addClass("connection_status", "alert-warning")
      
      showNotification("Connexion PostgreSQL fermée", type = "message")
    }
  })
  
  # ===== VALIDATION EN TEMPS RÉEL =====
  
  # Validation Test Date
  observeEvent(input$test_date, {
    if(!is.null(input$test_date) && input$test_date != "") {
      if(validate_date_format(input$test_date)) {
        shinyjs::removeClass("test_date", "validation-error")
        shinyjs::addClass("test_date", "validation-success")
      } else {
        shinyjs::removeClass("test_date", "validation-success")
        shinyjs::addClass("test_date", "validation-error")
        shinyjs::html("test_validation_messages", 
                      '<div class="alert alert-danger"><i class="fa fa-exclamation-triangle"></i> Format de date invalide. Utilisez DD/MM/YYYY</div>')
      }
    } else {
      shinyjs::removeClass("test_date", c("validation-error", "validation-success"))
      shinyjs::html("test_validation_messages", "")
    }
  })
  
  # Validation S&C Request
  observeEvent(input$sc_request, {
    if(!is.null(input$sc_request) && input$sc_request != "") {
      if(validate_numeric_format(input$sc_request)) {
        shinyjs::removeClass("sc_request", "validation-error")
        shinyjs::addClass("sc_request", "validation-success")
      } else {
        shinyjs::removeClass("sc_request", "validation-success")
        shinyjs::addClass("sc_request", "validation-error")
        shinyjs::html("test_validation_messages", 
                      '<div class="alert alert-danger"><i class="fa fa-exclamation-triangle"></i> S&C Request doit être numérique</div>')
      }
    } else {
      shinyjs::removeClass("sc_request", c("validation-error", "validation-success"))
    }
  })
  
  # Validation Dosage
  observeEvent(input$dosage_input, {
    if(!is.null(input$dosage_input) && input$dosage_input != "") {
      if(validate_percentage_format(input$dosage_input)) {
        shinyjs::removeClass("dosage_input", "validation-error")
        shinyjs::addClass("dosage_input", "validation-success")
      } else {
        shinyjs::removeClass("dosage_input", "validation-success")
        shinyjs::addClass("dosage_input", "validation-error")
        shinyjs::html("product_validation_messages", 
                      '<div class="alert alert-danger"><i class="fa fa-exclamation-triangle"></i> Format de dosage invalide. Ex: 1,5% ou 2.5%</div>')
      }
    } else {
      shinyjs::removeClass("dosage_input", c("validation-error", "validation-success"))
      shinyjs::html("product_validation_messages", "")
    }
  })
  
  # ===== OUTPUTS DE CONNEXION =====
  output$connection_info <- renderText({
    if(connection_status()) {
      paste(
        "Host: emfrndsunx574.emea.sesam.mane.com",
        "Port: 5432",
        "Database: xyz",
        "User: dbadmin",
        "Status: Connecté",
        sep = "\n"
      )
    } else {
      "Connexion non établie"
    }
  })
  
  output$available_tables <- renderText({
    con <- postgres_con()
    if(!is.null(con) && dbIsValid(con)) {
      tryCatch({
        tables <- dbListTables(con)
        if(length(tables) > 0) {
          paste("Tables disponibles:", paste(tables, collapse = ", "))
        } else {
          "Aucune table trouvée"
        }
      }, error = function(e) {
        paste("Erreur listage tables:", e$message)
      })
    } else {
      "Connexion requise"
    }
  })
  
  # ===== SCANNER TEST INFO =====
  observeEvent(input$scan_test_info_btn, {
    con <- postgres_con()
    if(is.null(con)) {
      showNotification("Connexion PostgreSQL requise", type = "error")
      return()
    }
    
    showNotification("Scan Test Info en cours...", type = "message")
    test_scan_completed(FALSE)
    
    missing_tests <- detect_missing_test_info(con)
    missing_test_info(missing_tests)
    test_scan_completed(TRUE)
    
    if(nrow(missing_tests) > 0) {
      showNotification(
        paste("Trouvé", nrow(missing_tests), "tests sans informations complètes"),
        type = "warning"
      )
    } else {
      showNotification("Tous les tests ont des informations complètes", type = "message")
    }
  })
  
  # ===== SCANNER PRODUCT INFO =====
  observeEvent(input$scan_product_info_btn, {
    con <- postgres_con()
    if(is.null(con)) {
      showNotification("Connexion PostgreSQL requise", type = "error")
      return()
    }
    
    showNotification("Scan Product Info en cours...", type = "message")
    product_scan_completed(FALSE)
    
    missing_products <- detect_missing_product_info(con)
    missing_product_info(missing_products)
    product_scan_completed(TRUE)
    
    if(nrow(missing_products) > 0) {
      showNotification(
        paste("Trouvé", nrow(missing_products), "produits sans informations complètes"),
        type = "warning"
      )
    } else {
      showNotification("Tous les produits ont des informations complètes", type = "message")
    }
  })
  
  # ===== TABLEAUX DES DONNÉES MANQUANTES =====
  output$missing_test_info_table <- DT::renderDataTable({
    req(test_scan_completed())
    
    missing_data <- missing_test_info()
    
    if(nrow(missing_data) == 0) {
      data.frame(
        Message = "Aucun test manquant détecté",
        Details = "Tous les tests ont des informations complètes"
      )
    } else {
      missing_data %>%
        select(test_name, source_table, statut) %>%
        DT::datatable(
          options = list(
            pageLength = 10,
            scrollX = TRUE,
            selection = 'single'
          ),
          rownames = FALSE
        )
    }
  })
  
  output$missing_product_info_table <- DT::renderDataTable({
    req(product_scan_completed())
    
    missing_data <- missing_product_info()
    
    if(nrow(missing_data) == 0) {
      data.frame(
        Message = "Aucun produit manquant détecté",
        Details = "Tous les produits ont des informations complètes"
      )
    } else {
      missing_data %>%
        select(nomprod, statut) %>%
        DT::datatable(
          options = list(
            pageLength = 10,
            scrollX = TRUE,
            selection = 'single'
          ),
          rownames = FALSE
        )
    }
  })
  
  # ===== GESTION DU DOUBLE-CLIC POUR TEST INFO =====
  observeEvent(input$missing_test_info_table_cell_clicked, {
    click_info <- input$missing_test_info_table_cell_clicked
    
    if(!is.null(click_info$value) && click_info$col == 0) {
      selected_test <- click_info$value
      
      # Charger le test dans le formulaire
      updateTextInput(session, "test_name_input", value = selected_test)
      updateSelectInput(session, "gmps_type", selected = "")
      updateTextInput(session, "gpms_code", value = "")
      updateTextInput(session, "sc_request", value = "")
      updateTextInput(session, "test_date", value = "")
      updateSelectInput(session, "master_customer_name", selected = "")
      updateSelectInput(session, "country_client", selected = "")
      updateSelectInput(session, "type_of_test", selected = "")
      updateSelectInput(session, "category", selected = "")
      updateSelectInput(session, "subsegment", selected = "")
      updateSelectInput(session, "methodology", selected = "")
      updateSelectInput(session, "panel", selected = "")
      updateSelectInput(session, "test_facilities", selected = "")
      
      # Basculer vers l'onglet de saisie
      updateTabItems(session, "sidebarMenu", "manual_test")
      
      showNotification(
        paste("Test", selected_test, "chargé pour saisie. Complétez les informations !"),
        type = "message"
      )
    }
  })
  
  # ===== GESTION DU DOUBLE-CLIC POUR PRODUCT INFO =====
  observeEvent(input$missing_product_info_table_cell_clicked, {
    click_info <- input$missing_product_info_table_cell_clicked
    
    if(!is.null(click_info$value) && click_info$col == 0) {
      selected_product <- click_info$value
      
      # Charger le produit dans le formulaire
      updateSelectizeInput(session, "nomprod_input", selected = selected_product)
      updateTextInput(session, "code_prod_input", value = "")
      updateSelectizeInput(session, "base_input", selected = "")
      updateSelectInput(session, "ref_input", selected = "")
      updateTextInput(session, "dosage_input", value = "")
      
      # Basculer vers l'onglet de saisie
      updateTabItems(session, "sidebarMenu", "manual_product")
      
      showNotification(
        paste("Produit", selected_product, "chargé pour saisie. Complétez les informations !"),
        type = "message"
      )
    }
  })
  
  # ===== GESTION DES INDICATEURS DE SAISIE =====
  output$current_test_name <- renderText({
    if(!is.null(input$test_name_input) && input$test_name_input != "") {
      input$test_name_input
    } else {
      ""
    }
  })
  
  output$current_product_name <- renderText({
    if(!is.null(input$nomprod_input) && input$nomprod_input != "") {
      input$nomprod_input
    } else {
      ""
    }
  })
  
  # Afficher/masquer les indicateurs
  observe({
    if(!is.null(input$test_name_input) && input$test_name_input != "") {
      shinyjs::show("test_indicator")
    } else {
      shinyjs::hide("test_indicator")
    }
  })
  
  observe({
    if(!is.null(input$nomprod_input) && input$nomprod_input != "") {
      shinyjs::show("product_indicator")
    } else {
      shinyjs::hide("product_indicator")
    }
  })
  
  # ===== SAUVEGARDE TEST INFO AVEC VALIDATION =====
  observeEvent(input$save_test_info_btn, {
    con <- postgres_con()
    if(is.null(con)) {
      showNotification("Connexion PostgreSQL requise", type = "error")
      return()
    }
    
    # Validation des champs obligatoires
    validation_errors <- c()
    
    if(input$test_name_input == "") {
      validation_errors <- c(validation_errors, "Test Name est obligatoire")
    }
    
    if(input$test_date != "" && !validate_date_format(input$test_date)) {
      validation_errors <- c(validation_errors, "Format de date invalide (DD/MM/YYYY)")
    }
    
    if(input$sc_request != "" && !validate_numeric_format(input$sc_request)) {
      validation_errors <- c(validation_errors, "S&C Request doit être numérique")
    }
    
    if(length(validation_errors) > 0) {
      showNotification(
        paste("Erreurs de validation:", paste(validation_errors, collapse = ", ")),
        type = "error",
        duration = 10
      )
      return()
    }
    
    # Vérifier si le test existe déjà
    existing_test_info <- load_test_info_from_postgres(con)
    if(input$test_name_input %in% existing_test_info$test_name) {
      showNotification("Ce test existe déjà dans la base", type = "warning")
      return()
    }
    
    new_test_entry <- data.frame(
      gmps_type = input$gmps_type,
      gpms_code = input$gpms_code,
      sc_request = input$sc_request,
      test_name = input$test_name_input,
      test_date = input$test_date,
      master_customer_name = input$master_customer_name,
      country_client = input$country_client,
      type_of_test = input$type_of_test,
      category = input$category,
      subsegment = input$subsegment,
      methodology = input$methodology,
      panel = input$panel,
      test_facilities = input$test_facilities,
      stringsAsFactors = FALSE
    )
    
    if(save_test_info_to_postgres(con, new_test_entry)) {
      showNotification("Test Info enregistré avec succès", type = "message")
      
      # Vider le formulaire après sauvegarde
      updateTextInput(session, "test_name_input", value = "")
      updateSelectInput(session, "gmps_type", selected = "")
      updateTextInput(session, "gpms_code", value = "")
      updateTextInput(session, "sc_request", value = "")
      updateTextInput(session, "test_date", value = "")
      updateSelectInput(session, "master_customer_name", selected = "")
      updateSelectInput(session, "country_client", selected = "")
      updateSelectInput(session, "type_of_test", selected = "")
      updateSelectInput(session, "category", selected = "")
      updateSelectInput(session, "subsegment", selected = "")
      updateSelectInput(session, "methodology", selected = "")
      updateSelectInput(session, "panel", selected = "")
      updateSelectInput(session, "test_facilities", selected = "")
      
      # Nettoyer les messages de validation
      shinyjs::html("test_validation_messages", "")
    } else {
      showNotification("Erreur lors de la sauvegarde", type = "error")
    }
  })
  
  # ===== SAUVEGARDE PRODUCT INFO AVEC VALIDATION =====
  observeEvent(input$save_product_info_btn, {
    con <- postgres_con()
    if(is.null(con)) {
      showNotification("Connexion PostgreSQL requise", type = "error")
      return()
    }
    
    # Validation des champs obligatoires
    validation_errors <- c()
    
    if(input$nomprod_input == "" || input$code_prod_input == "") {
      validation_errors <- c(validation_errors, "Nom Produit et Code Produit sont obligatoires")
    }
    
    if(input$dosage_input != "" && !validate_percentage_format(input$dosage_input)) {
      validation_errors <- c(validation_errors, "Format de dosage invalide")
    }
    
    if(length(validation_errors) > 0) {
      showNotification(
        paste("Erreurs de validation:", paste(validation_errors, collapse = ", ")),
        type = "error",
        duration = 10
      )
      return()
    }
    
    # Vérifier si le produit existe déjà
    existing_product_info <- load_product_info_from_postgres(con)
    if(input$nomprod_input %in% existing_product_info$nomprod) {
      showNotification("Ce produit existe déjà dans la base", type = "warning")
      return()
    }
    
    new_product_entry <- data.frame(
      code_prod = input$code_prod_input,
      nomprod = input$nomprod_input,
      base = input$base_input,
      ref = input$ref_input,
      dosage = input$dosage_input,
      stringsAsFactors = FALSE
    )
    
    if(save_product_info_to_postgres(con, new_product_entry)) {
      showNotification("Product Info enregistré avec succès", type = "message")
      
      # Vider le formulaire après sauvegarde
      updateTextInput(session, "code_prod_input", value = "")
      updateSelectizeInput(session, "nomprod_input", selected = "")
      updateSelectizeInput(session, "base_input", selected = "")
      updateSelectInput(session, "ref_input", selected = "")
      updateTextInput(session, "dosage_input", value = "")
      
      # Nettoyer les messages de validation
      shinyjs::html("product_validation_messages", "")
    } else {
      showNotification("Erreur lors de la sauvegarde", type = "error")
    }
  })
  
  # ===== FONCTIONNALITÉS SAVE AND NEXT AVEC VALIDATION =====
  observeEvent(input$save_test_and_next_btn, {
    con <- postgres_con()
    if(is.null(con)) {
      showNotification("Connexion PostgreSQL requise", type = "error")
      return()
    }
    
    # Validation des champs obligatoires
    validation_errors <- c()
    
    if(input$test_name_input == "") {
      validation_errors <- c(validation_errors, "Test Name est obligatoire")
    }
    
    if(input$test_date != "" && !validate_date_format(input$test_date)) {
      validation_errors <- c(validation_errors, "Format de date invalide (DD/MM/YYYY)")
    }
    
    if(input$sc_request != "" && !validate_numeric_format(input$sc_request)) {
      validation_errors <- c(validation_errors, "S&C Request doit être numérique")
    }
    
    if(length(validation_errors) > 0) {
      showNotification(
        paste("Erreurs de validation:", paste(validation_errors, collapse = ", ")),
        type = "error",
        duration = 10
      )
      return()
    }
    
    # Sauvegarder le test actuel
    new_test_entry <- data.frame(
      gmps_type = input$gmps_type,
      gpms_code = input$gpms_code,
      sc_request = input$sc_request,
      test_name = input$test_name_input,
      test_date = input$test_date,
      master_customer_name = input$master_customer_name,
      country_client = input$country_client,
      type_of_test = input$type_of_test,
      category = input$category,
      subsegment = input$subsegment,
      methodology = input$methodology,
      panel = input$panel,
      test_facilities = input$test_facilities,
      stringsAsFactors = FALSE
    )
    
    if(save_test_info_to_postgres(con, new_test_entry)) {
      showNotification("Test Info enregistré avec succès", type = "message")
      
      # Passer au test suivant
      missing_tests <- missing_test_info()
      existing_test_info <- load_test_info_from_postgres(con)
      
      # Trouver le prochain test à compléter
      remaining_tests <- missing_tests %>%
        filter(!test_name %in% existing_test_info$test_name)
      
      if(nrow(remaining_tests) > 0) {
        next_test <- remaining_tests$test_name[1]
        
        # Charger le test suivant
        updateTextInput(session, "test_name_input", value = next_test)
        updateSelectInput(session, "gmps_type", selected = "")
        updateTextInput(session, "gpms_code", value = "")
        updateTextInput(session, "sc_request", value = "")
        updateTextInput(session, "test_date", value = "")
        updateSelectInput(session, "master_customer_name", selected = "")
        updateSelectInput(session, "country_client", selected = "")
        updateSelectInput(session, "type_of_test", selected = "")
        updateSelectInput(session, "category", selected = "")
        updateSelectInput(session, "subsegment", selected = "")
        updateSelectInput(session, "methodology", selected = "")
        updateSelectInput(session, "panel", selected = "")
        updateSelectInput(session, "test_facilities", selected = "")
        
        showNotification(
          paste("Test suivant chargé:", next_test),
          type = "message"
        )
      } else {
        # Tous les tests sont complétés
        updateTextInput(session, "test_name_input", value = "")
        updateSelectInput(session, "gmps_type", selected = "")
        updateTextInput(session, "gpms_code", value = "")
        updateTextInput(session, "sc_request", value = "")
        updateTextInput(session, "test_date", value = "")
        updateSelectInput(session, "master_customer_name", selected = "")
        updateSelectInput(session, "country_client", selected = "")
        updateSelectInput(session, "type_of_test", selected = "")
        updateSelectInput(session, "category", selected = "")
        updateSelectInput(session, "subsegment", selected = "")
        updateSelectInput(session, "methodology", selected = "")
        updateSelectInput(session, "panel", selected = "")
        updateSelectInput(session, "test_facilities", selected = "")
        
        showNotification("Tous les tests sont maintenant complétés !", type = "success")
      }
      
      # Nettoyer les messages de validation
      shinyjs::html("test_validation_messages", "")
    } else {
      showNotification("Erreur lors de la sauvegarde", type = "error")
    }
  })
  
  # ===== SAVE AND NEXT POUR PRODUCT INFO =====
  observeEvent(input$save_product_and_next_btn, {
    con <- postgres_con()
    if(is.null(con)) {
      showNotification("Connexion PostgreSQL requise", type = "error")
      return()
    }
    
    # Validation des champs obligatoires
    validation_errors <- c()
    
    if(input$nomprod_input == "" || input$code_prod_input == "") {
      validation_errors <- c(validation_errors, "Nom Produit et Code Produit sont obligatoires")
    }
    
    if(input$dosage_input != "" && !validate_percentage_format(input$dosage_input)) {
      validation_errors <- c(validation_errors, "Format de dosage invalide")
    }
    
    if(length(validation_errors) > 0) {
      showNotification(
        paste("Erreurs de validation:", paste(validation_errors, collapse = ", ")),
        type = "error",
        duration = 10
      )
      return()
    }
    
    # Sauvegarder le produit actuel
    new_product_entry <- data.frame(
      code_prod = input$code_prod_input,
      nomprod = input$nomprod_input,
      base = input$base_input,
      ref = input$ref_input,
      dosage = input$dosage_input,
      stringsAsFactors = FALSE
    )
    
    if(save_product_info_to_postgres(con, new_product_entry)) {
      showNotification("Product Info enregistré avec succès", type = "message")
      
      # Passer au produit suivant
      missing_products <- missing_product_info()
      existing_product_info <- load_product_info_from_postgres(con)
      
      # Trouver le prochain produit à compléter
      remaining_products <- missing_products %>%
        filter(!nomprod %in% existing_product_info$nomprod)
      
      if(nrow(remaining_products) > 0) {
        next_product <- remaining_products$nomprod[1]
        
        # Charger le produit suivant
        updateSelectizeInput(session, "nomprod_input", selected = next_product)
        updateTextInput(session, "code_prod_input", value = "")
        updateSelectizeInput(session, "base_input", selected = "")
        updateSelectInput(session, "ref_input", selected = "")
        updateTextInput(session, "dosage_input", value = "")
        
        showNotification(
          paste("Produit suivant chargé:", next_product),
          type = "message"
        )
      } else {
        # Tous les produits sont complétés
        updateTextInput(session, "code_prod_input", value = "")
        updateSelectizeInput(session, "nomprod_input", selected = "")
        updateSelectizeInput(session, "base_input", selected = "")
        updateSelectInput(session, "ref_input", selected = "")
        updateTextInput(session, "dosage_input", value = "")
        
        showNotification("Tous les produits sont maintenant complétés !", type = "success")
      }
      
      # Nettoyer les messages de validation
      shinyjs::html("product_validation_messages", "")
    } else {
      showNotification("Erreur lors de la sauvegarde", type = "error")
    }
  })
  
  # ===== BOUTONS CLEAR FORM =====
  observeEvent(input$clear_test_form_btn, {
    updateTextInput(session, "test_name_input", value = "")
    updateSelectInput(session, "gmps_type", selected = "")
    updateTextInput(session, "gpms_code", value = "")
    updateTextInput(session, "sc_request", value = "")
    updateTextInput(session, "test_date", value = "")
    updateSelectInput(session, "master_customer_name", selected = "")
    updateSelectInput(session, "country_client", selected = "")
    updateSelectInput(session, "type_of_test", selected = "")
    updateSelectInput(session, "category", selected = "")
    updateSelectInput(session, "subsegment", selected = "")
    updateSelectInput(session, "methodology", selected = "")
    updateSelectInput(session, "panel", selected = "")
    updateSelectInput(session, "test_facilities", selected = "")
    
    # Nettoyer les messages de validation
    shinyjs::html("test_validation_messages", "")
    
    showNotification("Formulaire Test Info vidé", type = "message")
  })
  
  observeEvent(input$clear_product_form_btn, {
    updateTextInput(session, "code_prod_input", value = "")
    updateSelectizeInput(session, "nomprod_input", selected = "")
    updateSelectizeInput(session, "base_input", selected = "")
    updateSelectInput(session, "ref_input", selected = "")
    updateTextInput(session, "dosage_input", value = "")
    
    # Nettoyer les messages de validation
    shinyjs::html("product_validation_messages", "")
    
    showNotification("Formulaire Product Info vidé", type = "message")
  })
  
  # ===== COMPTEURS DE TESTS/PRODUITS RESTANTS =====
  output$remaining_tests_count <- renderText({
    con <- postgres_con()
    if(is.null(con)) return("Connexion requise")
    
    missing_tests <- missing_test_info()
    existing_test_info <- load_test_info_from_postgres(con)
    
    if(nrow(missing_tests) == 0) {
      return("Aucun test en attente")
    }
    
    remaining_count <- missing_tests %>%
      filter(!test_name %in% existing_test_info$test_name) %>%
      nrow()
    
    paste(remaining_count, "test(s) restant(s) à compléter")
  })
  
  output$remaining_products_count <- renderText({
    con <- postgres_con()
    if(is.null(con)) return("Connexion requise")
    
    missing_products <- missing_product_info()
    existing_product_info <- load_product_info_from_postgres(con)
    
    if(nrow(missing_products) == 0) {
      return("Aucun produit en attente")
    }
    
    remaining_count <- missing_products %>%
      filter(!nomprod %in% existing_product_info$nomprod) %>%
      nrow()
    
    paste(remaining_count, "produit(s) restant(s) à compléter")
  })
  
  # ===== OUTPUTS POUR LES CONDITIONS =====
  output$test_scan_completed <- reactive({
    test_scan_completed()
  })
  outputOptions(output, "test_scan_completed", suspendWhenHidden = FALSE)
  
  output$product_scan_completed <- reactive({
    product_scan_completed()
  })
  outputOptions(output, "product_scan_completed", suspendWhenHidden = FALSE)
  
  # ===== TABLES POSTGRESQL =====
  observeEvent(input$refresh_test_info_btn, {
    con <- postgres_con()
    if(is.null(con)) {
      showNotification("Connexion PostgreSQL requise", type = "error")
      return()
    }
    showNotification("Actualisation Test Info...", type = "message")
  })
  
  observeEvent(input$refresh_product_info_btn, {
    con <- postgres_con()
    if(is.null(con)) {
      showNotification("Connexion PostgreSQL requise", type = "error")
      return()
    }
    showNotification("Actualisation Product Info...", type = "message")
  })
  
  output$test_info_table <- DT::renderDataTable({
    con <- postgres_con()
    if(is.null(con)) {
      return(data.frame(Message = "Connexion PostgreSQL requise"))
    }
    
    # Déclencher l'actualisation quand le bouton est cliqué
    input$refresh_test_info_btn
    
    test_info <- load_test_info_from_postgres(con)
    
    if(nrow(test_info) == 0) {
      return(data.frame(Message = "Aucune donnée Test Info trouvée"))
    }
    
    test_info %>%
      DT::datatable(
        options = list(
          pageLength = 15,
          scrollX = TRUE,
          scrollY = "400px",
          searching = TRUE,
          ordering = TRUE
        ),
        rownames = FALSE,
        filter = 'top'
      )
  })
  
  output$product_info_table <- DT::renderDataTable({
    con <- postgres_con()
    if(is.null(con)) {
      return(data.frame(Message = "Connexion PostgreSQL requise"))
    }
    
    # Déclencher l'actualisation quand le bouton est cliqué
    input$refresh_product_info_btn
    
    product_info <- load_product_info_from_postgres(con)
    
    if(nrow(product_info) == 0) {
      return(data.frame(Message = "Aucune donnée Product Info trouvée"))
    }
    
    product_info %>%
      DT::datatable(
        options = list(
          pageLength = 15,
          scrollX = TRUE,
          scrollY = "400px",
          searching = TRUE,
          ordering = TRUE
        ),
        rownames = FALSE,
        filter = 'top'
      )
  })
  
  # ===== DEBUG OUTPUTS =====
  output$debug_postgres_status <- renderText({
    con <- postgres_con()
    if(is.null(con)) {
      return("Connexion PostgreSQL: NON ÉTABLIE")
    }
    
    if(dbIsValid(con)) {
      return(" Connexion PostgreSQL: ACTIVE")
    } else {
      return("️ Connexion PostgreSQL: INVALIDE")
    }
  })
  
  output$debug_postgres_tables <- renderText({
    con <- postgres_con()
    if(is.null(con) || !dbIsValid(con)) {
      return("Connexion requise pour lister les tables")
    }
    
    tryCatch({
      tables <- dbListTables(con)
      if(length(tables) > 0) {
        paste("Tables disponibles (", length(tables), "):\n", 
              paste(tables, collapse = "\n"))
      } else {
        "Aucune table trouvée dans la base"
      }
    }, error = function(e) {
      paste("Erreur listage tables:", e$message)
    })
  })
  
  output$debug_data_stats <- renderText({
    con <- postgres_con()
    if(is.null(con) || !dbIsValid(con)) {
      return("Connexion requise pour les statistiques")
    }
    
    tryCatch({
      stats <- c()
      
      # Stats Test Info
      if(dbExistsTable(con, "testinfo")) {
        test_count <- dbGetQuery(con, "SELECT COUNT(*) as count FROM testinfo")$count
        stats <- c(stats, paste("Test Info:", test_count, "enregistrements"))
      }
      
      # Stats Product Info
      if(dbExistsTable(con, "productinfo")) {
        product_count <- dbGetQuery(con, "SELECT COUNT(*) as count FROM productinfo")$count
        stats <- c(stats, paste("Product Info:", product_count, "enregistrements"))
      }
      
      # Stats Data Brute
      if(dbExistsTable(con, "databrute")) {
        raw_count <- dbGetQuery(con, "SELECT COUNT(*) as count FROM databrute")$count
        stats <- c(stats, paste("Data Brute:", raw_count, "enregistrements"))
      }
      
      if(length(stats) > 0) {
        paste(stats, collapse = "\n")
      } else {
        "Aucune statistique disponible"
      }
      
    }, error = function(e) {
      paste("Erreur calcul statistiques:", e$message)
    })
  })
  
  # ===== TEST CONNEXION POSTGRESQL =====
  observeEvent(input$test_postgres_btn, {
    showNotification("Test de connexion PostgreSQL en cours...", type = "message")
    
    test_con <- create_postgres_connection()
    if(!is.null(test_con)) {
      tryCatch({
        # Test simple de requête
        test_query <- dbGetQuery(test_con, "SELECT version()")
        
        showNotification(
          paste(" Test réussi! Version PostgreSQL:", substr(test_query$version, 1, 50), "..."),
          type = "success",
          duration = 10
        )
        
        safe_disconnect(test_con)
      }, error = function(e) {
        showNotification(
          paste(" Erreur lors du test:", e$message),
          type = "error",
          duration = 10
        )
        safe_disconnect(test_con)
      })
    } else {
      showNotification(" Échec du test de connexion PostgreSQL", type = "error")
    }
  })
  
  # ===== NETTOYAGE À LA FERMETURE =====
  session$onSessionEnded(function() {
    con <- postgres_con()
    if(!is.null(con)) {
      safe_disconnect(con)
      message("Connexion PostgreSQL fermée lors de la fermeture de session")
    }
  })
}

# ===== LANCEMENT DE L'APPLICATION =====
shinyApp(ui = ui, server = server)
