# ===== APPLICATION SHINY - GESTIONNAIRE MÉTADONNÉES AVEC POSTGRESQL =====
# Auteur: Interface utilisateur pour saisie manuelle avec base PostgreSQL
# Date: 2025-08-11 - VERSION POSTGRESQL INTÉGRÉE

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
  library(RPostgres)  #  AJOUT: Connexion PostgreSQL
  library(DBI)        #  AJOUT: Interface base de données
  library(shinyjs)    #  AJOUT: Pour les interactions JavaScript
})

# ===== CONFIGURATION ADAPTÉE AVEC POSTGRESQL =====
RAW_DATA_DIR <- "C:/testManon"
RESULTS_DIR <- "C:/ResultatsAnalyseSenso/Fichiers_Individuels"
METADATA_DB_PATH <- file.path(RESULTS_DIR, "METADATA_PRODUITS_DB.xlsx")
CONSOLIDATED_REPORT_PATH <- file.path("C:/ResultatsAnalyseSenso/RAPPORT_CONSOLIDE", "RAPPORT_CONSOLIDE.xlsx")

# ===== FONCTIONS DE CONNEXION POSTGRESQL =====
create_postgres_connection <- function() {
  tryCatch({
    con <- dbConnect(RPostgres::Postgres(),
                     dbname = "xyz",
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

# ===== FONCTIONS DE GESTION DES TABLES POSTGRESQL =====

# Fonction pour charger les données Test Info depuis PostgreSQL
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
    # Vérifier si la table existe
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

# Fonction pour charger les données Product Info depuis PostgreSQL
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
    # Vérifier si la table existe
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

# Fonction pour sauvegarder Test Info vers PostgreSQL
save_test_info_to_postgres <- function(con, test_info_data) {
  if(is.null(con) || !dbIsValid(con)) return(FALSE)
  
  tryCatch({
    # Nettoyer les noms de colonnes pour PostgreSQL
    test_info_clean <- test_info_data %>%
      rename_with(~str_to_lower(str_replace_all(.x, "[^[:alnum:]_]", "_")))
    
    # Insérer ou mettre à jour
    dbWriteTable(con, "testinfo", test_info_clean, 
                 append = TRUE, row.names = FALSE)
    
    message("Test Info sauvegardé vers PostgreSQL: ", nrow(test_info_clean), " lignes")
    return(TRUE)
  }, error = function(e) {
    message("Erreur sauvegarde Test Info: ", e$message)
    return(FALSE)
  })
}

# Fonction pour sauvegarder Product Info vers PostgreSQL
save_product_info_to_postgres <- function(con, product_info_data) {
  if(is.null(con) || !dbIsValid(con)) return(FALSE)
  
  tryCatch({
    # Nettoyer les noms de colonnes pour PostgreSQL
    product_info_clean <- product_info_data %>%
      rename_with(~str_to_lower(str_replace_all(.x, "[^[:alnum:]_]", "_")))
    
    # Insérer ou mettre à jour
    dbWriteTable(con, "productinfo", product_info_clean, 
                 append = TRUE, row.names = FALSE)
    
    message("Product Info sauvegardé vers PostgreSQL: ", nrow(product_info_clean), " lignes")
    return(TRUE)
  }, error = function(e) {
    message("Erreur sauvegarde Product Info: ", e$message)
    return(FALSE)
  })
}

# ===== FONCTIONS DE DÉTECTION DES DONNÉES MANQUANTES =====

# Fonction pour détecter les Test Info manquants
detect_missing_test_info <- function(con) {
  if(is.null(con) || !dbIsValid(con)) return(data.frame())
  
  tryCatch({
    # Récupérer les données de test existantes depuis les tables d'analyse
    existing_tests <- data.frame()
    
    # Scanner les tables d'analyse pour identifier les tests
    tables_to_scan <- c("strengthtest", "proximity", "triangulaire", "databrute")
    
    for(table_name in tables_to_scan) {
      if(dbExistsTable(con, table_name)) {
        table_data <- dbReadTable(con, table_name)
        
        # Extraire les identifiants de tests
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
    
    # Récupérer les Test Info existants
    existing_test_info <- load_test_info_from_postgres(con)
    
    # Identifier les tests manquants
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

# Fonction pour détecter les Product Info manquants
detect_missing_product_info <- function(con) {
  if(is.null(con) || !dbIsValid(con)) return(data.frame())
  
  tryCatch({
    # Récupérer les produits depuis les données brutes
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
    
    # Récupérer les Product Info existants
    existing_product_info <- load_product_info_from_postgres(con)
    
    # Identifier les produits manquants
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

# ===== INTERFACE UTILISATEUR ÉTENDUE =====
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
    useShinyjs(),  # ✅ AJOUT: Activer shinyjs
    
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
        .status-connected {
          color: #28a745;
          font-weight: bold;
        }
        .status-disconnected {
          color: #dc3545;
          font-weight: bold;
        }
      "))
    ),
    
    tabItems(
      # ===== ONGLET CONNEXION POSTGRESQL =====
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
      
      # ===== ONGLET SCANNER TEST INFO =====
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
      
      # ===== ONGLET SCANNER PRODUCT INFO =====
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
      
      # ===== ONGLET SAISIE TEST INFO =====
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
            
            textInput("test_name_input", "Test Name", 
                      placeholder = "Ex: TEST_2025_001"),
            
            fluidRow(
              column(6,
                     textInput("gmps_type", "GMPS Type", 
                               placeholder = "Ex: FRAGRANCE")
              ),
              column(6,
                     textInput("gpms_code", "GPMS Code", 
                               placeholder = "Ex: GPMS001")
              )
            ),
            
            fluidRow(
              column(6,
                     textInput("sc_request", "S&C Request #", 
                               placeholder = "Ex: REQ-2025-001")
              ),
              column(6,
                     textInput("test_date", "Test Date (DD/MM/YYYY)", 
                               placeholder = "Ex: 15/01/2025")
              )
            ),
            
            fluidRow(
              column(6,
                     textInput("master_customer_name", "Master Customer Name", 
                               placeholder = "Ex: CLIENT_ABC")
              ),
              column(6,
                     textInput("country_client", "Country Client", 
                               placeholder = "Ex: FRANCE")
              )
            ),
            
            fluidRow(
              column(6,
                     selectInput("type_of_test", "Type of Test",
                                 choices = c("", "Triangulaire", "Proximity", "Strength", "Descriptive"),
                                 selected = "")
              ),
              column(6,
                     textInput("category", "Category", 
                               placeholder = "Ex: FINE FRAGRANCE")
              )
            ),
            
            fluidRow(
              column(6,
                     textInput("subsegment", "Subsegment", 
                               placeholder = "Ex: PREMIUM")
              ),
              column(6,
                     textInput("methodology", "Methodology", 
                               placeholder = "Ex: STANDARD")
              )
            ),
            
            fluidRow(
              column(6,
                     textInput("panel", "Panel", 
                               placeholder = "Ex: EXPERT")
              ),
              column(6,
                     textInput("test_facilities", "Test Facilities", 
                               placeholder = "Ex: LAB_A")
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
              tags$li("GMPS TYPE: Type de produit GMPS"),
              tags$li("GPMS CODE: Code GPMS associé"),
              tags$li("S&C REQUEST #: Numéro de demande S&C"),
              tags$li("TEST NAME: Nom unique du test"),
              tags$li("TEST DATE: Date du test (format DD/MM/YYYY)"),
              tags$li("MASTER CUSTOMER NAME: Nom du client principal"),
              tags$li("COUNTRY CLIENT: Pays du client"),
              tags$li("TYPE OF TEST: Type de test sensoriel"),
              tags$li("CATEGORY: Catégorie de produit"),
              tags$li("SUBSEGMENT: Sous-segment"),
              tags$li("METHODOLOGY: Méthodologie utilisée"),
              tags$li("PANEL: Type de panel"),
              tags$li("TEST FACILITIES: Installations de test")
            ),
            
            br(),
            
            h4("Tests restants:"),
            textOutput("remaining_tests_count")
          )
        )
      ),
      
      # ===== ONGLET SAISIE PRODUCT INFO =====
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
            
            textInput("nomprod_input", "Nom Produit", 
                      placeholder = "Ex: MGOB714A04"),
            
            textInput("code_prod_input", "Code Produit", 
                      placeholder = "Ex: MGOB714A04"),
            
            textInput("base_input", "Base", 
                      placeholder = "Ex: B067C016G"),
            
            selectInput("ref_input", "Référence",
                        choices = c("", "Y", "N"),
                        selected = ""),
            
            textInput("dosage_input", "Dosage", 
                      placeholder = "Ex: 1,50%"),
            
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
              tags$li("CODE_PROD: Code unique du produit"),
              tags$li("NOMPROD: Nom du produit"),
              tags$li("BASE: Code de la base utilisée"),
              tags$li("REF: Y si produit de référence, N sinon"),
              tags$li("DOSAGE: Concentration (avec %)")
            ),
            
            br(),
            
            h4("Produits restants:"),
            textOutput("remaining_products_count")
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

# ===== SERVEUR AVEC POSTGRESQL =====
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
  
  # ===== SCANNER PRODUCT INFO (SUITE) =====
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
    
    if(!is.null(click_info$value) && click_info$col == 0) {  # Colonne test_name
      selected_test <- click_info$value
      
      # Charger le test dans le formulaire
      updateTextInput(session, "test_name_input", value = selected_test)
      updateTextInput(session, "gmps_type", value = "")
      updateTextInput(session, "gpms_code", value = "")
      updateTextInput(session, "sc_request", value = "")
      updateTextInput(session, "test_date", value = "")
      updateTextInput(session, "master_customer_name", value = "")
      updateTextInput(session, "country_client", value = "")
      updateSelectInput(session, "type_of_test", selected = "")
      updateTextInput(session, "category", value = "")
      updateTextInput(session, "subsegment", value = "")
      updateTextInput(session, "methodology", value = "")
      updateTextInput(session, "panel", value = "")
      updateTextInput(session, "test_facilities", value = "")
      
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
    
    if(!is.null(click_info$value) && click_info$col == 0) {  # Colonne nomprod
      selected_product <- click_info$value
      
      # Charger le produit dans le formulaire
      updateTextInput(session, "nomprod_input", value = selected_product)
      updateTextInput(session, "code_prod_input", value = "")
      updateTextInput(session, "base_input", value = "")
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
  
  # ===== SAUVEGARDE TEST INFO =====
  observeEvent(input$save_test_info_btn, {
    con <- postgres_con()
    if(is.null(con)) {
      showNotification("Connexion PostgreSQL requise", type = "error")
      return()
    }
    
    if(input$test_name_input == "") {
      showNotification("Test Name est obligatoire", type = "error")
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
      updateTextInput(session, "gmps_type", value = "")
      updateTextInput(session, "gpms_code", value = "")
      updateTextInput(session, "sc_request", value = "")
      updateTextInput(session, "test_date", value = "")
      updateTextInput(session, "master_customer_name", value = "")
      updateTextInput(session, "country_client", value = "")
      updateSelectInput(session, "type_of_test", selected = "")
      updateTextInput(session, "category", value = "")
      updateTextInput(session, "subsegment", value = "")
      updateTextInput(session, "methodology", value = "")
      updateTextInput(session, "panel", value = "")
      updateTextInput(session, "test_facilities", value = "")
    } else {
      showNotification("Erreur lors de la sauvegarde", type = "error")
    }
  })
  
  # ===== SAUVEGARDE PRODUCT INFO =====
  observeEvent(input$save_product_info_btn, {
    con <- postgres_con()
    if(is.null(con)) {
      showNotification("Connexion PostgreSQL requise", type = "error")
      return()
    }
    
    if(input$nomprod_input == "" || input$code_prod_input == "") {
      showNotification("Nom Produit et Code Produit sont obligatoires", type = "error")
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
      updateTextInput(session, "nomprod_input", value = "")
      updateTextInput(session, "code_prod_input", value = "")
      updateTextInput(session, "base_input", value = "")
      updateSelectInput(session, "ref_input", selected = "")
      updateTextInput(session, "dosage_input", value = "")
    } else {
      showNotification("Erreur lors de la sauvegarde", type = "error")
    }
  })
  
  # ===== FONCTIONNALITÉS SAVE AND NEXT =====
  observeEvent(input$save_test_and_next_btn, {
    con <- postgres_con()
    if(is.null(con)) {
      showNotification("Connexion PostgreSQL requise", type = "error")
      return()
    }
    
    if(input$test_name_input == "") {
      showNotification("Test Name est obligatoire", type = "error")
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
      
      remaining_tests <- missing_tests %>%
        filter(!test_name %in% existing_test_info$test_name)
      
      if(nrow(remaining_tests) > 0) {
        next_test <- remaining_tests[1, "test_name"]
        updateTextInput(session, "test_name_input", value = next_test)
        # Vider les autres champs
        updateTextInput(session, "gmps_type", value = "")
        updateTextInput(session, "gpms_code", value = "")
        updateTextInput(session, "sc_request", value = "")
        updateTextInput(session, "test_date", value = "")
        updateTextInput(session, "master_customer_name", value = "")
        updateTextInput(session, "country_client", value = "")
        updateSelectInput(session, "type_of_test", selected = "")
        updateTextInput(session, "category", value = "")
        updateTextInput(session, "subsegment", value = "")
        updateTextInput(session, "methodology", value = "")
        updateTextInput(session, "panel", value = "")
        updateTextInput(session, "test_facilities", value = "")
        
        showNotification(paste("Test suivant chargé:", next_test), type = "message")
      } else {
        # Tous les tests sont complétés
        updateTextInput(session, "test_name_input", value = "")
        showNotification("🎉 Tous les tests ont été complétés !", type = "message")
      }
    } else {
      showNotification("Erreur lors de la sauvegarde", type = "error")
    }
  })
  
  observeEvent(input$save_product_and_next_btn, {
    con <- postgres_con()
    if(is.null(con)) {
      showNotification("Connexion PostgreSQL requise", type = "error")
      return()
    }
    
    if(input$nomprod_input == "" || input$code_prod_input == "") {
      showNotification("Nom Produit et Code Produit sont obligatoires", type = "error")
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
      
      remaining_products <- missing_products %>%
        filter(!nomprod %in% existing_product_info$nomprod)
      
      if(nrow(remaining_products) > 0) {
        next_product <- remaining_products[1, "nomprod"]
        updateTextInput(session, "nomprod_input", value = next_product)
        updateTextInput(session, "code_prod_input", value = "")
        updateTextInput(session, "base_input", value = "")
        updateSelectInput(session, "ref_input", selected = "")
        updateTextInput(session, "dosage_input", value = "")
        
        showNotification(paste("Produit suivant chargé:", next_product), type = "message")
      } else {
        # Tous les produits sont complétés
        updateTextInput(session, "nomprod_input", value = "")
        showNotification("🎉 Tous les produits ont été complétés !", type = "message")
      }
    } else {
      showNotification("Erreur lors de la sauvegarde", type = "error")
    }
  })
  
  # ===== FONCTIONS CLEAR FORM =====
  observeEvent(input$clear_test_form_btn, {
    updateTextInput(session, "test_name_input", value = "")
    updateTextInput(session, "gmps_type", value = "")
    updateTextInput(session, "gpms_code", value = "")
    updateTextInput(session, "sc_request", value = "")
    updateTextInput(session, "test_date", value = "")
    updateTextInput(session, "master_customer_name", value = "")
    updateTextInput(session, "country_client", value = "")
    updateSelectInput(session, "type_of_test", selected = "")
    updateTextInput(session, "category", value = "")
    updateTextInput(session, "subsegment", value = "")
    updateTextInput(session, "methodology", value = "")
    updateTextInput(session, "panel", value = "")
    updateTextInput(session, "test_facilities", value = "")
    showNotification("Formulaire Test Info vidé", type = "message")
  })
  
  observeEvent(input$clear_product_form_btn, {
    updateTextInput(session, "nomprod_input", value = "")
    updateTextInput(session, "code_prod_input", value = "")
    updateTextInput(session, "base_input", value = "")
    updateSelectInput(session, "ref_input", selected = "")
    updateTextInput(session, "dosage_input", value = "")
    showNotification("Formulaire Product Info vidé", type = "message")
  })
  
  # ===== COMPTEURS RESTANTS =====
  output$remaining_tests_count <- renderText({
    missing_tests <- missing_test_info()
    con <- postgres_con()
    
    if(nrow(missing_tests) > 0 && !is.null(con)) {
      existing_test_info <- load_test_info_from_postgres(con)
      remaining <- missing_tests %>%
        filter(!test_name %in% existing_test_info$test_name)
      paste("Tests restants à compléter:", nrow(remaining))
    } else {
      "Aucun scan effectué"
    }
  })
  
  output$remaining_products_count <- renderText({
    missing_products <- missing_product_info()
    con <- postgres_con()
    
    if(nrow(missing_products) > 0 && !is.null(con)) {
      existing_product_info <- load_product_info_from_postgres(con)
      remaining <- missing_products %>%
        filter(!nomprod %in% existing_product_info$nomprod)
      paste("Produits restants à compléter:", nrow(remaining))
    } else {
      "Aucun scan effectué"
    }
  })
  
  # ===== TABLEAUX POSTGRESQL =====
  observeEvent(input$refresh_test_info_btn, {
    con <- postgres_con()
    if(!is.null(con)) {
      showNotification("Table Test Info actualisée", type = "message")
    }
  })
  
  observeEvent(input$refresh_product_info_btn, {
    con <- postgres_con()
    if(!is.null(con)) {
      showNotification("Table Product Info actualisée", type = "message")
    }
  })
  
  output$test_info_table <- DT::renderDataTable({
    con <- postgres_con()
    if(!is.null(con)) {
      test_info_data <- load_test_info_from_postgres(con)
      if(nrow(test_info_data) > 0) {
        test_info_data %>%
          DT::datatable(
            options = list(
              pageLength = 15,
              scrollX = TRUE,
              selection = 'multiple'
            ),
            rownames = FALSE
          )
      } else {
        data.frame(Message = "Aucune donnée Test Info trouvée")
      }
    } else {
      data.frame(Message = "Connexion PostgreSQL requise")
    }
  })
  
  output$product_info_table <- DT::renderDataTable({
    con <- postgres_con()
    if(!is.null(con)) {
      product_info_data <- load_product_info_from_postgres(con)
      if(nrow(product_info_data) > 0) {
        product_info_data %>%
          DT::datatable(
            options = list(
              pageLength = 15,
              scrollX = TRUE,
              selection = 'multiple'
            ),
            rownames = FALSE
          )
      } else {
        data.frame(Message = "Aucune donnée Product Info trouvée")
      }
    } else {
      data.frame(Message = "Connexion PostgreSQL requise")
    }
  })
  
  # ===== OUTPUTS RÉACTIFS =====
  output$test_scan_completed <- reactive({
    test_scan_completed()
  })
  outputOptions(output, "test_scan_completed", suspendWhenHidden = FALSE)
  
  output$product_scan_completed <- reactive({
    product_scan_completed()
  })
  outputOptions(output, "product_scan_completed", suspendWhenHidden = FALSE)
  
  # ===== DEBUG POSTGRESQL =====
  output$debug_postgres_status <- renderText({
    con <- postgres_con()
    if(!is.null(con) && dbIsValid(con)) {
      paste(
        "Connexion: ✓ Active",
        "Host: emfrndsunx574.emea.sesam.mane.com",
        "Database: xyz",
        "Status: Connecté",
        sep = "\n"
      )
    } else {
      "Connexion: ✗ Non établie"
    }
  })
  
  output$debug_postgres_tables <- renderText({
    con <- postgres_con()
    if(!is.null(con) && dbIsValid(con)) {
      tryCatch({
        tables <- dbListTables(con)
        paste(
          "Tables détectées:",
          paste(tables, collapse = ", "),
          "",
          "Tables cibles:",
          "- testinfo:", ifelse("testinfo" %in% tables, "✓", "✗"),
          "- productinfo:", ifelse("productinfo" %in% tables, "✓", "✗"),
          "- databrute:", ifelse("databrute" %in% tables, "✓", "✗"),
          sep = "\n"
        )
      }, error = function(e) {
        paste("Erreur:", e$message)
      })
    } else {
      "Connexion requise"
    }
  })
  
  output$debug_data_stats <- renderText({
    con <- postgres_con()
    if(!is.null(con) && dbIsValid(con)) {
      tryCatch({
        test_info_count <- if(dbExistsTable(con, "testinfo")) {
          nrow(dbReadTable(con, "testinfo"))
        } else { 0 }
        
        product_info_count <- if(dbExistsTable(con, "productinfo")) {
          nrow(dbReadTable(con, "productinfo"))
        } else { 0 }
        
        raw_data_count <- if(dbExistsTable(con, "databrute")) {
          nrow(dbReadTable(con, "databrute"))
        } else { 0 }
        
        paste(
          "=== STATISTIQUES DONNÉES ===",
          paste("Test Info:", test_info_count, "entrées"),
          paste("Product Info:", product_info_count, "entrées"),
          paste("Données brutes:", raw_data_count, "entrées"),
          "",
          "=== DONNÉES MANQUANTES ===",
          paste("Tests manquants:", nrow(missing_test_info())),
          paste("Produits manquants:", nrow(missing_product_info())),
          sep = "\n"
        )
      }, error = function(e) {
        paste("Erreur statistiques:", e$message)
      })
    } else {
      "Connexion requise"
    }
  })
  
  observeEvent(input$test_postgres_btn, {
    con <- create_postgres_connection()
    if(!is.null(con)) {
      tables <- dbListTables(con)
      safe_disconnect(con)
      showNotification(
        paste("Test réussi -", length(tables), "tables détectées"),
        type = "message"
      )
    } else {
      showNotification("Test échoué - Vérifiez la connexion", type = "error")
    }
  })
  
  # ===== NETTOYAGE À LA FERMETURE =====
  session$onSessionEnded(function() {
    con <- postgres_con()
    if(!is.null(con)) {
      safe_disconnect(con)
    }
  })
}

# ===== LANCEMENT DE L'APPLICATION =====
shinyApp(ui = ui, server = server)
