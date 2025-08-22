# ===== APPLICATION SHINY - GESTIONNAIRE MÉTADONNÉES SA_METADATA =====
# Auteur: Interface utilisateur pour saisie manuelle avec base PostgreSQL SA_METADATA
# Date: 2025-08-13 - VERSION RACCORDÉE AVEC SCRIPT D'ANALYSE

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

# ===== LISTES DE VALEURS MÉTIER (INCHANGÉES) =====

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

SCALE_CHOICES <- c("", "1 to 5", "0 to 10")

# Liste REF (Product Info)
REF_CHOICES <- c("", "Y", "N")

# ===== CONFIGURATION RACCORDÉE AVEC LE SCRIPT D'ANALYSE =====
DB_CONFIG <- list(
  host = "emfrndsunx574.emea.sesam.mane.com",
  port = 5432,
  user = "dbadmin",
  password = "Azerty06*"
)

# Base de données utilisée (même que le script d'analyse)
METADATA_DATABASE <- "SA_METADATA"
RESULTS_DATABASE <- "SA_RESULTS_DATA"
# ===== FONCTIONS DE CONNEXION POSTGRESQL RACCORDÉES =====
create_postgres_connection <- function() {
  tryCatch({
    con <- dbConnect(RPostgres::Postgres(),
                     dbname = METADATA_DATABASE,
                     host = DB_CONFIG$host,
                     port = DB_CONFIG$port,
                     user = DB_CONFIG$user,
                     password = DB_CONFIG$password)
    message("✅ Connexion PostgreSQL établie avec succès à SA_METADATA")
    return(con)
  }, error = function(e) {
    message("❌ ERREUR connexion PostgreSQL: ", e$message)
    return(NULL)
  })
}

safe_disconnect <- function(con) {
  if(!is.null(con) && dbIsValid(con)) {
    dbDisconnect(con)
    message("🔌 Connexion PostgreSQL fermée")
  }
}
# ===== FONCTION DE CONNEXION POUR SA_RESULTS_DATA =====
create_results_connection <- function() {
  tryCatch({
    con <- dbConnect(RPostgres::Postgres(),
                     dbname = RESULTS_DATABASE,
                     host = DB_CONFIG$host,
                     port = DB_CONFIG$port,
                     user = DB_CONFIG$user,
                     password = DB_CONFIG$password)
    message("✅ Connexion PostgreSQL établie avec succès à SA_RESULTS_DATA")
    return(con)
  }, error = function(e) {
    message("❌ ERREUR connexion PostgreSQL SA_RESULTS_DATA", e$message)
    return(NULL)
  })
}
create_test_info_table <- function(con) {
  if(is.null(con)) return(FALSE)
  
  tryCatch({
    if(dbExistsTable(con, "test_info")) {
      message("📋 Table test_info existe déjà")
      
      # ✅ VÉRIFIER ET AJOUTER LA COLONNE SCALE SI MANQUANTE
      columns <- dbListFields(con, "test_info")
      if(!"scale" %in% columns) {
        message("🔧 Ajout de la colonne 'scale' à la table existante...")
        dbExecute(con, "ALTER TABLE test_info ADD COLUMN scale VARCHAR(100)")
        message("✅ Colonne 'scale' ajoutée avec succès")
      }
      
      return(TRUE)
    }
    
    # Création complète si la table n'existe pas
    create_sql <- "
    CREATE TABLE IF NOT EXISTS test_info (
      id SERIAL PRIMARY KEY,
      source_name VARCHAR(255) NOT NULL,
      test_name VARCHAR(255) NOT NULL,
      gmps_type VARCHAR(10),
      gpms_code VARCHAR(100),
      sc_request VARCHAR(50),
      test_date VARCHAR(20),
      master_customer_name VARCHAR(255),
      country_client VARCHAR(100),
      type_of_test VARCHAR(50),
      category VARCHAR(100),
      subsegment VARCHAR(100),
      methodology VARCHAR(100),
      panel VARCHAR(100),
      test_facilities VARCHAR(100),
      scale VARCHAR(100),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE INDEX IF NOT EXISTS idx_test_info_source ON test_info(source_name);
    CREATE INDEX IF NOT EXISTS idx_test_info_test_name ON test_info(test_name);
    CREATE UNIQUE INDEX IF NOT EXISTS idx_test_info_unique ON test_info(source_name, test_name);
    "
    
    dbExecute(con, create_sql)
    message("✅ Table test_info créée avec succès")
    return(TRUE)
    
  }, error = function(e) {
    message("❌ Erreur création table test_info : ", e$message)
    return(FALSE)
  })
}


create_product_info_metadata_table <- function(con) {
  if(is.null(con)) return(FALSE)
  
  tryCatch({
    if(dbExistsTable(con, "product_info_metadata")) {  # ✅ MINUSCULES
      message("📋 Table product_info_metadata existe déjà")
      return(TRUE)
    }
    
    create_sql <- "
    CREATE TABLE IF NOT EXISTS product_info_metadata (
      id SERIAL PRIMARY KEY,
      source_name VARCHAR(255) NOT NULL,
      product_name VARCHAR(255) NOT NULL,
      code_prod VARCHAR(255),
      base VARCHAR(255),
      ref VARCHAR(10),
      dosage VARCHAR(50),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE INDEX IF NOT EXISTS idx_product_info_meta_source ON product_info_metadata(source_name);
    CREATE INDEX IF NOT EXISTS idx_product_info_meta_product ON product_info_metadata(product_name);
    CREATE UNIQUE INDEX IF NOT EXISTS idx_product_info_meta_unique ON product_info_metadata(source_name, product_name);
    "
    
    dbExecute(con, create_sql)
    message("✅ Table product_info_metadata créée avec succès")
    return(TRUE)
    
  }, error = function(e) {
    message("❌ Erreur création table product_info_metadata : ", e$message)
    return(FALSE)
  })
}


# ===== FONCTIONS DE VALIDATION (INCHANGÉES) =====

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

# ===== FONCTIONS DE GESTION DES TABLES RACCORDÉES =====
# ===== FONCTIONS DE GESTION DES TABLES RACCORDÉES (CORRIGÉES) =====
load_test_info_from_postgres <- function(con) {
  if(is.null(con) || !dbIsValid(con)) {
    return(data.frame(
      source_name = character(0),
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
    create_test_info_table(con)
    
    if(dbExistsTable(con, "test_info")) {  # ✅ MINUSCULES
      test_info <- dbReadTable(con, "test_info")  # ✅ MINUSCULES
      message("📊 Test Info chargé depuis SA_METADATA: ", nrow(test_info), " lignes")
      return(test_info)
    } else {
      message("⚠️ Table 'test_info' non trouvée dans SA_METADATA")
      return(data.frame())
    }
  }, error = function(e) {
    message("❌ Erreur lecture Test Info: ", e$message)
    return(data.frame())
  })
}

load_product_info_metadata_from_postgres <- function(con) {
  if(is.null(con) || !dbIsValid(con)) {
    return(data.frame(
      source_name = character(0),
      product_name = character(0),
      code_prod = character(0),
      base = character(0),
      ref = character(0),
      dosage = character(0),
      stringsAsFactors = FALSE
    ))
  }
  
  tryCatch({
    create_product_info_metadata_table(con)
    
    if(dbExistsTable(con, "product_info_metadata")) {  # ✅ MINUSCULES
      product_info <- dbReadTable(con, "product_info_metadata")  # ✅ MINUSCULES
      message("📊 Product Info Metadata chargé depuis SA_METADATA: ", nrow(product_info), " lignes")
      return(product_info)
    } else {
      message("⚠️ Table 'product_info_metadata' non trouvée dans SA_METADATA")
      return(data.frame())
    }
  }, error = function(e) {
    message("❌ Erreur lecture Product Info Metadata: ", e$message)
    return(data.frame())
  })
}

# Fonction pour obtenir les noms de produits uniques depuis product_info (créée par le script)
get_unique_product_names <- function(con) {
  if(is.null(con) || !dbIsValid(con)) return(c(""))
  
  tryCatch({
    if(dbExistsTable(con, "product_info")) {  # ✅ MINUSCULES
      raw_data <- dbReadTable(con, "product_info")  # ✅ MINUSCULES
      if("product_name" %in% names(raw_data)) {
        unique_products <- unique(raw_data$product_name)
        unique_products <- unique_products[!is.na(unique_products) & unique_products != ""]
        message("📋 Produits uniques chargés depuis product_info: ", length(unique_products))
        return(c("", sort(unique_products)))
      }
    }
    return(c(""))
  }, error = function(e) {
    message("❌ Erreur récupération noms produits: ", e$message)
    return(c(""))
  })
}

# Fonction pour obtenir les sources uniques depuis product_info (créée par le script)
get_unique_sources <- function(con) {
  if(is.null(con) || !dbIsValid(con)) return(c(""))
  
  tryCatch({
    if(dbExistsTable(con, "product_info")) {  # ✅ MINUSCULES
      raw_data <- dbReadTable(con, "product_info")  # ✅ MINUSCULES
      if("source_name" %in% names(raw_data)) {
        unique_sources <- unique(raw_data$source_name)
        unique_sources <- unique_sources[!is.na(unique_sources) & unique_sources != ""]
        message("📋 Sources uniques chargées depuis product_info: ", length(unique_sources))
        return(c("", sort(unique_sources)))
      }
    }
    return(c(""))
  }, error = function(e) {
    message("❌ Erreur récupération sources: ", e$message)
    return(c(""))
  })
}

# Fonction pour obtenir les bases uniques depuis product_info (CORRIGÉE)
get_unique_bases <- function(con) {
  if(is.null(con) || !dbIsValid(con)) return(c(""))
  
  tryCatch({
    # CORRECTION: Chercher d'abord dans product_info (du script)
    if(dbExistsTable(con, "product_info")) {  # ✅ MINUSCULES
      raw_data <- dbReadTable(con, "product_info")  # ✅ MINUSCULES
      if("base" %in% names(raw_data)) {
        unique_bases <- unique(raw_data$base)
        unique_bases <- unique_bases[!is.na(unique_bases) & unique_bases != ""]
        if(length(unique_bases) > 0) {
          return(c("", sort(unique_bases)))
        }
      }
    }
    
    # Si pas de bases dans product_info, chercher dans product_info_metadata
    if(dbExistsTable(con, "product_info_metadata")) {  # ✅ MINUSCULES
      product_info <- dbReadTable(con, "product_info_metadata")  # ✅ MINUSCULES
      if("base" %in% names(product_info)) {
        unique_bases <- unique(product_info$base)
        unique_bases <- unique_bases[!is.na(unique_bases) & unique_bases != ""]
        return(c("", sort(unique_bases)))
      }
    }
    
    return(c(""))
  }, error = function(e) {
    message("❌ Erreur récupération bases: ", e$message)
    return(c(""))
  })
}
# ===== FONCTION POUR CHARGER LES SOURCES UNIQUES DES RÉSULTATS (CORRIGÉE) =====
get_unique_sources_from_results <- function(table_name) {
  # Validation du nom de table
  if(is.null(table_name) || table_name == "" || is.na(table_name)) {
    message("❌ Nom de table invalide: '", table_name, "'")
    return(c(""))
  }
  
  # Créer une connexion temporaire à SA_RESULTS_DATA
  con_results <- create_results_connection()
  if(is.null(con_results)) {
    message("❌ Impossible de se connecter à SA_RESULTS_DATA")
    return(c(""))
  }
  
  tryCatch({
    # Vérifier que la table existe
    if(!dbExistsTable(con_results, table_name)) {
      message("⚠️ Table '", table_name, "' non trouvée dans SA_RESULTS_DATA")
      return(c(""))
    }
    
    # Vérifier que la table a des données
    count_query <- paste0("SELECT COUNT(*) as count FROM ", table_name)
    count_result <- dbGetQuery(con_results, count_query)
    
    if(count_result$count == 0) {
      message("⚠️ Table '", table_name, "' est vide")
      return(c(""))
    }
    
    # Récupérer les sources uniques
    sources_query <- paste0("SELECT DISTINCT source_name FROM ", table_name, 
                            " WHERE source_name IS NOT NULL AND source_name != '' ORDER BY source_name")
    sources <- dbGetQuery(con_results, sources_query)
    
    if(nrow(sources) > 0) {
      unique_sources <- sources$source_name
      unique_sources <- unique_sources[!is.na(unique_sources) & unique_sources != ""]
      message("📋 Sources trouvées dans '", table_name, "': ", length(unique_sources))
      return(c("", unique_sources))
    } else {
      message("⚠️ Aucune source trouvée dans '", table_name, "'")
      return(c(""))
    }
    
  }, error = function(e) {
    message("❌ Erreur récupération sources depuis '", table_name, "': ", e$message)
    return(c(""))
  }, finally = {
    safe_disconnect(con_results)
  })
}


save_test_info_to_postgres <- function(con, test_info_data) {
  if(is.null(con) || !dbIsValid(con)) return(FALSE)
  
  tryCatch({
    create_test_info_table(con)
    
    # Vérifier si l'entrée existe déjà
    existing_check <- dbGetQuery(con, 
                                 "SELECT COUNT(*) as count FROM test_info WHERE source_name = $1 AND test_name = $2",  # ✅ MINUSCULES
                                 params = list(test_info_data$source_name, test_info_data$test_name))
    
    if(existing_check$count > 0) {
      # Mettre à jour l'entrée existante
      update_sql <- "
      UPDATE test_info SET  
        gmps_type = $3,
        gpms_code = $4,
        sc_request = $5,
        test_date = $6,
        master_customer_name = $7,
        country_client = $8,
        type_of_test = $9,
        category = $10,
        subsegment = $11,
        methodology = $12,
        panel = $13,
        test_facilities = $14,
        scale = $15,
        updated_at = CURRENT_TIMESTAMP
      WHERE source_name = $1 AND test_name = $2
      "
      
      dbExecute(con, update_sql, params = list(
        test_info_data$source_name,
        test_info_data$test_name,
        test_info_data$gmps_type,
        test_info_data$gpms_code,
        test_info_data$sc_request,
        test_info_data$test_date,
        test_info_data$master_customer_name,
        test_info_data$country_client,
        test_info_data$type_of_test,
        test_info_data$category,
        test_info_data$subsegment,
        test_info_data$methodology,
        test_info_data$panel,
        test_info_data$test_facilities,
        test_info_data$scale
      ))
      
      message("✅ Test Info mis à jour dans SA_METADATA")
    } else {
      # Insérer nouvelle entrée
      dbWriteTable(con, "test_info", test_info_data,  # ✅ MINUSCULES
                   append = TRUE, row.names = FALSE)
      message("✅ Test Info sauvegardé vers SA_METADATA: ", nrow(test_info_data), " lignes")
    }
    
    return(TRUE)
  }, error = function(e) {
    message("❌ Erreur sauvegarde Test Info: ", e$message)
    return(FALSE)
  })
}

# ===== FONCTION UNIQUE POUR SAUVEGARDER DANS product_info (minuscules) =====
save_product_info_to_postgres <- function(con, product_info_data) {
  if(is.null(con) || !dbIsValid(con)) return(FALSE)
  
  tryCatch({
    # Mettre à jour directement dans product_info (créée par le script)
    update_sql <- "
    UPDATE product_info SET  
      code_prod = $3,
      base = $4,
      ref = $5,
      dosage = $6,
      internal_name_additional_product = $7,
      perfumer_code_additional_product = $8,
      internal_code_additional_product = $9,
      external_name_additional_product = $10,
      external_code_additional_product = $11,
      dosage_additional_product = $12
    WHERE source_name = $1 AND product_name = $2
    "
    
    result <- dbExecute(con, update_sql, params = list(
      product_info_data$source_name,
      product_info_data$product_name,
      product_info_data$code_prod,
      product_info_data$base,
      product_info_data$ref,
      product_info_data$dosage,
      product_info_data$internal_name_additional_product,
      product_info_data$perfumer_code_additional_product,
      product_info_data$internal_code_additional_product,
      product_info_data$external_name_additional_product,
      product_info_data$external_code_additional_product,
      product_info_data$dosage_additional_product
    ))
    
    if(result > 0) {
      message("✅ Product Info mis à jour dans product_info (", result, " ligne(s))")
      return(TRUE)
    } else {
      message("⚠️ Aucune ligne mise à jour - vérifiez source_name et product_name")
      return(FALSE)
    }
    
  }, error = function(e) {
    message("❌ Erreur sauvegarde Product Info: ", e$message)
    return(FALSE)
  })
}

# ===== FONCTION UNIQUE POUR DÉTECTER LES TEST INFO MANQUANTS =====
detect_missing_test_info <- function(con) {
  if(is.null(con) || !dbIsValid(con)) return(data.frame())
  
  tryCatch({
    # Récupérer tous les tests depuis product_info (créée par le script d'analyse)
    if(!dbExistsTable(con, "product_info")) {
      message("⚠️ Product_info table not found")
      return(data.frame())
    }
    
    product_data <- dbReadTable(con, "product_info")
    
    if(nrow(product_data) == 0) {
      message("⚠️ Product_info table empty")
      return(data.frame())
    }
    
    # Extraire les tests uniques depuis product_info
    existing_tests <- product_data %>%
      select(source_name) %>%
      distinct() %>%
      mutate(
        test_name = source_name  # Le source_name correspond au test_name
      )
    
    if(nrow(existing_tests) == 0) {
      message("⚠️ Aucun test trouvé dans product_info")
      return(data.frame())
    }
    
    # Charger les test_info existants
    existing_test_info <- load_test_info_from_postgres(con)
    
    if(nrow(existing_test_info) == 0) {
      # Si aucun test_info n'existe, tous les tests de product_info sont manquants
      missing_tests <- existing_tests %>%
        mutate(
          gmps_type = "",
          gpms_code = "",
          sc_request = "",
          test_date = "",
          master_customer_name = "",
          country_client = "",
          type_of_test = "",
          category = "",
          subsegment = "",
          methodology = "",
          panel = "",
          test_facilities = "",
          scale = "",  # ✅ AJOUT EXPLICITE
          statut = "To complete"
        )
    } else {
      # ✅ VÉRIFIER QUELLES COLONNES EXISTENT AVANT DE LES UTILISER
      columns_to_check <- c("gmps_type", "gpms_code", "test_date", "master_customer_name", 
                            "country_client", "type_of_test", "category", "subsegment", 
                            "methodology", "panel", "test_facilities", "scale")
      
      # Ajouter les colonnes manquantes avec des valeurs vides
      for(col in columns_to_check) {
        if(!col %in% names(existing_test_info)) {
          existing_test_info[[col]] <- ""
          message("🔧 Colonne '", col, "' ajoutée temporairement pour la détection")
        }
      }
      
      # Maintenant on peut filtrer en toute sécurité
      missing_tests <- existing_test_info %>%
        filter(
          (is.null(gmps_type) | is.na(gmps_type) | gmps_type == "" | str_trim(gmps_type) == "") |
            (is.null(gpms_code) | is.na(gpms_code) | gpms_code == "" | str_trim(gpms_code) == "") |
            (is.null(test_date) | is.na(test_date) | test_date == "" | str_trim(test_date) == "") |
            (is.null(master_customer_name) | is.na(master_customer_name) | master_customer_name == "" | str_trim(master_customer_name) == "") |
            (is.null(country_client) | is.na(country_client) | country_client == "" | str_trim(country_client) == "") |
            (is.null(type_of_test) | is.na(type_of_test) | type_of_test == "" | str_trim(type_of_test) == "") |
            (is.null(category) | is.na(category) | category == "" | str_trim(category) == "") |
            (is.null(subsegment) | is.na(subsegment) | subsegment == "" | str_trim(subsegment) == "") |
            (is.null(methodology) | is.na(methodology) | methodology == "" | str_trim(methodology) == "") |
            (is.null(panel) | is.na(panel) | panel == "" | str_trim(panel) == "") |
            (is.null(test_facilities) | is.na(test_facilities) | test_facilities == "" | str_trim(test_facilities) == "") | 
            (is.null(scale) | is.na(scale) | scale == "" | str_trim(scale) == "")
        ) %>%
        select(source_name, test_name, gmps_type, gpms_code, sc_request, test_date, 
               master_customer_name, country_client, type_of_test, category, 
               subsegment, methodology, panel, test_facilities, scale) %>%
        distinct()
      
      # Ajouter les tests de product_info qui n'existent pas du tout dans test_info
      tests_not_in_test_info <- existing_tests %>%
        anti_join(existing_test_info, by = c("source_name", "test_name")) %>%
        mutate(
          gmps_type = "",
          gpms_code = "",
          sc_request = "",
          test_date = "",
          master_customer_name = "",
          country_client = "",
          type_of_test = "",
          category = "",
          subsegment = "",
          methodology = "",
          panel = "",
          test_facilities = "",
          scale = ""
        )
      
      # Combiner les deux types de tests manquants
      missing_tests <- bind_rows(missing_tests, tests_not_in_test_info)
    }
    
    # Nettoyer les valeurs pour l'affichage
    # Nettoyer les valeurs pour l'affichage ET CALCULER LE STATUT DYNAMIQUEMENT
    if(nrow(missing_tests) > 0) {
      missing_tests <- missing_tests %>%
        mutate(
          # Nettoyer les valeurs NULL/NA pour l'affichage
          gmps_type = ifelse(is.na(gmps_type) | is.null(gmps_type), "", as.character(gmps_type)),
          gpms_code = ifelse(is.na(gpms_code) | is.null(gpms_code), "", as.character(gpms_code)),
          sc_request = ifelse(is.na(sc_request) | is.null(sc_request), "", as.character(sc_request)),
          test_date = ifelse(is.na(test_date) | is.null(test_date), "", as.character(test_date)),
          master_customer_name = ifelse(is.na(master_customer_name) | is.null(master_customer_name), "", as.character(master_customer_name)),
          country_client = ifelse(is.na(country_client) | is.null(country_client), "", as.character(country_client)),
          type_of_test = ifelse(is.na(type_of_test) | is.null(type_of_test), "", as.character(type_of_test)),
          category = ifelse(is.na(category) | is.null(category), "", as.character(category)),
          subsegment = ifelse(is.na(subsegment) | is.null(subsegment), "", as.character(subsegment)),
          methodology = ifelse(is.na(methodology) | is.null(methodology), "", as.character(methodology)),
          panel = ifelse(is.na(panel) | is.null(panel), "", as.character(panel)),
          test_facilities = ifelse(is.na(test_facilities) | is.null(test_facilities), "", as.character(test_facilities)),
          scale = ifelse(is.na(scale) | is.null(scale), "", as.character(scale))
        ) %>%
        # ✅ CALCULER LE STATUT DYNAMIQUEMENT APRÈS NETTOYAGE
        mutate(
          statut = case_when(
            # Vérifier si TOUS les champs obligatoires sont remplis
            str_trim(gmps_type) != "" & 
              str_trim(test_date) != "" & 
              str_trim(master_customer_name) != "" & 
              str_trim(country_client) != "" & 
              str_trim(type_of_test) != "" & 
              str_trim(category) != "" & 
              str_trim(subsegment) != "" & 
              str_trim(methodology) != "" & 
              str_trim(panel) != "" & 
              str_trim(test_facilities) != "" & 
              str_trim(scale) != "" ~ "Complete",
            
            # Sinon, à compléter
            TRUE ~ "To complete"
          )
        )
    }
    
    message("🔍 Tests avec champs vides détectés: ", nrow(missing_tests))
    return(missing_tests)
    
  }, error = function(e) {
    message("❌ Erreur détection Test Info manquants: ", e$message)
    return(data.frame())
  })
}


# ===== FONCTION UNIQUE POUR DÉTECTER LES PRODUCT INFO MANQUANTS =====
detect_missing_product_info <- function(con) {
  if(is.null(con) || !dbIsValid(con)) return(data.frame())
  
  tryCatch({
    # Récupérer tous les produits depuis product_info (créée par le script d'analyse)
    if(!dbExistsTable(con, "product_info")) {
      message("⚠️ Product_info table not found")
      return(data.frame())
    }
    
    raw_data <- dbReadTable(con, "product_info")
    
    if(nrow(raw_data) == 0) {
      message("⚠️ Product_info table empty")
      return(data.frame())
    }
    
    # Chercher les champs vides OU NULL directement dans product_info
    # Chercher les champs vides OU NULL directement dans product_info
    missing_products <- raw_data %>%
      filter(
        # Vérifier si les champs sont NULL, NA, vides ou contiennent seulement des espaces
        (is.null(code_prod) | is.na(code_prod) | code_prod == "" | str_trim(code_prod) == "") |
          (is.null(base) | is.na(base) | base == "" | str_trim(base) == "") |
          (is.null(ref) | is.na(ref) | ref == "" | str_trim(ref) == "") |
          (is.null(dosage) | is.na(dosage) | dosage == "" | str_trim(dosage) == "")
      ) %>%
      select(source_name, product_name, code_prod, base, ref, dosage) %>%
      distinct() %>%
      mutate(
        # Nettoyer les valeurs NULL/NA pour l'affichage
        code_prod = ifelse(is.na(code_prod) | is.null(code_prod), "", as.character(code_prod)),
        base = ifelse(is.na(base) | is.null(base), "", as.character(base)),
        ref = ifelse(is.na(ref) | is.null(ref), "", as.character(ref)),
        dosage = ifelse(is.na(dosage) | is.null(dosage), "", as.character(dosage))
      ) %>%
      # ✅ CALCULER LE STATUT DYNAMIQUEMENT
      mutate(
        statut = case_when(
          # Vérifier si TOUS les champs sont remplis
          str_trim(code_prod) != "" & 
            str_trim(base) != "" & 
            str_trim(ref) != "" & 
            str_trim(dosage) != "" ~ "Complete",
          
          # Sinon, à compléter
          TRUE ~ "To complete"
        )
      )
    
    
    message("🔍 Produits avec champs vides détectés: ", nrow(missing_products))
    
    # Debug : afficher quelques exemples
    if(nrow(missing_products) > 0) {
      message("📋 Exemples de produits à compléter:")
      sample_products <- head(missing_products, 3)
      for(i in 1:nrow(sample_products)) {
        message("  - ", sample_products$product_name[i], " (source: ", sample_products$source_name[i], ")")
      }
    }
    
    return(missing_products)
    
  }, error = function(e) {
    message("❌ Erreur détection Product Info manquants: ", e$message)
    return(data.frame())
  })
}


# ===== INTERFACE UTILISATEUR (INCHANGÉE) =====
ui <- dashboardPage(
  dashboardHeader(title =  "SA_METADATA Metadata Manager - Analysis Script"),
  
  dashboardSidebar(
    sidebarMenu(
      menuItem("Connexion SA_METADATA", tabName = "connection", icon = icon("database")),
      menuItem("Test Info Scanner", tabName = "scan_tests", icon = icon("search")),
      menuItem("Product Info Scanner", tabName = "scan_products", icon = icon("search")),
      menuItem("Test Info details", tabName = "manual_test", icon = icon("edit")),
      menuItem("Product Info details", tabName = "manual_product", icon = icon("edit")),
      menuItem("SA_METADATA tables", tabName = "postgres_tables", icon = icon("table")),
      menuItem("Result changes", tabName = "change_results", icon = icon("edit")),
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
      # ===== ONGLET CONNEXION SA_METADATA =====
      tabItem(
        tabName = "connection",
        fluidRow(
          box(
            title = "Connexion SA_METADATA", 
            status = "primary", 
            solidHeader = TRUE,
            width = 12,
            
            h4("Connection Status:"),
            div(id = "connection_status", class = "alert alert-warning",
                icon("exclamation-triangle"),
                " Connection not established"
            ),
            
            br(),
            
            fluidRow(
              column(6,
                     actionButton(
                       "connect_btn",
                       "Connect to SA_METADATA",
                       icon = icon("plug"),
                       class = "btn-success btn-lg"
                     )
              ),
              column(6,
                     actionButton(
                       "disconnect_btn",
                       "Disconnect",
                       icon = icon("unlink"),
                       class = "btn-danger btn-lg"
                     )
              )
            ),
            
            br(),
            
            h4( "Connection Information:" ),
            verbatimTextOutput("connection_info"),
            
            br(),
            
            h4("Available Tables in SA_METADATA:"),
            verbatimTextOutput("available_tables")
          )
        )
      ),
      
      # ===== ONGLET SCANNER TEST INFO =====
      tabItem(
        tabName = "scan_tests",
        fluidRow(
          box(
            title = "Missing Test Info Scanner", 
            status = "primary", 
            solidHeader = TRUE,
            width = 12,
            
            p("This tool identifies tests that exist in Product_Info (created by the analysis script) but don't have complete information in Test_Info."),
            
            div(class = "alert alert-info",
                icon("info-circle"),
                "Double-click on a test from the list to complete it directly!"
            ),
            
            actionButton(
              "scan_test_info_btn",
              "Missing Test Info Scanner",
              icon = icon("search"),
              class = "btn-primary btn-lg"
            ),
            
            br(), br(),
            
            conditionalPanel(
              condition = "output.test_scan_completed",
              div(class = "alert alert-warning",
                  icon("mouse-pointer"),
                  "Double-click on a test from the list to complete it directly!"
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
            title = "Missing Product Info Scanner", 
            status = "primary", 
            solidHeader = TRUE,
            width = 12,
            
            p("This tool identifies products that exist in Product_Info (created by the analysis script) but have empty fields (code_prod, base, ref, dosage)."),
            
            div(class = "alert alert-info",
                icon("info-circle"),
                "Double-click on a product from the list to complete it directly!"
            ),
            
            actionButton(
              "scan_product_info_btn",
              "Missing Product Info Scanner",
              icon = icon("search"),
              class = "btn-primary btn-lg"
            ),
            
            br(), br(),
            
            conditionalPanel(
              condition = "output.product_scan_completed",
              div(class = "alert alert-warning",
                  icon("mouse-pointer"),
                  "Double-click on a product from the list to complete it directly!"
              ),
              withSpinner(
                DT::dataTableOutput("missing_product_info_table"),
                type = 4
              )
            )
          )
        )
      ),
      
      # ===== ONGLET SAISIE TEST INFO AVEC SOURCE_NAME =====
      tabItem(
        tabName = "manual_test",
        fluidRow(
          box(
            title = "Test Info Entry", 
            status = "primary", 
            solidHeader = TRUE,
            width = 8,
            
            div(id = "test_indicator", class = "alert alert-info", style = "display: none;",
                icon("info-circle"),
                "Currently entering test: ",
                textOutput("current_test_name", inline = TRUE)
            ),
            
            # Messages de validation
            div(id = "test_validation_messages"),
            
            # Source Name (automatiquement rempli lors du double-clic)
            selectizeInput("source_name_input", "Source Name", 
                           choices = NULL,
                           options = list(
                             placeholder = "Select the Source Name of the test",
                             create = FALSE
                           )),
            
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
                     textInput("sc_request", "S&C Request # (Numerical)", 
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
            
            fluidRow(
              column(6,
                     selectInput("scale", "Scale", 
                                 choices = SCALE_CHOICES,
                                 selected = "")
              ),
              column(6,
                     div()  # Colonne vide pour l'alignement
              )
            ),
            
            br(),
            
            fluidRow(
              column(6,
                     actionButton(
                       "save_test_info_btn",
                       "Save Test Info",
                       icon = icon("save"),
                       class = "btn-success btn-lg"
                     )
              ),
              column(6,
                     actionButton(
                       "save_test_and_next_btn",
                       "Save and Next",
                       icon = icon("arrow-right"),
                       class = "btn-primary btn-lg"
                     )
              )
            ),
            
            br(),
            
            actionButton(
              "clear_test_form_btn",
              "Clear Form",
              icon = icon("eraser"),
              class = "btn-secondary"
            )
          ),
          
          box(
            title = "Test Info help", 
            status = "info", 
            solidHeader = TRUE,
            width = 4,
            
            h4("Test into fields:"),
            tags$ul(
              tags$li(strong("SOURCE NAME:"), " Unique identifier of the source file"),
              tags$li(strong("GMPS TYPE:"), " Dropdown list (INFO or SUB)"),
              tags$li(strong("GPMS CODE:"), " Free text"),
              tags$li(strong("S&C REQUEST #:"), " Numeric only"),
              tags$li(strong("TEST NAME:"), " Unique test name"),
              tags$li(strong("TEST DATE:"), " DD/MM/YYYY format required"),
              tags$li(strong("MASTER CUSTOMER NAME:"), " Dropdown list"),
              tags$li(strong("COUNTRY CLIENT:"), " Dropdown list"),
              tags$li(strong("TYPE OF TEST:"), " Dropdown list"),
              tags$li(strong("CATEGORY:"), " Dropdown list"),
              tags$li(strong("SUBSEGMENT:"), " Dropdown list"),
              tags$li(strong("METHODOLOGY:"), " Dropdown list"),
              tags$li(strong("PANEL:"), " Dropdown list"),
              tags$li(strong("TEST FACILITIES:"), " Dropdown list"),
              tags$li(strong("SCALE:"), " Dropdown list (1 to 5 or 0 to 10)")
            ),
            
            br(),
            
            h4("Remaining Tests:"),
            textOutput("remaining_tests_count"),
            
            br(),
            
            div(class = "alert alert-info",
                icon("info-circle"),
                strong("Automatic Validation:"),
                br(),
                "• Date: DD/MM/YYYY format",
                br(),
                "• S&C Request: numéric only",
                br(),
                "• Source Name: from Product_Info"
            )
          )
        )
      ),
      
      # ===== ONGLET SAISIE PRODUCT INFO AVEC SOURCE_NAME =====
      tabItem(
        tabName = "manual_product",
        fluidRow(
          box(
            title = "Product Info Entry", 
            status = "primary", 
            solidHeader = TRUE,
            width = 8,
            
            div(id = "product_indicator", class = "alert alert-info", style = "display: none;",
                icon("info-circle"),
                "Currently entering product: ",
                textOutput("current_product_name", inline = TRUE)
            ),
            
            # Messages de validation
            div(id = "product_validation_messages"),
            
            # Source Name (automatiquement rempli lors du double-clic)
            selectizeInput("product_source_name_input", "Source Name", 
                           choices = NULL,
                           options = list(
                             placeholder = "Select the Source Name of the product",
                             create = FALSE
                           )),
            
            textInput("code_prod_input", "Product Code", 
                      placeholder = "Ex: MGOB714A04, I, trial, Smell-It..."),
            
            # NOMPROD avec liste déroulante dynamique
            selectizeInput("nomprod_input", "Product Name", 
                           choices = NULL,
                           options = list(
                             placeholder = "Select or type the product name",
                             create = FALSE
                           )),
            
            # BASE avec liste déroulante + texte libre
            selectizeInput("base_input", "Base", 
                           choices = NULL,
                           options = list(
                             placeholder = "Select an existing base or type a new one",
                             create = TRUE
                           )),
            
            selectInput("ref_input", "Reference (Y/N)",
                        choices = REF_CHOICES,
                        selected = ""),
            
            textInput("dosage_input", "Dosage (Percentage)", 
                      placeholder = "Ex: 1,50% or 2.5%"),
            
            br(),
            
            # ===== NOUVEAU CADRE ADDITIONAL PRODUCTS =====
            box(
              title = "Additional Products (Optional)", 
              status = "info", 
              solidHeader = TRUE,
              width = 12,
              collapsible = TRUE,
              collapsed = TRUE,  # Fermé par défaut
              
              div(class = "alert alert-info",
                  icon("info-circle"),
                  "These fields are optional and can remain empty."
              ),
              
              fluidRow(
                column(6,
                       textInput("internal_name_additional_product", 
                                 "Internal Name Additional Product", 
                                 placeholder = "Internal Name Additional Product...")
                ),
                column(6,
                       textInput("perfumer_code_additional_product", 
                                 "Perfumer Code Additional Product", 
                                 placeholder = "Perfumer Code...")
                )
              ),
              
              fluidRow(
                column(6,
                       textInput("internal_code_additional_product", 
                                 "Internal Code Additional Product", 
                                 placeholder = "Internal Code Additional Product...")
                ),
                column(6,
                       textInput("external_name_additional_product", 
                                 "External Name Additional Product", 
                                 placeholder = "External Name Additional Product...")
                )
              ),
              
              fluidRow(
                column(6,
                       textInput("external_code_additional_product", 
                                 "External Code Additional Product", 
                                 placeholder = "External Code Additional Product...")
                ),
                column(6,
                       textInput("dosage_additional_product", 
                                 "Dosage Additional Product", 
                                 placeholder = "Ex: 2,5% or 1.8%...")
                )
              )
            ),
            
            br(),
            
            
            fluidRow(
              column(6,
                     actionButton(
                       "save_product_info_btn",
                       "Save Product Info",
                       icon = icon("save"),
                       class = "btn-success btn-lg"
                     )
              ),
              column(6,
                     actionButton(
                       "save_product_and_next_btn",
                       "Save and Next",
                       icon = icon("arrow-right"),
                       class = "btn-primary btn-lg"
                     )
              )
            ),
            
            br(),
            
            actionButton(
              "clear_product_form_btn",
              "Clear Form",
              icon = icon("eraser"),
              class = "btn-secondary"
            )
          ),
          
          box(
            title = "Product Info Help", 
            status = "info", 
            solidHeader = TRUE,
            width = 4,
            
            h4("Champs Product Info:"),
            tags$ul(
              tags$li(strong("SOURCE NAME:"), " Unique identifier of the source file"),
              tags$li(strong("CODE_PROD:"), " Free text (I, trial, Smell-It, official code...)"),
              tags$li(strong("NOMPROD:"), " Dropdown list based on Product_Info"),
              tags$li(strong("BASE:"), " List + free text (existing bases + new ones)"),
              tags$li(strong("REF:"), " Y (Yes) or N (No) only"),
              tags$li(strong("DOSAGE:"), " Percentage format (1.5% or 2.5%)"),
              tags$li(strong("INTERNAL NAME ADDITIONAL:"), " Internal additional product name (optional)"),
              tags$li(strong("PERFUMER CODE ADDITIONAL:"), " Additional perfumer code (optional)"),
              tags$li(strong("INTERNAL CODE ADDITIONAL:"), " Internal additional code (optional)"),
              tags$li(strong("EXTERNAL NAME ADDITIONAL:"), " External additional product name (optional)"),
              tags$li(strong("EXTERNAL CODE ADDITIONAL:"), " External additional code (optional)"),
              tags$li(strong("DOSAGE ADDITIONAL:"), " Additional product dosage - percentage format (optional)")
            ),
            
            br(),
            
            h4("Remaining Product:"),
            textOutput("remaining_products_count"),
            
            br(),
            
            div(class = "alert alert-info",
                icon("info-circle"),
                strong("Automatic Validation:"),
                br(),
                "• Dosage: percentage format",
                br(),
                "• Reference: Y or N only",
                br(),
                "• Nom Produit: from Product_Info"
            )
          )
        )
      ),
      
      # ===== ONGLET TABLES SA_METADATA =====
      tabItem(
        tabName = "postgres_tables",
        fluidRow(
          box(
            title = "SA_METADATA Tables", 
            status = "primary", 
            solidHeader = TRUE,
            width = 12,
            
            tabsetPanel(
              tabPanel("Test_Info",
                       br(),
                       actionButton("refresh_test_info_btn", "Refresh", 
                                    icon = icon("refresh"), class = "btn-info"),
                       br(), br(),
                       withSpinner(DT::dataTableOutput("test_info_table"), type = 4)
              ),
              tabPanel("Product_Info (Script)",
                       br(),
                       p("Table automatically created by the analysis script"),
                       actionButton("refresh_product_info_script_btn", "Refresh", 
                                    icon = icon("refresh"), class = "btn-info"),
                       br(), br(),
                       withSpinner(DT::dataTableOutput("product_info_script_table"), type = 4)
              )
            )
          )
        )
      ),
      
      # ===== ONGLET CHANGEMENT RÉSULTATS =====
      tabItem(
        tabName = "change_results",
        fluidRow(
          box(
            title = "SA_RESULTS Modification", 
            status = "primary", 
            solidHeader = TRUE,
            width = 12,
            
            # Sélection du type de test
            fluidRow(
              column(4,
                     selectInput("results_test_type", "Test Type",
                                 choices = c("Strength" = "strengthandmo_results",
                                             "Strength with Malodour" = "strengthandmo_results",
                                             "Proximity" = "proximity_results",
                                             "Triangular" = "triangulaire_results"),
                                 selected = "strengthandmo_results")
              ),
              column(4,
                     selectizeInput("results_source_filter", "Filtrer by Source",
                                    choices = NULL,
                                    options = list(
                                      placeholder = "All sources",
                                      create = FALSE
                                    ))
              ),
              column(4,
                     actionButton("load_results_btn", "Load Results",
                                  icon = icon("download"),
                                  class = "btn-primary",
                                  style = "margin-top: 25px;")
              )
            ),
            
            br(),
            
            # Message d'information
            div(id = "results_info_message", class = "alert alert-info",
                icon("info-circle"),
                "Select a test type and click 'Load' to display results. Double-click on a cell to edit it."
            ),
            
            # Table éditable des résultats
            withSpinner(
              DT::dataTableOutput("editable_results_table"),
              type = 4
            ),
            
            br(),
            
            # Boutons d'action
            fluidRow(
              column(6,
                     actionButton("save_changes_btn", "Save changes",
                                  icon = icon("save"),
                                  class = "btn-success btn-lg",
                                  disabled = TRUE)
              ),
              column(6,
                     actionButton("reset_changes_btn", "Cancel Changes",
                                  icon = icon("undo"),
                                  class = "btn-warning btn-lg",
                                  disabled = TRUE)
              )
            ),
            
            br(),
            
            # Log des modifications
            conditionalPanel(
              condition = "output.has_changes",
              box(
                title = "Change log", 
                status = "warning", 
                solidHeader = TRUE,
                width = 12,
                collapsible = TRUE,
                collapsed = TRUE,
                
                verbatimTextOutput("changes_log")
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
            title = "SA_METADATA Debug Information", 
            status = "warning", 
            solidHeader = TRUE,
            width = 12,
            
            h4("SA_METADATA Connection Status:"),
            verbatimTextOutput("debug_postgres_status"),
            
            br(),
            
            h4("Available SA_METADATA Tables:"),
            verbatimTextOutput("debug_postgres_tables"),
            
            br(),
            
            h4("Data Statistics:"),
            verbatimTextOutput("debug_data_stats"),
            
            br(),
            
            actionButton(
              "test_postgres_btn",
              "Test SA_METADATA Connection",
              icon = icon("database"),
              class = "btn-warning"
            )
          )
        )
      )
    )
  )
)

# ===== SERVEUR AVEC LOGIQUE RACCORDÉE =====
server <- function(input, output, session) {
  # ===== VARIABLES RÉACTIVES POUR CHANGEMENT RÉSULTATS =====
  results_data_original <- reactiveVal(NULL)
  results_data_current <- reactiveVal(NULL)
  results_table_name <- reactiveVal("")
  changes_made <- reactiveVal(FALSE)
  changes_log <- reactiveVal(character())
  
  
  # ===== OBSERVATEUR POUR METTRE À JOUR LES SOURCES =====
  observeEvent(input$results_test_type, {
    sources <- get_unique_sources_from_results(input$results_test_type)
    updateSelectizeInput(session, "results_source_filter", choices = sources)
  })
  
  # ===== CHARGEMENT DES RÉSULTATS =====
  observeEvent(input$load_results_btn, {
    # Connexion à SA_RESULTS_DATA au lieu de SA_METADATA
    con_results <- create_results_connection()
    if(is.null(con_results)) {
      showNotification("Erreur connexion SA_RESULTS", type = "error")
      return()
    }
    
    table_name <- input$results_test_type
    results_table_name(table_name)
    
    showNotification("Chargement des résultats...", type = "default")
    
    tryCatch({
      # Construire la requête SQL
      if(input$results_source_filter != "") {
        query <- paste0("SELECT * FROM ", table_name, 
                        " WHERE source_name = '", input$results_source_filter, "'",
                        " ORDER BY id")
      } else {
        query <- paste0("SELECT * FROM ", table_name, " ORDER BY id")
      }
      
      results <- dbGetQuery(con_results, query)
      
      if(nrow(results) > 0) {
        # Sauvegarder les données originales et courantes
        results_data_original(results)
        results_data_current(results)
        changes_made(FALSE)
        changes_log(character())
        
        # Activer/désactiver les boutons
        shinyjs::disable("save_changes_btn")
        shinyjs::disable("reset_changes_btn")
        showNotification(paste("Chargé", nrow(results), "résultats depuis SA_RESULTS"), type = "message")
        
      } else {
        showNotification("Aucun résultat trouvé", type = "warning")
        results_data_original(NULL)
        results_data_current(NULL)
      }
      
    }, error = function(e) {
      showNotification(paste("Erreur chargement:", e$message), type = "error")
    }, finally = {
      safe_disconnect(con_results)
    })
  })
  
  
  # ===== AFFICHAGE DE LA TABLE ÉDITABLE =====
  output$editable_results_table <- DT::renderDataTable({
    data <- results_data_current()
    
    if(is.null(data) || nrow(data) == 0) {
      return(DT::datatable(
        data.frame(Message = "No data loaded"),
        options = list(dom = 't')
      ))
    }
    
    # Déterminer les colonnes éditables selon le type de table
    table_name <- results_table_name()
    
    if(table_name == "triangulaire_results") {
      # Colonnes pour triangulaire: reference, candidate, n_total, n_correct, p_value, decision
      editable_cols <- c("reference", "candidate", "n_total", "n_correct", 
                         "p_value", "decision")
    } else if(table_name == "proximity_results") {
      # Colonnes pour proximity: product_name, classe, mean_value, sd_value, n_observations
      editable_cols <- c("product_name", "classe", "mean_value", "sd_value", 
                         "n_observations")
    } else {  # strengthandmo_results
      # Colonnes pour strength: product_name, classe, mean_value, sd_value, n_observations, anova_5pct, anova_10pct
      editable_cols <- c("product_name", "classe", "mean_value", "sd_value", 
                         "n_observations", "anova_5pct", "anova_10pct")
    }
    
    # Vérifier quelles colonnes éditables existent vraiment dans les données
    editable_cols <- intersect(editable_cols, names(data))
    
    # Trouver les indices des colonnes éditables
    col_indices <- which(names(data) %in% editable_cols) - 1  # -1 car DT est 0-based
    
    # Colonnes à cacher (id, created_at, updated_at)
    hidden_cols <- which(names(data) %in% c("id", "created_at", "updated_at")) - 1
    
    # Créer le datatable
    dt <- DT::datatable(
      data,
      editable = list(target = "cell", disable = list(columns = setdiff(0:(ncol(data)-1), col_indices))),
      options = list(
        pageLength = 15,
        scrollX = TRUE,
        scrollY = "400px",
        columnDefs = list(
          list(visible = FALSE, targets = hidden_cols)
        )
      ),
      rownames = FALSE
    )
    
    # Formater seulement les colonnes numériques qui existent
    format_cols <- intersect(c("mean_value", "sd_value", "p_value"), names(data))
    if(length(format_cols) > 0) {
      dt <- dt %>% DT::formatRound(columns = format_cols, digits = 3)
    }
    
    return(dt)
  })
  
  
  # ===== GESTION DES MODIFICATIONS DE CELLULES =====
  observeEvent(input$editable_results_table_cell_edit, {
    info <- input$editable_results_table_cell_edit
    
    if(is.null(info)) return()
    
    current_data <- results_data_current()
    original_data <- results_data_original()
    
    # Récupérer les informations de la modification
    row <- info$row
    col <- info$col + 1  # +1 car R est 1-based
    old_value <- original_data[row, col]
    new_value <- info$value
    col_name <- names(current_data)[col]
    
    # Validation selon le type de colonne
    if(col_name %in% c("n_total", "n_correct", "n_observations")) {
      # Colonnes entières
      new_value <- as.integer(new_value)
      if(is.na(new_value)) {
        showNotification("Valeur entière invalide", type = "error")
        return()
      }
    } else if(col_name %in% c("mean_value", "sd_value", "p_value")) {
      # Colonnes numériques
      new_value <- as.numeric(new_value)
      if(is.na(new_value)) {
        showNotification("Valeur numérique invalide", type = "error")
        return()
      }
    } else if(col_name %in% c("anova_5pct", "anova_10pct")) {
      # Colonnes booléennes
      new_value <- tolower(new_value) %in% c("true", "t", "1", "yes", "oui")
    }
    
    # Appliquer la modification
    current_data[row, col] <- new_value
    results_data_current(current_data)
    
    # Logger la modification
    log_entry <- paste(
      format(Sys.time(), "%H:%M:%S"),
      "- Ligne", row,
      "- Colonne:", col_name,
      "- Ancienne valeur:", old_value,
      "- Nouvelle valeur:", new_value
    )
    
    current_log <- changes_log()
    changes_log(c(current_log, log_entry))
    
    # Vérifier s'il y a des changements
    if(!identical(current_data, original_data)) {
      changes_made(TRUE)
      shinyjs::enable("save_changes_btn")
      shinyjs::enable("reset_changes_btn")
    }
  })
  
  # ===== SAUVEGARDE DES MODIFICATIONS =====
  observeEvent(input$save_changes_btn, {
    # ❌ PAS BESOIN DE CONNEXION ICI - on crée la connexion dans confirm_save
    current_data <- results_data_current()
    original_data <- results_data_original()
    table_name <- results_table_name()
    
    if(is.null(current_data) || !changes_made()) {
      showNotification("Aucune modification à sauvegarder", type = "warning")
      return()
    }
    
    
    current_data <- results_data_current()
    original_data <- results_data_original()
    table_name <- results_table_name()
    
    if(is.null(current_data) || !changes_made()) {
      showNotification("Aucune modification à sauvegarder", type = "warning")
      return()
    }
    
    showModal(modalDialog(
      title = "Confirm Changes",
      paste("Do you really want to save", sum(current_data != original_data, na.rm = TRUE), 
            "changes to the database?"),
      footer = tagList(
        modalButton("Cancel"),
        actionButton("confirm_save", "Confirm", class = "btn-success")
      )
    ))
  })
  
  # ===== CONFIRMATION DE SAUVEGARDE =====
  observeEvent(input$confirm_save, {
    removeModal()
    
    # Connexion à SA_RESULTS_DATA
    con_results <- create_results_connection()
    if(is.null(con_results)) {
      showNotification("Connexion SA_RESULTS_DATA requise", type = "error")
      return()
    }
    
    current_data <- results_data_current()
    table_name <- results_table_name()
    
    tryCatch({
      # Mettre à jour chaque ligne modifiée
      for(i in 1:nrow(current_data)) {
        row_data <- current_data[i, ]
        id <- row_data$id
        
        # Construire la requête UPDATE dynamiquement
        update_cols <- setdiff(names(row_data), c("id", "created_at", "updated_at"))
        
        set_clause <- paste(
          sapply(update_cols, function(col) {
            val <- row_data[[col]]
            if(is.na(val)) {
              paste0(col, " = NULL")
            } else if(is.logical(val)) {
              paste0(col, " = ", ifelse(val, "TRUE", "FALSE"))
            } else if(is.numeric(val)) {
              paste0(col, " = ", val)
            } else {
              paste0(col, " = '", gsub("'", "''", as.character(val)), "'")
            }
          }),
          collapse = ", "
        )
        
        update_query <- paste0(
          "UPDATE ", table_name,
          " SET ", set_clause, ", updated_at = CURRENT_TIMESTAMP",
          " WHERE id = ", id
        )
        
        dbExecute(con_results, update_query)
      }
      
      showNotification("Modifications sauvegardées avec succès dans SA_RESULTS!", type = "message")
      
      
      # Recharger les données
      results_data_original(current_data)
      changes_made(FALSE)
      changes_log(character())
      shinyjs::disable("save_changes_btn")
      shinyjs::disable("reset_changes_btn")
      
    }, error = function(e) {
      showNotification(paste("Erreur sauvegarde:", e$message), type = "error")
    }, finally = {
      safe_disconnect(con_results)
    })
  })
  
  
  # ===== ANNULATION DES MODIFICATIONS =====
  observeEvent(input$reset_changes_btn, {
    showModal(modalDialog(
      title = "Cancel Changes",
      "Do you really want to cancel all unsaved changes?",
      footer = tagList(
        modalButton("Non"),
        actionButton("confirm_reset", "Yes, cancel", class = "btn-warning")
      )
    ))
  })
  
  observeEvent(input$confirm_reset, {
    removeModal()
    
    # Restaurer les données originales
    results_data_current(results_data_original())
    changes_made(FALSE)
    changes_log(character())
    shinyjs::disable("save_changes_btn")
    shinyjs::disable("reset_changes_btn")
    
    showNotification("Modifications canceled", type = "default")
  })
  
  # ===== OUTPUT POUR LE LOG DES MODIFICATIONS =====
  output$changes_log <- renderText({
    log <- changes_log()
    if(length(log) == 0) {
      "No changes"
    } else {
      paste(log, collapse = "\n")
    }
  })
  
  # ===== OUTPUT POUR AFFICHER/CACHER LE LOG =====
  output$has_changes <- reactive({
    changes_made()
  })
  outputOptions(output, "has_changes", suspendWhenHidden = FALSE)
  
  # Variables réactives
  postgres_con <- reactiveVal(NULL)
  connection_status <- reactiveVal(FALSE)
  missing_test_info <- reactiveVal(data.frame())
  missing_product_info <- reactiveVal(data.frame())
  test_scan_completed <- reactiveVal(FALSE)
  product_scan_completed <- reactiveVal(FALSE)
  
  # ===== GESTION CONNEXION SA_METADATA =====
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
      updateSelectizeInput(session, "source_name_input", 
                           choices = get_unique_sources(con))
      updateSelectizeInput(session, "product_source_name_input", 
                           choices = get_unique_sources(con))
      
      # Mettre à jour l'interface
      shinyjs::html("connection_status", 
                    HTML('<i class="fa fa-check-circle status-connected"></i>Successful connection to SA_METADATA'))
      shinyjs::removeClass("connection_status", "alert-warning")
      shinyjs::addClass("connection_status", "alert-success")
      
      showNotification("SA_METADATA connection established", type = "default")
    } else {
      connection_status(FALSE)
      showNotification("SA_METADATA connection failed", type = "error")
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
      
      showNotification("SA_METADATA connection closed", type = "default")
    }
  })
  
  # ===== VALIDATION EN TEMPS RÉEL =====
  
  # Validation Test Date
  observeEvent(input$test_date, {
    if(!is.null(input$test_date) && input$test_date != "") {
      if(validate_date_format(input$test_date)) {
        shinyjs::removeClass("test_date", "validation-error")
        shinyjs::addClass("test_date", "validation-success")
        shinyjs::html("test_validation_messages", "")
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
        shinyjs::html("test_validation_messages", "")
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
        shinyjs::html("product_validation_messages", "")
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
        "Database: SA_METADATA",
        "User: dbadmin",
        "Status: Connected",
        sep = "\n"
      )
    } else {
      "Connection not established"
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
      "Require connection to database"
    }
  })
  
  # ===== SCANNER TEST INFO RACCORDÉ =====
  observeEvent(input$scan_test_info_btn, {
    con <- postgres_con()
    if(is.null(con)) {
      showNotification("SA_METADATA connection required", type = "error")
      return()
    }
    
    showNotification("Scan Test Info ongoing...", type = "default")
    test_scan_completed(FALSE)
    
    missing_tests <- detect_missing_test_info(con)
    missing_test_info(missing_tests)
    test_scan_completed(TRUE)
    
    if(nrow(missing_tests) > 0) {
      showNotification(
        paste("Trouvé", nrow(missing_tests), "tests without complete information"),
        type = "warning"
      )
    } else {
      showNotification("All the tests have complete informations", type = "default")
    }
  })
  
  # ===== SCANNER PRODUCT INFO RACCORDÉ =====
  observeEvent(input$scan_product_info_btn, {
    con <- postgres_con()
    if(is.null(con)) {
      showNotification("SA_METADATA connection required", type = "error")
      return()
    }
    
    showNotification("Scan Product Info ongoing...", type = "default")
    product_scan_completed(FALSE)
    
    missing_products <- detect_missing_product_info(con)
    missing_product_info(missing_products)
    product_scan_completed(TRUE)
    
    if(nrow(missing_products) > 0) {
      showNotification(
        paste("Trouvé", nrow(missing_products), "Product with empty columns"),
        type = "warning"
      )
    } else {
      showNotification("Well done, all products are completed", type = "default")
    }
  })
  
  # ===== TABLEAUX DES DONNÉES MANQUANTES RACCORDÉS =====
  output$missing_test_info_table <- DT::renderDataTable({
    req(test_scan_completed())
    
    missing_data <- missing_test_info()
    
    if(nrow(missing_data) == 0) {
      data.frame(
        Message = "No missing test detected",
        Details = "Well done, all tests are completed"
      )
    } else {
      missing_data %>%
        select(source_name, test_name, statut) %>%
        DT::datatable(
          options = list(
            pageLength = 10,
            scrollX = TRUE,
            selection = 'single',
            stateSave = TRUE,  # ✅ AJOUT : Sauvegarder l'état
            info = TRUE,       # ✅ AJOUT : Afficher les infos de pagination
            lengthChange = TRUE # ✅ AJOUT : Permettre de changer la taille des pages
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
        Message = "No product with empty columns detected",
        Details = "All products columns are completed"
      )
    } else {
      missing_data %>%
        select(source_name, product_name, code_prod, base, ref, dosage, statut) %>%
        DT::datatable(
          options = list(
            pageLength = 10,
            scrollX = TRUE,
            selection = 'single',
            stateSave = TRUE,  # ✅ AJOUT : Sauvegarder l'état
            info = TRUE,       # ✅ AJOUT : Afficher les infos de pagination
            lengthChange = TRUE # ✅ AJOUT : Permettre de changer la taille des pages
          ),
          rownames = FALSE
        )
    }
  })
  
  
  # ===== GESTION DU DOUBLE-CLIC POUR TEST INFO (VERSION FINALE CORRIGÉE) =====
  observeEvent(input$missing_test_info_table_cell_clicked, {
    click_info <- input$missing_test_info_table_cell_clicked
    
    if(is.null(click_info) || is.null(click_info$row) || is.null(click_info$col)) {
      message("❌ Click info invalide")
      return()
    }
    
    con <- postgres_con()
    if(is.null(con)) {
      showNotification("SA_METADATA connection required", type = "error")
      return()
    }
    
    # 🎯 UTILISER DIRECTEMENT L'INDEX DT (SANS +1)
    dt_row_index <- click_info$row  # DT est basé sur 0, R sur 1
    
    message("=== CLICK TEST FINAL CORRIGÉ ===")
    message("Index DT (basé sur 0): ", click_info$row)
    message("Index pour R (basé sur 1): ", dt_row_index)
    
    # Récupérer les données complètes
    missing_data <- missing_test_info()
    
    if(nrow(missing_data) == 0) {
      showNotification("No data available", type = "error")
      return()
    }
    
    # Reproduire exactement le filtrage de DT
    dt_displayed_data <- missing_data %>%
      select(source_name, test_name, statut)
    
    message("Nombre total de lignes dans DT: ", nrow(dt_displayed_data))
    
    # 🎯 VÉRIFICATION DES LIMITES
    if(dt_row_index < 1 || dt_row_index > nrow(dt_displayed_data)) {
      showNotification(paste("Index invalide:", dt_row_index, "pour", nrow(dt_displayed_data), "lignes"), type = "error")
      return()
    }
    
    # 🎯 RÉCUPÉRER LA LIGNE CORRECTE
    clicked_dt_row <- dt_displayed_data[dt_row_index, ]
    
    message("=== LIGNE SÉLECTIONNÉE ===")
    message("Source name sélectionné: '", clicked_dt_row$source_name, "'")
    message("Test name sélectionné: '", clicked_dt_row$test_name, "'")
    
    # Retrouver les données complètes correspondantes
    selected_test <- missing_data %>%
      filter(
        source_name == clicked_dt_row$source_name & 
          test_name == clicked_dt_row$test_name
      ) %>%
      slice(1)
    
    if(nrow(selected_test) == 0) {
      showNotification("Test not found", type = "error")
      return()
    }
    
    message("✅ Final Test selected'", selected_test$source_name, "'")
    
    tryCatch({
      # Charger dans le formulaire
      updateSelectizeInput(session, "source_name_input", selected = selected_test$source_name)
      updateTextInput(session, "test_name_input", value = selected_test$source_name)
      
      # Vider les autres champs
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
      updateSelectInput(session, "scale", selected = "")
      
      # Basculer vers l'onglet
      updateTabItems(session, "sidebarMenu", "manual_test")
      
      showNotification(
        paste(" Test loaded '", selected_test$source_name, "' (line", dt_row_index, ")"),
        type = "message"
      )
      
    }, error = function(e) {
      message("❌ Erreur chargement: ", e$message)
      showNotification(paste("Erreur chargement:", e$message), type = "error")
    })
  })
  
  
  # ===== GESTION DU DOUBLE-CLIC POUR PRODUCT INFO (VERSION FINALE CORRIGÉE) =====
  observeEvent(input$missing_product_info_table_cell_clicked, {
    click_info <- input$missing_product_info_table_cell_clicked
    
    if(is.null(click_info) || is.null(click_info$row) || is.null(click_info$col)) {
      message("❌ Click info invalide")
      return()
    }
    
    con <- postgres_con()
    if(is.null(con)) {
      showNotification("SA_METADATA connection required", type = "error")
      return()
    }
    
    # 🎯 UTILISER DIRECTEMENT L'INDEX DT (SANS +1)
    dt_row_index <- click_info$row   # DT est basé sur 0, R sur 1
    
    message("=== CLICK PRODUCT FINAL CORRIGÉ ===")
    message("Index DT (basé sur 0): ", click_info$row)
    message("Index pour R (basé sur 1): ", dt_row_index)
    
    # Récupérer les données complètes
    missing_data <- missing_product_info()
    
    if(nrow(missing_data) == 0) {
      showNotification("No data available", type = "error")
      return()
    }
    
    # Reproduire exactement le filtrage de DT
    dt_displayed_data <- missing_data %>%
      select(source_name, product_name, code_prod, base, ref, dosage, statut)
    
    message("Nombre total de lignes dans DT: ", nrow(dt_displayed_data))
    
    # 🎯 VÉRIFICATION DES LIMITES
    if(dt_row_index < 1 || dt_row_index > nrow(dt_displayed_data)) {
      showNotification(paste("Index invalide:", dt_row_index, "pour", nrow(dt_displayed_data), "lignes"), type = "error")
      return()
    }
    
    # 🎯 RÉCUPÉRER LA LIGNE CORRECTE
    clicked_dt_row <- dt_displayed_data[dt_row_index, ]
    
    message("=== LIGNE SÉLECTIONNÉE ===")
    message("Source name selected: '", clicked_dt_row$source_name, "'")
    message("Product name selected: '", clicked_dt_row$product_name, "'")
    
    # Retrouver les données complètes correspondantes
    selected_product <- missing_data %>%
      filter(
        source_name == clicked_dt_row$source_name & 
          product_name == clicked_dt_row$product_name
      ) %>%
      slice(1)
    
    if(nrow(selected_product) == 0) {
      showNotification("Product not found in complete data", type = "error")
      return()
    }
    
    message("Final product Selected'", selected_product$product_name, "' (source: '", selected_product$source_name, "')")
    
    tryCatch({
      # Charger dans le formulaire
      updateSelectizeInput(session, "product_source_name_input", selected = selected_product$source_name)
      updateSelectizeInput(session, "nomprod_input", selected = selected_product$product_name)
      updateTextInput(session, "code_prod_input", value = ifelse(is.na(selected_product$code_prod), "", selected_product$code_prod))
      updateSelectizeInput(session, "base_input", selected = ifelse(is.na(selected_product$base), "", selected_product$base))
      updateSelectInput(session, "ref_input", selected = ifelse(is.na(selected_product$ref), "", selected_product$ref))
      updateTextInput(session, "dosage_input", value = ifelse(is.na(selected_product$dosage), "", selected_product$dosage))
      if("internal_name_additional_product" %in% names(selected_product)) {
        updateTextInput(session, "internal_name_additional_product", 
                        value = ifelse(is.na(selected_product$internal_name_additional_product), "", selected_product$internal_name_additional_product))
      } else {
        updateTextInput(session, "internal_name_additional_product", value = "")
      }
      
      if("perfumer_code_additional_product" %in% names(selected_product)) {
        updateTextInput(session, "perfumer_code_additional_product", 
                        value = ifelse(is.na(selected_product$perfumer_code_additional_product), "", selected_product$perfumer_code_additional_product))
      } else {
        updateTextInput(session, "perfumer_code_additional_product", value = "")
      }
      
      if("internal_code_additional_product" %in% names(selected_product)) {
        updateTextInput(session, "internal_code_additional_product", 
                        value = ifelse(is.na(selected_product$internal_code_additional_product), "", selected_product$internal_code_additional_product))
      } else {
        updateTextInput(session, "internal_code_additional_product", value = "")
      }
      
      if("external_name_additional_product" %in% names(selected_product)) {
        updateTextInput(session, "external_name_additional_product", 
                        value = ifelse(is.na(selected_product$external_name_additional_product), "", selected_product$external_name_additional_product))
      } else {
        updateTextInput(session, "external_name_additional_product", value = "")
      }
      
      if("external_code_additional_product" %in% names(selected_product)) {
        updateTextInput(session, "external_code_additional_product", 
                        value = ifelse(is.na(selected_product$external_code_additional_product), "", selected_product$external_code_additional_product))
      } else {
        updateTextInput(session, "external_code_additional_product", value = "")
      }
      
      if("dosage_additional_product" %in% names(selected_product)) {
        updateTextInput(session, "dosage_additional_product", 
                        value = ifelse(is.na(selected_product$dosage_additional_product), "", selected_product$dosage_additional_product))
      } else {
        updateTextInput(session, "dosage_additional_product", value = "")
      }
      
      # Basculer vers l'onglet
      updateTabItems(session, "sidebarMenu", "manual_product")
      
      showNotification(
        paste("Product loaded: '", selected_product$product_name, "' (ligne", dt_row_index, ")"),
        type = "message"
      )
      
    }, error = function(e) {
      message("❌ Erreur chargement: ", e$message)
      showNotification(paste("Error while loading:", e$message), type = "error")
    })
  })  
  
  
  
  # ===== SAUVEGARDE TEST INFO RACCORDÉE (CORRIGÉE) =====
  observeEvent(input$save_test_info_btn, {
    con <- postgres_con()
    if(is.null(con)) {
      showNotification("SA_METADATA connection required", type = "error")
      return()
    }
    
    # Validation des champs obligatoires
    if(is.null(input$source_name_input) || input$source_name_input == "") {
      showNotification("Source Name is mandatory", type = "error")
      return()
    }
    
    if(is.null(input$test_name_input) || input$test_name_input == "") {
      showNotification("Test Name is mandatory", type = "error")
      return()
    }
    
    # Validation des formats
    if(!is.null(input$test_date) && input$test_date != "" && !validate_date_format(input$test_date)) {
      showNotification("Invalid Date Format. Use DD/MM/YYYY", type = "error")
      return()
    }
    
    if(!is.null(input$sc_request) && input$sc_request != "" && !validate_numeric_format(input$sc_request)) {
      showNotification("S&C Request must be numerical", type = "error")
      return()
    }
    
    # ✅ PRÉPARER LES DONNÉES (AVANT LA SAUVEGARDE)
    test_data <- data.frame(
      source_name = input$source_name_input,
      test_name = input$test_name_input,
      gmps_type = ifelse(is.null(input$gmps_type) || input$gmps_type == "", "", input$gmps_type),
      gpms_code = ifelse(is.null(input$gpms_code) || input$gpms_code == "", "", input$gpms_code),
      sc_request = ifelse(is.null(input$sc_request) || input$sc_request == "", "", input$sc_request),
      test_date = ifelse(is.null(input$test_date) || input$test_date == "", "", input$test_date),
      master_customer_name = ifelse(is.null(input$master_customer_name) || input$master_customer_name == "", "", input$master_customer_name),
      country_client = ifelse(is.null(input$country_client) || input$country_client == "", "", input$country_client),
      type_of_test = ifelse(is.null(input$type_of_test) || input$type_of_test == "", "", input$type_of_test),
      category = ifelse(is.null(input$category) || input$category == "", "", input$category),
      subsegment = ifelse(is.null(input$subsegment) || input$subsegment == "", "", input$subsegment),
      methodology = ifelse(is.null(input$methodology) || input$methodology == "", "", input$methodology),
      panel = ifelse(is.null(input$panel) || input$panel == "", "", input$panel),
      test_facilities = ifelse(is.null(input$test_facilities) || input$test_facilities == "", "", input$test_facilities),
      scale = ifelse(is.null(input$scale) || input$scale == "", "", input$scale),
      stringsAsFactors = FALSE
    )
    
    # ✅ SAUVEGARDER ET ACTUALISER
    if(save_test_info_to_postgres(con, test_data)) {
      showNotification("Test Info successfully saved !", type = "message")
      
      # Actualiser les données manquantes
      missing_tests <- detect_missing_test_info(con)
      missing_test_info(missing_tests)
      
      # ✅ ACTUALISER AUTOMATIQUEMENT LA TABLE TEST_INFO
      shinyjs::click("refresh_test_info_btn")
      
    } else {
      showNotification("Error while saving", type = "error")
    }
  })
  
  
  # ===== SAUVEGARDE PRODUCT INFO RACCORDÉE (CORRIGÉE) =====
  observeEvent(input$save_product_info_btn, {
    con <- postgres_con()
    if(is.null(con)) {
      showNotification("SA_METADATA connection required", type = "error")
      return()
    }
    
    # Validation des champs obligatoires
    if(is.null(input$product_source_name_input) || input$product_source_name_input == "") {
      showNotification("Source Name is mandatory", type = "error")
      return()
    }
    
    if(is.null(input$nomprod_input) || input$nomprod_input == "") {
      showNotification("Product Name is mandatory", type = "error")
      return()
    }
    
    # Validation des formats
    if(!is.null(input$dosage_input) && input$dosage_input != "" && !validate_percentage_format(input$dosage_input)) {
      showNotification("Invalid Dosage Format. Ex: 1,5% ou 2.5%", type = "error")
      return()
    }
    
    if(!is.null(input$dosage_additional_product) && input$dosage_additional_product != "" && !validate_percentage_format(input$dosage_additional_product)) {
      showNotification("Invalid additional dosage format. Ex: 1.5% or 2.5%", type = "error")
      return()
    }
    
    # ✅ PRÉPARER LES DONNÉES (AVANT LA SAUVEGARDE)
    product_data <- data.frame(
      source_name = input$product_source_name_input,
      product_name = input$nomprod_input,
      code_prod = ifelse(is.null(input$code_prod_input) || input$code_prod_input == "", "", input$code_prod_input),
      base = ifelse(is.null(input$base_input) || input$base_input == "", "", input$base_input),
      ref = ifelse(is.null(input$ref_input) || input$ref_input == "", "", input$ref_input),
      dosage = ifelse(is.null(input$dosage_input) || input$dosage_input == "", "", input$dosage_input),
      internal_name_additional_product = ifelse(is.null(input$internal_name_additional_product) || input$internal_name_additional_product == "", "", input$internal_name_additional_product),
      perfumer_code_additional_product = ifelse(is.null(input$perfumer_code_additional_product) || input$perfumer_code_additional_product == "", "", input$perfumer_code_additional_product),
      internal_code_additional_product = ifelse(is.null(input$internal_code_additional_product) || input$internal_code_additional_product == "", "", input$internal_code_additional_product),
      external_name_additional_product = ifelse(is.null(input$external_name_additional_product) || input$external_name_additional_product == "", "", input$external_name_additional_product),
      external_code_additional_product = ifelse(is.null(input$external_code_additional_product) || input$external_code_additional_product == "", "", input$external_code_additional_product),
      dosage_additional_product = ifelse(is.null(input$dosage_additional_product) || input$dosage_additional_product == "", "", input$dosage_additional_product),
      stringsAsFactors = FALSE
    )
    
    # ✅ SAUVEGARDER ET ACTUALISER
    if(save_product_info_to_postgres(con, product_data)) {
      showNotification("Product Info sauvegardé avec succès !", type = "message")
      
      # Actualiser les données manquantes
      missing_products <- detect_missing_product_info(con)
      missing_product_info(missing_products)
      
      # ✅ ACTUALISER AUTOMATIQUEMENT LA TABLE PRODUCT_INFO
      shinyjs::click("refresh_product_info_script_btn")
      
    } else {
      showNotification("Erreur lors de la sauvegarde", type = "error")
    }
  })
  
  # ===== SAUVEGARDE ET SUIVANT =====
  observeEvent(input$save_test_and_next_btn, {
    # Déclencher la sauvegarde
    shinyjs::click("save_test_info_btn")
    
    # Attendre un peu puis charger le test suivant
    Sys.sleep(0.5)
    
    missing_data <- missing_test_info()
    if(nrow(missing_data) > 0) {
      # Charger le premier test de la liste
      next_test <- missing_data[1, ]
      
      updateSelectizeInput(session, "source_name_input", selected = next_test$source_name)
      updateTextInput(session, "test_name_input", value = next_test$test_name)
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
      
      showNotification(paste("Test suivant chargé:", next_test$test_name), type = "message")
    } else {
      showNotification("Tous les tests sont complétés !", type = "message")
    }
  })
  
  observeEvent(input$save_product_and_next_btn, {
    # Déclencher la sauvegarde
    shinyjs::click("save_product_info_btn")
    
    # Attendre un peu puis charger le produit suivant
    Sys.sleep(0.5)
    
    missing_data <- missing_product_info()
    if(nrow(missing_data) > 0) {
      # Charger le premier produit de la liste
      next_product <- missing_data[1, ]
      
      updateSelectizeInput(session, "product_source_name_input", selected = next_product$source_name)
      updateSelectizeInput(session, "nomprod_input", selected = next_product$product_name)
      updateTextInput(session, "code_prod_input", value = ifelse(is.na(next_product$code_prod), "", next_product$code_prod))
      updateSelectizeInput(session, "base_input", selected = ifelse(is.na(next_product$base), "", next_product$base))
      updateSelectInput(session, "ref_input", selected = ifelse(is.na(next_product$ref), "", next_product$ref))
      updateTextInput(session, "dosage_input", value = ifelse(is.na(next_product$dosage), "", next_product$dosage))
      
      showNotification(paste("Next Product Loaded:", next_product$product_name), type = "message")
    } else {
      showNotification("Tous les produits sont complétés !", type = "message")
    }
  })
  
  # ===== VIDER LES FORMULAIRES =====
  observeEvent(input$clear_test_form_btn, {
    updateSelectizeInput(session, "source_name_input", selected = "")
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
    
    shinyjs::html("test_validation_messages", "")
    showNotification("Test Info form cleared", type = "message")
  })
  
  observeEvent(input$clear_product_form_btn, {
    updateSelectizeInput(session, "product_source_name_input", selected = "")
    updateSelectizeInput(session, "nomprod_input", selected = "")
    updateTextInput(session, "code_prod_input", value = "")
    updateSelectizeInput(session, "base_input", selected = "")
    updateSelectInput(session, "ref_input", selected = "")
    updateTextInput(session, "dosage_input", value = "")
    updateTextInput(session, "internal_name_additional_product", value = "")
    updateTextInput(session, "perfumer_code_additional_product", value = "")
    updateTextInput(session, "internal_code_additional_product", value = "")
    updateTextInput(session, "external_name_additional_product", value = "")
    updateTextInput(session, "external_code_additional_product", value = "")
    updateTextInput(session, "dosage_additional_product", value = "")
    
    shinyjs::html("product_validation_messages", "")
    showNotification("Product Info form cleared", type = "message")
  })
  
  # ===== COMPTEURS DE DONNÉES RESTANTES =====
  output$remaining_tests_count <- renderText({
    missing_data <- missing_test_info()
    if(nrow(missing_data) > 0) {
      paste("Remaining tests to complete:", nrow(missing_data))
    } else {
      "All tests are completed!"
    }
  })
  
  output$remaining_products_count <- renderText({
    missing_data <- missing_product_info()
    if(nrow(missing_data) > 0) {
      paste("Remaining products to complete:", nrow(missing_data))
    } else {
      "All products are completed!"
    }
  })
  
  # ===== INDICATEURS DE SCAN =====
  output$test_scan_completed <- reactive({
    test_scan_completed()
  })
  outputOptions(output, "test_scan_completed", suspendWhenHidden = FALSE)
  
  output$product_scan_completed <- reactive({
    product_scan_completed()
  })
  outputOptions(output, "product_scan_completed", suspendWhenHidden = FALSE)
  
  # ===== TABLEAUX DES TABLES SA_METADATA =====
  output$test_info_table <- DT::renderDataTable({
    con <- postgres_con()
    if(!is.null(con)) {
      test_info <- load_test_info_from_postgres(con)
      if(nrow(test_info) > 0) {
        # CORRECTION : Vérifier quelles colonnes existent avant de les exclure
        columns_to_exclude <- c("id", "created_at", "updated_at")
        existing_columns <- names(test_info)
        columns_to_exclude <- columns_to_exclude[columns_to_exclude %in% existing_columns]
        
        if(length(columns_to_exclude) > 0) {
          test_info <- test_info %>% select(-all_of(columns_to_exclude))
        }
        
        test_info %>%
          DT::datatable(
            options = list(
              pageLength = 15,
              scrollX = TRUE,
              scrollY = "400px"
            ),
            rownames = FALSE
          )
      } else {
        data.frame(Message = "No Test_info data available")
      }
    } else {
      data.frame(Message = "SA_METADATA connection required")
    }
  })
  
  output$product_info_script_table <- DT::renderDataTable({
    con <- postgres_con()
    if(!is.null(con)) {
      tryCatch({
        if(dbExistsTable(con, "product_info")) {  # ✅ MINUSCULES
          product_info <- dbReadTable(con, "product_info")  # ✅ MINUSCULES
          if(nrow(product_info) > 0) {
            product_info %>%
              DT::datatable(
                options = list(
                  pageLength = 15,
                  scrollX = TRUE,
                  scrollY = "400px"
                ),
                rownames = FALSE
              )
          } else {
            data.frame(Message = "Product_info table empty")
          }
        } else {
          data.frame(Message = "Product_info table not found")
        }
      }, error = function(e) {
        data.frame(Message = paste("Erreur:", e$message))
      })
    } else {
      data.frame(Message = "SA_METADATA connection required")
    }
  })
  
  # ===== ACTUALISATION DES TABLEAUX =====
  observeEvent(input$refresh_test_info_btn, {
    showNotification("Refreshing Test_Info...", type = "message")
    # ✅ FORCER LE RECHARGEMENT EN INVALIDANT LA SORTIE
    output$test_info_table <- DT::renderDataTable({
      con <- postgres_con()
      if(!is.null(con)) {
        test_info <- load_test_info_from_postgres(con)
        if(nrow(test_info) > 0) {
          columns_to_exclude <- c("id", "created_at", "updated_at")
          existing_columns <- names(test_info)
          columns_to_exclude <- columns_to_exclude[columns_to_exclude %in% existing_columns]
          
          if(length(columns_to_exclude) > 0) {
            test_info <- test_info %>% select(-all_of(columns_to_exclude))
          }
          
          test_info %>%
            DT::datatable(
              options = list(
                pageLength = 15,
                scrollX = TRUE,
                scrollY = "400px"
              ),
              rownames = FALSE
            )
        } else {
          data.frame(Message = "No Test_info data available")
        }
      } else {
        data.frame(Message = "SA_METADATA connection required")
      }
    })
  })
  
  observeEvent(input$refresh_product_info_script_btn, {
    showNotification("Refreshing Product_Info...", type = "message")
    # ✅ FORCER LE RECHARGEMENT EN INVALIDANT LA SORTIE
    output$product_info_script_table <- DT::renderDataTable({
      con <- postgres_con()
      if(!is.null(con)) {
        tryCatch({
          if(dbExistsTable(con, "product_info")) {
            product_info <- dbReadTable(con, "product_info")
            if(nrow(product_info) > 0) {
              product_info %>%
                DT::datatable(
                  options = list(
                    pageLength = 15,
                    scrollX = TRUE,
                    scrollY = "400px"
                  ),
                  rownames = FALSE
                )
            } else {
              data.frame(Message = "Product_info table empty")
            }
          } else {
            data.frame(Message = "Product_info table not found")
          }
        }, error = function(e) {
          data.frame(Message = paste("Erreur:", e$message))
        })
      } else {
        data.frame(Message = "SA_METADATA connection required")
      }
    })
  })
  

  
  # ===== DEBUG OUTPUTS =====
  output$debug_postgres_status <- renderText({
    con <- postgres_con()
    if(!is.null(con) && dbIsValid(con)) {
      paste(
        "✅ Connexion PostgreSQL active",
        "Host: emfrndsunx574.emea.sesam.mane.com",
        "Database: SA_METADATA",
        "Port: 5432",
        "User: dbadmin",
        sep = "\n"
      )
    } else {
      "PostGreSQL connection not established"
    }
  })
  
  output$debug_postgres_tables <- renderText({
    con <- postgres_con()
    if(!is.null(con) && dbIsValid(con)) {
      tryCatch({
        tables <- dbListTables(con)
        if(length(tables) > 0) {
          paste("Tables SA_METADATA:", paste(tables, collapse = ", "))
        } else {
          "Aucune table trouvée dans SA_METADATA"
        }
      }, error = function(e) {
        paste("Erreur listage tables:", e$message)
      })
    } else {
      "SA_METADATA connection required"
    }
  })
  
  output$debug_data_stats <- renderText({
    con <- postgres_con()
    if(!is.null(con) && dbIsValid(con)) {
      tryCatch({
        stats <- c()
        
        # ✅ CORRIGER : Statistiques test_info (minuscules)
        if(dbExistsTable(con, "test_info")) {
          test_count <- dbGetQuery(con, "SELECT COUNT(*) as count FROM test_info")$count
          stats <- c(stats, paste("test_info:", test_count, "lignes"))
        }
        
        # ✅ CORRIGER : Statistiques product_info (minuscules)
        if(dbExistsTable(con, "product_info")) {
          product_count <- dbGetQuery(con, "SELECT COUNT(*) as count FROM product_info")$count
          stats <- c(stats, paste("product_info:", product_count, "lignes"))
        }
        
        # Statistiques des données manquantes
        missing_tests <- nrow(missing_test_info())
        missing_products <- nrow(missing_product_info())
        
        stats <- c(stats, 
                   paste("Tests manquants:", missing_tests),
                   paste("Produits avec champs vides:", missing_products))
        
        paste(stats, collapse = "\n")
        
      }, error = function(e) {
        paste("Erreur calcul statistiques:", e$message)
      })
    } else {
      "SA_METADATA connection required"
    }
  })
  
  
  # ===== TEST CONNEXION =====
  observeEvent(input$test_postgres_btn, {
    showNotification("Testing SA_METADATA connection...", type = "message")
    
    con <- create_postgres_connection()
    if(!is.null(con)) {
      tryCatch({
        # Test simple
        result <- dbGetQuery(con, "SELECT version()")
        safe_disconnect(con)
        
        showNotification("✅ Test connexion SA_METADATA réussi !", type = "message")
      }, error = function(e) {
        showNotification(paste("❌ Erreur test connexion:", e$message), type = "error")
      })
    } else {
      showNotification("❌ Échec du test de connexion SA_METADATA", type = "error")
    }
  })
  
  # ===== NETTOYAGE À LA FERMETURE =====
  onStop(function() {
    con <- isolate(postgres_con())
    if(!is.null(con) && DBI::dbIsValid(con)) {
      safe_disconnect(con)
    }
  })
  
}

# ===== LANCEMENT DE L'APPLICATION =====
shinyApp(ui = ui, server = server)
