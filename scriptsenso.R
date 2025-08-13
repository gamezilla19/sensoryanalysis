# PROJET ANALYSE SENSO - VERSION AVEC TRACKING ET MULTI-DATABASES
# Auteur: Version optimisée avec système de tracking et intégration multi-bases PostgreSQL
# Date: 2025-08-12

# ===== CHARGEMENT DES BIBLIOTHÈQUES =====
suppressPackageStartupMessages({
  library(tidyverse)
  library(readxl)
  library(agricolae)
  library(DBI)
  library(RPostgreSQL)
  library(RPostgres)      # Ajout pour meilleure compatibilité
  library(odbc)
  library(fs)
  library(writexl)
  library(stringr)
  library(digest)
})

# ===== CONFIGURATION DES BASES DE DONNÉES =====
DB_CONFIG <- list(
  host = "emfrndsunx574.emea.sesam.mane.com",
  port = 5432,
  user = "dbadmin",
  password = "Azerty06*"
)

# Noms des bases de données
DATABASES <- list(
  RAW_DATA = "SA_RAW_DATA",
  RESULTS = "SA_RESULTS_DATA", 
  JUDGES = "SA_JUDGES",
  METADATA = "Metadata_SA"
)

# ===== FONCTIONS DE CONNEXION MULTI-DATABASES =====
create_db_connection <- function(database_name) {
  tryCatch({
    con <- dbConnect(RPostgres::Postgres(),
                     dbname = database_name,
                     host = DB_CONFIG$host,
                     port = DB_CONFIG$port,
                     user = DB_CONFIG$user,
                     password = DB_CONFIG$password)
    message("Connexion établie à la base : ", database_name)
    return(con)
  }, error = function(e) {
    message("ERREUR connexion à ", database_name, " : ", e$message)
    return(NULL)
  })
}

safe_disconnect <- function(con) {
  if(!is.null(con) && dbIsValid(con)) {
    dbDisconnect(con)
  }
}

# ===== FONCTIONS DE CRÉATION DES TABLES =====
create_raw_data_table <- function(con) {
  if(is.null(con)) return(FALSE)
  
  tryCatch({
    # Vérifier si la table existe déjà
    if(dbExistsTable(con, "rawdata")) {
      message("Table rawdata existe déjà")
      return(TRUE)
    }
    
    # Créer la table rawdata
    create_sql <- "
    CREATE TABLE IF NOT EXISTS rawdata (
      id SERIAL PRIMARY KEY,
      source_name VARCHAR(255) NOT NULL,
      trial_name VARCHAR(255),
      cj VARCHAR(100),
      product_name VARCHAR(255),
      attribute_name VARCHAR(255),
      nom_fonction VARCHAR(255),
      value NUMERIC,
      judge_status VARCHAR(50) DEFAULT 'conserved',
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE INDEX IF NOT EXISTS idx_rawdata_source ON rawdata(source_name);
    CREATE INDEX IF NOT EXISTS idx_rawdata_trial ON rawdata(trial_name);
    CREATE INDEX IF NOT EXISTS idx_rawdata_product ON rawdata(product_name);
    "
    
    dbExecute(con, create_sql)
    message("Table rawdata créée avec succès")
    return(TRUE)
    
  }, error = function(e) {
    message("Erreur création table rawdata : ", e$message)
    return(FALSE)
  })
}

create_results_table <- function(con) {
  if(is.null(con)) return(FALSE)
  
  tryCatch({
    # Table pour les résultats de tests (tous types)
    if(!dbExistsTable(con, "test_results")) {
      create_sql <- "
      CREATE TABLE IF NOT EXISTS test_results (
        id SERIAL PRIMARY KEY,
        source_name VARCHAR(255) NOT NULL,
        idtest VARCHAR(255),
        test_type VARCHAR(50), -- 'Strength', 'Proximity', 'Triangulaire', 'MO'
        segment VARCHAR(500),
        segment_id INTEGER,
        product_name VARCHAR(255),
        classe VARCHAR(10),
        mean_value NUMERIC,
        sd_value NUMERIC,
        n_observations INTEGER,
        p_value_produit NUMERIC,
        significatif_10pct BOOLEAN,
        ratio NUMERIC, -- Pour tests triangulaires
        n_total INTEGER, -- Pour tests triangulaires
        n_correct INTEGER, -- Pour tests triangulaires
        ic_inf NUMERIC, -- Intervalle de confiance
        ic_sup NUMERIC, -- Intervalle de confiance
        ref_product VARCHAR(255), -- Pour tests triangulaires
        candidat_product VARCHAR(255), -- Pour tests triangulaires
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
      
      CREATE INDEX IF NOT EXISTS idx_results_source ON test_results(source_name);
      CREATE INDEX IF NOT EXISTS idx_results_type ON test_results(test_type);
      CREATE INDEX IF NOT EXISTS idx_results_product ON test_results(product_name);
      "
      
      dbExecute(con, create_sql)
      message("Table test_results créée avec succès")
    }
    
    return(TRUE)
    
  }, error = function(e) {
    message("Erreur création table test_results : ", e$message)
    return(FALSE)
  })
}

create_judges_table <- function(con) {
  if(is.null(con)) return(FALSE)
  
  tryCatch({
    if(!dbExistsTable(con, "judge_tracking")) {
      create_sql <- "
      CREATE TABLE IF NOT EXISTS judge_tracking (
        id SERIAL PRIMARY KEY,
        source_name VARCHAR(255) NOT NULL,
        cj VARCHAR(100) NOT NULL,
        nb_evaluations INTEGER,
        moyenne_score NUMERIC,
        attributes_evalues INTEGER,
        produits_evalues INTEGER,
        nb_tests_total INTEGER,
        nb_tests_conserve INTEGER,
        taux_conservation NUMERIC,
        judge_status VARCHAR(50),
        date_analyse DATE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
      
      CREATE INDEX IF NOT EXISTS idx_judges_source ON judge_tracking(source_name);
      CREATE INDEX IF NOT EXISTS idx_judges_cj ON judge_tracking(cj);
      "
      
      dbExecute(con, create_sql)
      message("Table judge_tracking créée avec succès")
    }
    
    return(TRUE)
    
  }, error = function(e) {
    message("Erreur création table judge_tracking : ", e$message)
    return(FALSE)
  })
}

create_metadata_table <- function(con) {
  if(is.null(con)) return(FALSE)
  
  tryCatch({
    if(!dbExistsTable(con, "databrute")) {
      create_sql <- "
      CREATE TABLE IF NOT EXISTS databrute (
        id SERIAL PRIMARY KEY,
        idtest VARCHAR(255),
        productname VARCHAR(255),
        sourcefile VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
      
      CREATE INDEX IF NOT EXISTS idx_databrute_product ON databrute(productname);
      CREATE INDEX IF NOT EXISTS idx_databrute_source ON databrute(sourcefile);
      CREATE UNIQUE INDEX IF NOT EXISTS idx_databrute_unique ON databrute(sourcefile, productname);
      "
      
      dbExecute(con, create_sql)
      message("Table databrute créée avec succès dans Metadata_SA")
    }
    
    return(TRUE)
    
  }, error = function(e) {
    message("Erreur création table databrute : ", e$message)
    return(FALSE)
  })
}

# ===== FONCTIONS DE SAUVEGARDE DANS LES BASES =====
save_raw_data_to_db <- function(raw_data, source_name) {
  con <- create_db_connection(DATABASES$RAW_DATA)
  if(is.null(con)) return(FALSE)
  
  tryCatch({
    # Créer la table si nécessaire
    create_raw_data_table(con)
    
    # Préparer les données avec SOURCE_NAME
    raw_data_db <- raw_data %>%
      mutate(
        source_name = source_name,
        trial_name = TrialName,
        cj = CJ,
        product_name = ProductName,
        attribute_name = AttributeName,
        nom_fonction = NomFonction,
        value = as.numeric(Value),
        judge_status = ifelse(is.na(JudgeStatus), "conserved", JudgeStatus)
      ) %>%
      select(source_name, trial_name, cj, product_name, attribute_name, 
             nom_fonction, value, judge_status)
    
    # Supprimer les données existantes pour ce fichier source
    dbExecute(con, "DELETE FROM rawdata WHERE source_name = $1", params = list(source_name))
    
    # Insérer les nouvelles données
    dbWriteTable(con, "rawdata", raw_data_db, append = TRUE, row.names = FALSE)
    
    message("Données brutes sauvegardées pour : ", source_name, " (", nrow(raw_data_db), " lignes)")
    safe_disconnect(con)
    return(TRUE)
    
  }, error = function(e) {
    message("Erreur sauvegarde données brutes : ", e$message)
    safe_disconnect(con)
    return(FALSE)
  })
}

save_results_to_db <- function(results_data, source_name, test_type) {
  con <- create_db_connection(DATABASES$RESULTS)
  if(is.null(con)) return(FALSE)
  
  tryCatch({
    create_results_table(con)
    
    # Adapter les données selon le type de test
    if(test_type == "Triangulaire") {
      results_db <- results_data %>%
        mutate(
          source_name = source_name,
          test_type = test_type,
          idtest = IDTEST,
          segment = ifelse("Segment" %in% names(.), Segment, NA),
          segment_id = ifelse("ID segment" %in% names(.), `ID segment`, NA),
          ratio = ifelse("Ratio" %in% names(.), Ratio, NA),
          n_total = ifelse("N_tot" %in% names(.), N_tot, NA),
          n_correct = ifelse("N_correct" %in% names(.), N_correct, NA),
          p_value_produit = ifelse("P-value" %in% names(.), `P-value`, NA),
          ref_product = ifelse("Ref" %in% names(.), Ref, NA),
          candidat_product = ifelse("Candidat" %in% names(.), Candidat, NA)
        ) %>%
        select(source_name, idtest, test_type, segment, segment_id, 
               ratio, n_total, n_correct, p_value_produit, ref_product, candidat_product)
      
    } else {
      # Tests standard (Strength, Proximity, MO)
      results_db <- results_data %>%
        mutate(
          source_name = source_name,
          test_type = test_type,
          idtest = IDTest,
          segment = Segment,
          segment_id = `ID segment`,
          product_name = Product,
          classe = Classe,
          mean_value = Mean,
          sd_value = Sd,
          n_observations = n,
          p_value_produit = P_value_produit,
          significatif_10pct = Significatif_10pct
        ) %>%
        select(source_name, idtest, test_type, segment, segment_id, product_name,
               classe, mean_value, sd_value, n_observations, p_value_produit, significatif_10pct)
    }
    
    # Supprimer les résultats existants pour ce fichier
    dbExecute(con, "DELETE FROM test_results WHERE source_name = $1", params = list(source_name))
    
    # Insérer les nouveaux résultats
    dbWriteTable(con, "test_results", results_db, append = TRUE, row.names = FALSE)
    
    message("Résultats sauvegardés (", test_type, ") pour : ", source_name, " (", nrow(results_db), " lignes)")
    safe_disconnect(con)
    return(TRUE)
    
  }, error = function(e) {
    message("Erreur sauvegarde résultats : ", e$message)
    safe_disconnect(con)
    return(FALSE)
  })
}

save_judges_to_db <- function(judge_data, source_name) {
  con <- create_db_connection(DATABASES$JUDGES)
  if(is.null(con)) return(FALSE)
  
  tryCatch({
    create_judges_table(con)
    
    # Préparer les données des juges
    judges_db <- judge_data %>%
      mutate(
        source_name = source_name,
        cj = CJ,
        nb_evaluations = ifelse("NbEvaluations" %in% names(.), NbEvaluations, NA),
        moyenne_score = ifelse("MoyenneScore" %in% names(.), MoyenneScore, NA),
        attributes_evalues = ifelse("AttributesEvalues" %in% names(.), AttributesEvalues, NA),
        produits_evalues = ifelse("ProduitsEvalues" %in% names(.), ProduitsEvalues, NA),
        nb_tests_total = ifelse("NbTestsTotal" %in% names(.), NbTestsTotal, NA),
        nb_tests_conserve = ifelse("NbTestsConserve" %in% names(.), NbTestsConserve, NA),
        taux_conservation = ifelse("TauxConservation" %in% names(.), TauxConservation, NA),
        judge_status = ifelse("JudgeStatus" %in% names(.), JudgeStatus, "conserved"),
        date_analyse = Sys.Date()
      ) %>%
      select(source_name, cj, nb_evaluations, moyenne_score, attributes_evalues,
             produits_evalues, nb_tests_total, nb_tests_conserve, taux_conservation,
             judge_status, date_analyse)
    
    # Supprimer les données existantes pour ce fichier
    dbExecute(con, "DELETE FROM judge_tracking WHERE source_name = $1", params = list(source_name))
    
    # Insérer les nouvelles données
    dbWriteTable(con, "judge_tracking", judges_db, append = TRUE, row.names = FALSE)
    
    message("Tracking juges sauvegardé pour : ", source_name, " (", nrow(judges_db), " lignes)")
    safe_disconnect(con)
    return(TRUE)
    
  }, error = function(e) {
    message("Erreur sauvegarde tracking juges : ", e$message)
    safe_disconnect(con)
    return(FALSE)
  })
}

save_metadata_couples <- function(raw_data, source_name) {
  con <- create_db_connection(DATABASES$METADATA)
  if(is.null(con)) return(FALSE)
  
  tryCatch({
    create_metadata_table(con)
    
    # Extraire les couples uniques (SOURCE_NAME, ProductName)
    couples <- raw_data %>%
      select(TrialName, ProductName) %>%
      distinct() %>%
      mutate(
        idtest = TrialName,
        productname = ProductName,
        sourcefile = source_name
      ) %>%
      select(idtest, productname, sourcefile)
    
    # Insérer les couples (avec gestion des doublons via ON CONFLICT)
    for(i in 1:nrow(couples)) {
      couple <- couples[i, ]
      
      # Vérifier si le couple existe déjà
      existing <- dbGetQuery(con, 
                             "SELECT COUNT(*) as count FROM databrute WHERE sourcefile = $1 AND productname = $2",
                             params = list(couple$sourcefile, couple$productname))
      
      if(existing$count == 0) {
        # Insérer le nouveau couple
        dbExecute(con,
                  "INSERT INTO databrute (idtest, productname, sourcefile) VALUES ($1, $2, $3)",
                  params = list(couple$idtest, couple$productname, couple$sourcefile))
      }
    }
    
    message("Couples métadonnées sauvegardés pour : ", source_name, " (", nrow(couples), " couples)")
    safe_disconnect(con)
    return(TRUE)
    
  }, error = function(e) {
    message("Erreur sauvegarde couples métadonnées : ", e$message)
    safe_disconnect(con)
    return(FALSE)
  })
}

# ===== FONCTION DE DÉTERMINATION DU TYPE DE TEST =====
determine_test_type <- function(segments) {
  # Vérifier s'il y a des tests triangulaires
  triangular_segments <- segments[sapply(segments, function(seg) {
    !is.na(seg$NomFonction[1]) && str_detect(seg$NomFonction[1], "Triangulaire|triangle")
  })]
  
  if(length(triangular_segments) > 0) {
    return("Triangulaire")
  }
  
  # Vérifier s'il y a des tests de proximité
  proximity_segments <- segments[sapply(segments, function(seg) {
    !is.na(seg$AttributeName[1]) && str_detect(str_to_lower(seg$AttributeName[1]), "prox")
  })]
  
  if(length(proximity_segments) > 0) {
    return("Proximity")
  }
  
  # Vérifier s'il y a des tests MO (odeur corporelle)
  mo_segments <- segments[sapply(segments, function(seg) {
    !is.na(seg$AttributeName[1]) && str_detect(str_to_lower(seg$AttributeName[1]), "odeur corporell")
  })]
  
  if(length(mo_segments) > 0) {
    return("MO")
  }
  
  # Par défaut, test de force (Strength)
  return("Strength")
}

# ===== INITIALISATION =====
message("Début analyse avec intégration multi-databases: ", Sys.time())

raw_data_dir <- "C:/testManon"
output_base_dir <- "C:/ResultatsAnalyseSenso"

if(!dir.exists(output_base_dir)) {
  dir.create(output_base_dir, recursive = TRUE, showWarnings = FALSE)
  message("Création du dossier de sortie principal: ", output_base_dir)
}

# ===== SYSTÈME DE TRACKING (INCHANGÉ) =====
tracking_file <- file.path(output_base_dir, "TRACKING_FICHIERS.xlsx")

load_tracking_data <- function() {
  if(file.exists(tracking_file)) {
    tryCatch({
      tracking_data <- read_excel(tracking_file)
      message("Fichier de tracking chargé: ", nrow(tracking_data), " entrées")
      return(tracking_data)
    }, error = function(e) {
      message("Erreur lecture tracking, création nouveau fichier: ", e$message)
      return(tibble(
        Fichier = character(0),
        Chemin_Complet = character(0),
        Hash_MD5 = character(0),
        Date_Traitement = as.POSIXct(character(0)),
        Statut = character(0),
        Taille_Fichier = numeric(0),
        Nb_Lignes_Results = numeric(0)
      ))
    })
  } else {
    message("Création nouveau fichier de tracking")
    return(tibble(
      Fichier = character(0),
      Chemin_Complet = character(0),
      Hash_MD5 = character(0),
      Date_Traitement = as.POSIXct(character(0)),
      Statut = character(0),
      Taille_Fichier = numeric(0),
      Nb_Lignes_Results = numeric(0)
    ))
  }
}

calculate_file_hash <- function(file_path) {
  tryCatch({
    digest(file_path, algo = "md5", file = TRUE)
  }, error = function(e) {
    message("Erreur calcul hash pour ", basename(file_path), ": ", e$message)
    return(NA_character_)
  })
}

is_file_already_processed <- function(file_path, tracking_data) {
  if(nrow(tracking_data) == 0) return(FALSE)
  
  current_hash <- calculate_file_hash(file_path)
  if(is.na(current_hash)) return(FALSE)
  
  existing_entry <- tracking_data %>%
    filter(
      Fichier == basename(file_path) &
        Hash_MD5 == current_hash &
        Statut == "SUCCES"
    )
  
  return(nrow(existing_entry) > 0)
}

update_tracking <- function(file_path, statut, nb_lignes = NA, tracking_data) {
  current_hash <- calculate_file_hash(file_path)
  file_size <- file.info(file_path)$size
  
  new_entry <- tibble(
    Fichier = basename(file_path),
    Chemin_Complet = as.character(file_path),
    Hash_MD5 = current_hash,
    Date_Traitement = Sys.time(),
    Statut = statut,
    Taille_Fichier = file_size,
    Nb_Lignes_Results = nb_lignes
  )
  
  tracking_data_updated <- tracking_data %>%
    filter(!(
      Fichier == basename(file_path) & 
        Hash_MD5 == current_hash
    ))
  
  tracking_data_updated <- bind_rows(tracking_data_updated, new_entry)
  return(tracking_data_updated)
}

save_tracking_data <- function(tracking_data) {
  tryCatch({
    write_xlsx(tracking_data, tracking_file)
    message("Fichier de tracking sauvegardé: ", tracking_file)
  }, error = function(e) {
    message("Erreur sauvegarde tracking: ", e$message)
  })
}

# ===== CHARGEMENT DES FONCTIONS D'ANALYSE (INCHANGÉES) =====
# [Toutes les fonctions d'analyse restent identiques : 
#  log_probleme, validate_data_consistency, create_judge_tracking_table,
#  handle_proximity_test, handle_triangular_test_complete, process_triangular_segments,
#  handle_triangular_test, analyze_judges_iterative, analyze_products, verify_segments]

# Pour la brièveté, je ne recopie pas toutes les fonctions d'analyse qui restent identiques
# Elles doivent être incluses exactement comme dans votre script original

# ===== PROGRAMME PRINCIPAL MODIFIÉ =====
tracking_data <- load_tracking_data()
excel_files <- dir_ls(raw_data_dir, regexp = "\\.xlsx$", ignore.case = TRUE, recurse = TRUE) %>%
  as.character()

message("Fichiers Excel détectés: ", length(excel_files))

all_results <- list()
judge_removal_info <- list()
all_raw_data <- list()
data_issues_log <- list()

files_processed <- 0
files_skipped <- 0
files_new <- 0

# ===== BOUCLE PRINCIPALE MODIFIÉE =====
for (file_path in excel_files) {
  file_basename <- basename(file_path)
  source_name <- tools::file_path_sans_ext(file_basename)  # Nom sans extension
  
  if(is_file_already_processed(file_path, tracking_data)) {
    message("\n=== FICHIER DÉJÀ TRAITÉ (SKIP): ", file_basename, " ===")
    files_skipped <- files_skipped + 1
    next
  }
  
  message("\n=== TRAITEMENT NOUVEAU FICHIER: ", file_basename, " ===")
  files_new <- files_new + 1
  
  # Lecture et validation (code inchangé)
  sheet_names <- tryCatch({
    excel_sheets(file_path)
  }, error = function(e) {
    log_probleme("ERREUR_LECTURE", paste("Impossible de lire les onglets:", e$message), file_path)
    tracking_data <<- update_tracking(file_path, "ERREUR_LECTURE", NA, tracking_data)
    return(NULL)
  })
  
  if(is.null(sheet_names)) next
  
  if(!"Results" %in% sheet_names) {
    log_probleme("ONGLET_MANQUANT", "Onglet 'Results' manquant", file_path)
    tracking_data <- update_tracking(file_path, "ONGLET_MANQUANT", NA, tracking_data)
    next
  }
  
  # Lecture des données (code inchangé)
  file_data <- tryCatch({
    read_excel(file_path, sheet = "Results") %>%
      mutate(SourceFile = basename(file_path))
  }, error = function(e) {
    log_probleme("ERREUR_LECTURE_RESULTS", paste("Erreur lecture Results:", e$message), file_path)
    tracking_data <<- update_tracking(file_path, "ERREUR_LECTURE_RESULTS", NA, tracking_data)
    return(NULL)
  })
  
  if(is.null(file_data)) next
  
  # Validation et traitement (code inchangé jusqu'à la génération des résultats)
  # [Tout le code de traitement reste identique]
  
  # ===== NOUVEAU : SAUVEGARDE DANS LES BASES DE DONNÉES =====
  
  # 1. Sauvegarder les données brutes avec SOURCE_NAME
  if(save_raw_data_to_db(file_data, source_name)) {
    message(" Données brutes sauvegardées dans SA_RAW_DATA")
  }
  
  # 2. Déterminer le type de test et sauvegarder les résultats
  test_type <- determine_test_type(segments)
  if(exists("file_results") && nrow(file_results) > 0) {
    if(save_results_to_db(file_results, source_name, test_type)) {
      message(" Résultats sauvegardés dans SA_RESULTS_DATA (", test_type, ")")
    }
  }
  
  # 3. Sauvegarder le tracking des juges
  if(exists("judge_tracking_table") && nrow(judge_tracking_table) > 0) {
    if(save_judges_to_db(judge_tracking_table, source_name)) {
      message(" Tracking juges sauvegardé dans SA_JUDGES")
    }
  }
  
  # 4. Sauvegarder les couples métadonnées
  if(save_metadata_couples(file_data, source_name)) {
    message(" Couples métadonnées sauvegardés dans Metadata_SA")
  }
  
  # Mise à jour du tracking
  tracking_data <- update_tracking(file_path, "SUCCES", nrow(file_data), tracking_data)
  files_processed <- files_processed + 1
  
  # Génération des fichiers Excel (code inchangé)
  # [Code de génération des fichiers individuels reste identique]
}

# ===== SAUVEGARDE FINALE =====
save_tracking_data(tracking_data)

# ===== RÉSUMÉ FINAL =====
message("\n=== RÉSUMÉ DE L'ANALYSE AVEC INTÉGRATION MULTI-DATABASES ===")
message("Fichiers détectés : ", length(excel_files))
message("Fichiers déjà traités (skippés) : ", files_skipped)
message("Nouveaux fichiers détectés : ", files_new)
message("Nouveaux fichiers traités avec succès : ", files_processed)

message("\n=== BASES DE DONNÉES MISES À JOUR ===")
message(" SA_RAW_DATA : Données brutes avec SOURCE_NAME et JudgeStatus")
message(" SA_RESULTS_DATA : Résultats par type de test (Strength/Proximity/Triangulaire/MO)")
message(" SA_JUDGES : Tracking détaillé des juges")
message(" Metadata_SA : Couples (SOURCE_NAME, ProductName) pour l'application")

message("\nAnalyse terminée: ", Sys.time())
# ===== FONCTIONS UTILITAIRES ET D'ANALYSE (SUITE) =====

# Fonction de logging centralisé pour les problèmes de données
log_probleme <- function(type, details, fichier) {
  msg <- paste0(
    "[", type, "] ",
    details,
    " | Fichier: ", basename(fichier)
  )
  
  data_issues_log <<- append(data_issues_log, msg)
  message("PROBLEME: ", msg)
}

# Fonction de validation de la cohérence des données
validate_data_consistency <- function(file_data) {
  issues <- character()
  
  invalid_values <- sum(
    is.na(as.numeric(file_data$Value)) &
      !is.na(file_data$Value)
  )
  
  if(invalid_values > 0) {
    issues <- c(issues, paste("Valeurs non numériques détectées:", invalid_values))
  }
  
  return(issues)
}

# ===== FONCTION CRÉATION TABLE TRACKING JUGES =====
create_judge_tracking_table <- function(all_judge_info, all_raw_data) {
  tryCatch({
    if(nrow(all_raw_data) == 0) {
      message("Aucune donnée brute disponible pour le tracking des juges")
      return(tibble(
        Message = "Aucune donnée disponible",
        Timestamp = Sys.time()
      ))
    }
    
    judge_participation <- all_raw_data %>%
      group_by(SourceFile, CJ) %>%
      summarise(
        NbEvaluations = n(),
        MoyenneScore = mean(Value, na.rm = TRUE),
        AttributesEvalues = n_distinct(AttributeName, na.rm = TRUE),
        ProduitsEvalues = n_distinct(ProductName, na.rm = TRUE),
        .groups = 'drop'
      ) %>%
      mutate(
        DateAnalyse = Sys.Date()
      )
    
    if(nrow(all_judge_info) > 0 && "RemovedJudges" %in% names(all_judge_info)) {
      judge_removal_summary <- all_judge_info %>%
        separate_rows(RemovedJudges, sep = ", ") %>%
        filter(RemovedJudges != "" & !is.na(RemovedJudges)) %>%
        select(File, Segment, RemovedJudges) %>%
        rename(SourceFile = File, CJ = RemovedJudges) %>%
        mutate(JudgeStatus = "removed")
      
      judge_tracking <- judge_participation %>%
        left_join(judge_removal_summary, by = c("SourceFile", "CJ")) %>%
        mutate(
          JudgeStatus = coalesce(JudgeStatus, "conserved")
        )
    } else {
      judge_tracking <- judge_participation %>%
        mutate(JudgeStatus = "conserved")
    }
    
    judge_tracking <- judge_tracking %>%
      group_by(CJ) %>%
      mutate(
        NbTestsTotal = n(),
        NbTestsConserve = sum(JudgeStatus == "conserved", na.rm = TRUE),
        TauxConservation = round(NbTestsConserve / NbTestsTotal, 3)
      ) %>%
      ungroup()
    
    return(judge_tracking)
    
  }, error = function(e) {
    message("Erreur création table tracking juges: ", e$message)
    return(tibble(
      Erreur = paste("Échec création table tracking:", e$message),
      Details = "Vérifiez les colonnes dans vos données",
      Timestamp = Sys.time()
    ))
  })
}

# ===== FONCTION GESTION DES TESTS DE PROXIMITÉ =====
handle_proximity_test <- function(segment) {
  bench_product <- segment %>%
    group_by(ProductName) %>%
    summarise(avg = mean(Value, na.rm = TRUE), .groups = 'drop') %>%
    slice_min(avg, n = 1) %>%
    pull(ProductName)
  
  filtered_judges <- segment %>%
    group_by(CJ) %>%
    mutate(bench_score = Value[ProductName %in% bench_product]) %>%
    filter(
      max(Value[ProductName %in% bench_product], na.rm = TRUE) > 4 |
        any(Value[!ProductName %in% bench_product] <= (bench_score - 1))
    ) %>%
    distinct(CJ) %>%
    pull(CJ)
  
  list(
    segment = segment %>% filter(!CJ %in% filtered_judges),
    removed_judges = filtered_judges,
    n_initial = n_distinct(segment$CJ),
    n_final = n_distinct(segment$CJ) - length(filtered_judges)
  )
}

# ===== FONCTION GESTION DES TESTS TRIANGULAIRES COMPLETS AVEC P-VALUE =====
handle_triangular_test_complete <- function(segment, file_path) {
  tryCatch({
    message("Traitement test triangulaire complet: ", unique(segment$NomFonction))
    
    combined_segment <- segment
    ref_product <- NA_character_
    candidat_product <- NA_character_
    
    available_products <- unique(combined_segment$ProductName)
    message("Produits disponibles dans le segment: ", paste(available_products, collapse = ", "))
    
    if(length(available_products) > 0) {
      ref_product <- available_products[1]
      message("Référence par défaut: ", ref_product)
    }
    
    if(length(available_products) > 1) {
      candidat_product <- available_products[2]
      message("Candidat par défaut: ", candidat_product)
    }
    
    # ===== CALCUL DU TEST TRIANGULAIRE =====
    n_total <- sum(!is.na(combined_segment$Value))  # n = nb de participants
    n_correct <- sum(combined_segment$Value == 1, na.rm = TRUE)  # x = nb de bonnes réponses
    ratio <- round(n_correct / n_total, 3)
    
    # Test binomial avec p = 1/3 (constante pour test triangulaire)
    # alternative = "greater" pour tester si le taux de réussite > hasard (1/3)
    test_result <- binom.test(n_correct, n_total, p = 1/3, alternative = "greater")
    p_value <- test_result$p.value
    
    # Différence significative si p-value < 0.05
    significatif_5pct <- p_value < 0.05
    significatif_10pct <- p_value < 0.10
    
    message("Test triangulaire - Participants: ", n_total, 
            " | Bonnes réponses: ", n_correct, 
            " | Ratio: ", ratio, 
            " | p-value: ", round(p_value, 4),
            " | Significatif (5%): ", significatif_5pct,
            " | Significatif (10%): ", significatif_10pct)
    
    result <- tibble(
      IDTEST = unique(combined_segment$TrialName)[1],
      Type = "Triangulaire",
      Ref = ref_product,
      Candidat = candidat_product,
      Ratio = ratio,
      N_tot = n_total,
      N_correct = n_correct,
      `P-value` = round(p_value, 4),
      Significatif_5pct = significatif_5pct,
      Significatif_10pct = significatif_10pct,
      IC_inf = round(test_result$conf.int[1], 3),
      IC_sup = round(test_result$conf.int[2], 3)
    )
    
    return(result)
    
  }, error = function(e) {
    message("ERREUR dans handle_triangular_test_complete: ", e$message)
    return(tibble(
      IDTEST = "ERREUR",
      Type = "Triangulaire",
      Ref = NA_character_,
      Candidat = NA_character_,
      Ratio = NA_real_,
      N_tot = NA_integer_,
      N_correct = NA_integer_,
      `P-value` = NA_real_,
      Significatif_5pct = NA,
      Significatif_10pct = NA,
      IC_inf = NA_real_,
      IC_sup = NA_real_
    ))
  })
}


# ===== FONCTION DE TRAITEMENT DES SEGMENTS TRIANGULAIRES =====
process_triangular_segments <- function(segments, file_path) {
  triangular_indices <- which(sapply(segments, function(seg) {
    !is.na(seg$NomFonction[1]) && str_detect(seg$NomFonction[1], "Triangulaire|triangle")
  }))
  
  if(length(triangular_indices) == 0) {
    return(NULL)
  }
  
  all_triangular_data <- bind_rows(segments[triangular_indices])
  message("Fusion de ", length(triangular_indices), " segments triangulaires en un seul")
  
  result <- handle_triangular_test_complete(all_triangular_data, file_path)
  return(result)
}

# ===== FONCTION DE TRAITEMENT TRIANGULAIRE SIMPLE AVEC P-VALUE =====
handle_triangular_test <- function(segment) {
  tryCatch({
    message("Traitement test triangulaire: ", unique(segment$NomFonction))
    
    # Calcul des paramètres du test
    n_total <- sum(!is.na(segment$Value))  # n = nb de participants
    n_correct <- sum(segment$Value == 1, na.rm = TRUE)  # x = nb de bonnes réponses
    ratio <- round(n_correct / n_total, 3)
    
    # Test binomial triangulaire : H0: p = 1/3 vs H1: p > 1/3
    test_result <- binom.test(n_correct, n_total, p = 1/3, alternative = "greater")
    p_value <- test_result$p.value
    
    # Seuils de significativité
    significatif_5pct <- p_value < 0.05
    significatif_10pct <- p_value < 0.10
    
    message("Test triangulaire - Participants: ", n_total, 
            " | Bonnes réponses: ", n_correct, 
            " | Ratio: ", ratio, 
            " | p-value: ", round(p_value, 4),
            " | Significatif (5%): ", significatif_5pct)
    
    result <- tibble(
      IDTest = unique(segment$TrialName)[1],
      Segment = paste("Triangulaire", unique(segment$NomFonction)[1], sep = " - "),
      `ID segment` = 1,
      TestType = "Triangulaire",
      N_total = n_total,
      N_correct = n_correct,
      Ratio = ratio,
      P_value = round(p_value, 4),
      Significatif_5pct = significatif_5pct,
      Significatif_10pct = significatif_10pct,
      IC_inf = round(test_result$conf.int[1], 3),
      IC_sup = round(test_result$conf.int[2], 3)
    )
    
    return(result)
    
  }, error = function(e) {
    message("ERREUR dans handle_triangular_test: ", e$message)
    return(NULL)
  })
}


# ===== FONCTION D'ANALYSE ITÉRATIVE DES JUGES =====
analyze_judges_iterative <- function(segment) {
  if (length(segment$AttributeName) > 0 && !is.na(segment$AttributeName[1]) && 
      str_detect(str_to_lower(segment$AttributeName[1]), "odeur corporell")) {
    
    message("Traitement MO détecté - Pas de filtrage des juges")
    
    return(list(
      segment = segment,
      removed_judges = character(0),
      n_initial = n_distinct(segment$CJ),
      n_final = n_distinct(segment$CJ)
    ))
  }
  
  n_judges_initial <- n_distinct(segment$CJ)
  removed_judges_total <- c()
  current_data <- segment
  
  repeat {
    n_judges_current <- n_distinct(current_data$CJ)
    if (n_judges_current <= 8) {
      message("Seuil minimal atteint (8 juges) - Arrêt du filtrage")
      break
    }
    
    model <- aov(Value ~ CJ, data = current_data)
    anova_res <- anova(model)
    p_value <- anova_res["CJ", "Pr(>F)"]
    
    if (is.na(p_value)) {
      message("Problème de calcul ANOVA. Arrêt de l'itération pour ce segment.")
      break
    }
    
    if (p_value >= 0.05) {
      message("Effet juge non significatif (p=", round(p_value, 4), ") - Arrêt du filtrage")
      break
    }
    
    if (n_judges_current <= (2/3) * n_judges_initial) {
      message("Seuil de conservation atteint (2/3 des juges initiaux) - Arrêt du filtrage")
      break
    }
    
    judge_stats <- current_data %>%
      group_by(CJ) %>%
      summarise(MeanScore = mean(Value, na.rm = TRUE), .groups = 'drop') %>%
      mutate(
        OverallMean = mean(MeanScore, na.rm = TRUE),
        AbsDeviation = abs(MeanScore - OverallMean)
      )
    
    judge_to_remove <- judge_stats %>%
      slice_max(AbsDeviation, n = 1) %>%
      pull(CJ)
    
    removed_judges_total <- c(removed_judges_total, judge_to_remove)
    current_data <- current_data %>% filter(CJ != judge_to_remove)
    
    message("Juge retiré: ", judge_to_remove,
            " | Déviation: ", round(max(judge_stats$AbsDeviation), 2),
            " | Nouveau n juges: ", n_judges_current - 1)
  }
  
  return(list(
    segment = current_data,
    removed_judges = unique(removed_judges_total),
    n_initial = n_judges_initial,
    n_final = n_distinct(current_data$CJ)
  ))
}

# ===== FONCTION D'ANALYSE DES PRODUITS =====
analyze_products <- function(segment, segment_index, file_path = NULL) {
  tryCatch({
    seg_data <- segment
    
    if(str_detect(segment$NomFonction[1], "Triangulaire") || 
       str_detect(segment$NomFonction[1], "triangle")) {
      
      message("Détection test triangulaire pour segment: ", segment$NomFonction[1])
      
      if(!is.null(file_path)) {
        return(handle_triangular_test_complete(segment, file_path))
      } else {
        return(handle_triangular_test(segment))
      }
    }
    
    if (n_distinct(seg_data$ProductName) < 2) {
      message("Analyse impossible : moins de 2 produits dans le segment")
      return(NULL)
    }
    
    stats_df <- seg_data %>%
      group_by(ProductName) %>%
      summarise(
        Mean = ifelse(n() == 0, NA, round(mean(Value, na.rm = TRUE), 2)),
        Sd = ifelse(n() < 2, NA, round(sd(Value, na.rm = TRUE), 2)),
        n = n(),
        .groups = 'drop'
      )
    
    if (any(stats_df$n == 0)) {
      warning("Groupe produit vide détecté: ", segment$AttributeName[1])
    }
    
    model <- aov(Value ~ ProductName, data = seg_data)
    anova_result <- anova(model)
    p_value_produit <- anova_result["ProductName", "Pr(>F)"]
    
    significatif_10pct <- !is.na(p_value_produit) && p_value_produit < 0.10
    
    snk_result <- tryCatch({
      SNK.test(model, "ProductName", group = TRUE, alpha = 0.10)
    }, error = function(e) {
      message("SNK échoué, utilisation de Duncan avec alpha=0.10")
      duncan.test(model, "ProductName", group = TRUE, alpha = 0.10)
    })
    
    result_df <- snk_result$groups %>%
      as.data.frame() %>%
      rownames_to_column("ProductName") %>%
      rename(Classe = groups) %>%
      select(ProductName, Classe) %>%
      left_join(stats_df, by = "ProductName")
    
    result_df <- result_df %>%
      mutate(
        IDTest = trial_name,
        Segment = paste(segment$AttributeName[1],
                        segment$NomFonction[1], sep = " - "),
        `ID segment` = segment_index,
        P_value_produit = round(p_value_produit, 4),
        Significatif_10pct = significatif_10pct,
        TestType = "Standard"
      ) %>%
      select(
        IDTest,
        Segment,
        `ID segment`,
        Product = ProductName,
        Classe,
        Mean,
        Sd,
        n,
        P_value_produit,
        Significatif_10pct,
        TestType
      )
    
    return(result_df)
    
  }, error = function(e) {
    warning("Erreur analyse produits: ", e$message,
            " dans ", segment$AttributeName[1], " - ", segment$NomFonction[1])
    return(NULL)
  })
}

# ===== FONCTION DE VÉRIFICATION DES SEGMENTS =====
verify_segments <- function(segment) {
  nom_fonction <- na.omit(segment$NomFonction)
  is_triangulaire <- length(nom_fonction) > 0 && 
    any(str_detect(nom_fonction, "Triangulaire|triangle"))
  
  if (is_triangulaire) {
    return(list(
      Products = NA_integer_,
      Judges = n_distinct(segment$CJ),
      MissingValues = sum(is.na(segment$Value)),
      MinJudgesOK = TRUE,
      MinProductsOK = TRUE,
      OutOfRange = sum(segment$Value < 0 | segment$Value > 1, na.rm = TRUE)
    ))
  }
  
  n_products <- n_distinct(segment$ProductName)
  n_judges <- n_distinct(segment$CJ)
  non_numeric <- suppressWarnings(
    sum(is.na(as.numeric(segment$Value)) & !is.na(segment$Value))
  )
  
  list(
    Products = n_products,
    Judges = n_judges,
    MissingValues = sum(is.na(segment$Value)),
    NonNumericValues = non_numeric,
    ValuesOver10 = sum(segment$Value > 10, na.rm = TRUE),
    NegativeValues = sum(segment$Value < 0, na.rm = TRUE),
    OutOfRange = sum(segment$Value < 0 | segment$Value > 10, na.rm = TRUE),
    MinJudgesOK = n_judges >= 3,
    MinProductsOK = n_products >= 2
  )
}

# ===== BOUCLE PRINCIPALE COMPLÈTE =====
for (file_path in excel_files) {
  file_basename <- basename(file_path)
  source_name <- tools::file_path_sans_ext(file_basename)
  
  if(is_file_already_processed(file_path, tracking_data)) {
    message("\n=== FICHIER DÉJÀ TRAITÉ (SKIP): ", file_basename, " ===")
    files_skipped <- files_skipped + 1
    next
  }
  
  message("\n=== TRAITEMENT NOUVEAU FICHIER: ", file_basename, " ===")
  files_new <- files_new + 1
  
  # Lecture et validation
  sheet_names <- tryCatch({
    excel_sheets(file_path)
  }, error = function(e) {
    log_probleme("ERREUR_LECTURE", paste("Impossible de lire les onglets:", e$message), file_path)
    tracking_data <<- update_tracking(file_path, "ERREUR_LECTURE", NA, tracking_data)
    return(NULL)
  })
  
  if(is.null(sheet_names)) next
  
  if(!"Results" %in% sheet_names) {
    log_probleme("ONGLET_MANQUANT", "Onglet 'Results' manquant", file_path)
    tracking_data <- update_tracking(file_path, "ONGLET_MANQUANT", NA, tracking_data)
    next
  }
  
  # Lecture des données
  file_data <- tryCatch({
    read_excel(file_path, sheet = "Results") %>%
      mutate(SourceFile = basename(file_path))
  }, error = function(e) {
    log_probleme("ERREUR_LECTURE_RESULTS", paste("Erreur lecture Results:", e$message), file_path)
    tracking_data <<- update_tracking(file_path, "ERREUR_LECTURE_RESULTS", NA, tracking_data)
    return(NULL)
  })
  
  if(is.null(file_data)) next
  
  # Validation de l'unicité du trial
  tryCatch({
    n_trials <- n_distinct(file_data$TrialName)
    
    if (n_trials != 1) {
      issue_msg <- paste("MULTIPLE TRIALNAMES (", n_trials, 
                         ") | Fichier:", basename(file_path))
      
      data_issues_log[[length(data_issues_log) + 1]] <- issue_msg
      tracking_data <- update_tracking(file_path, "MULTIPLE_TRIALNAMES", 
                                       nrow(file_data), tracking_data)
      next
    } else {
      trial_name <- unique(file_data$TrialName)
    }
    
    # Validation de la cohérence des données
    consistency_issues <- validate_data_consistency(file_data)
    
    if(length(consistency_issues) > 0) {
      walk(consistency_issues, ~log_probleme("COHERENCE", .x, file_path))
    }
    
    # Préparation et nettoyage des données
    df <- file_data %>%
      mutate(Value = suppressWarnings(as.numeric(gsub(",", ".", Value)))) %>%
      select(-any_of("NR")) %>%
      mutate(JudgeStatus = "conserved")
    
    # Stockage des données brutes
    all_raw_data[[basename(file_path)]] <- df
    
    # Segmentation pour analyse
    segments <- df %>%
      group_by(AttributeName, NomFonction) %>%
      group_split()
    
    message("Nombre de segments dans ce fichier: ", length(segments))
    
  }, error = function(e) {
    log_probleme("ERREUR_TRAITEMENT", paste("Erreur générale:", e$message), file_path)
    tracking_data <- update_tracking(file_path, "ERREUR_TRAITEMENT", NA, tracking_data)
    next
  })
  
  # Vérification des segments
  verification_results <- map(segments, verify_segments)
  
  # Logging des problèmes détectés
  for(i in seq_along(verification_results)) {
    res <- verification_results[[i]]
    seg <- segments[[i]]
    seg_name <- paste(seg$AttributeName[1], seg$NomFonction[1], sep = " - ")
    
    if(!isTRUE(res$MinJudgesOK)) {
      issue_msg <- paste("TROP PEU DE JUGES (", res$Judges, "/3) | Segment:", seg_name)
      data_issues_log[[length(data_issues_log) + 1]] <- issue_msg
    }
    
    if(!isTRUE(res$MinProductsOK)) {
      issue_msg <- paste("TROP PEU DE PRODUITS (", res$Products, "/2) | Segment:", seg_name)
      data_issues_log[[length(data_issues_log) + 1]] <- issue_msg
    }
    
    if(res$OutOfRange > 0) {
      issue_msg <- paste("VALEURS HORS LIMITES (", res$OutOfRange, ") | Segment:", seg_name)
      data_issues_log[[length(data_issues_log) + 1]] <- issue_msg
    }
  }
  
  # Traitement adaptatif des segments
  segments_processed <- list()
  
  for (i in seq_along(segments)) {
    seg_name <- paste(segments[[i]]$AttributeName[1], segments[[i]]$NomFonction[1], sep = " - ")
    seg_verif <- verification_results[[i]]
    
    is_triangular <- !is.na(segments[[i]]$NomFonction[1]) && 
      str_detect(segments[[i]]$NomFonction[1], "Triangulaire|triangle")
    
    if(!isTRUE(is_triangular) && 
       (!isTRUE(seg_verif$MinProductsOK) || !isTRUE(seg_verif$MinJudgesOK))) {
      next
    }
    
    if (is_triangular) {
      message("Application test TRIANGULAIRE: ", seg_name)
      result <- list(
        segment = segments[[i]],
        removed_judges = character(0),
        n_initial = n_distinct(segments[[i]]$CJ),
        n_final = n_distinct(segments[[i]]$CJ)
      )
      
    } else if (str_detect(seg_name, regex("prox", ignore_case = TRUE))) {
      message("Application test PROXIMITE: ", seg_name)
      result <- handle_proximity_test(segments[[i]])
      
    } else if (str_detect(seg_name, regex("odeur corporell", ignore_case = TRUE))) {
      message("Application test MO (odeur corporell): ", seg_name)
      result <- analyze_judges_iterative(segments[[i]])
      
    } else {
      message("Application test STANDARD: ", seg_name)
      result <- analyze_judges_iterative(segments[[i]])
    }
    
    segments_processed[[i]] <- result$segment
    
    if (length(result$removed_judges) > 0) {
      judge_info <- data.frame(
        File = basename(file_path),
        Segment = seg_name,
        SegmentIndex = i,
        RemovedJudges = paste(result$removed_judges, collapse = ", "),
        JudgesInitial = result$n_initial,
        JudgesFinal = result$n_final,
        stringsAsFactors = FALSE
      )
      judge_removal_info[[length(judge_removal_info) + 1]] <- judge_info
    }
    
    message("Juges initiaux: ", result$n_initial,
            " | Juges finaux: ", result$n_final,
            " | Juges retirés: ", ifelse(length(result$removed_judges) > 0,
                                         paste(result$removed_judges, collapse = ", "), "aucun"))
  }
  
  # Consolidation des segments traités
  final_data <- bind_rows(segments_processed)
  
  # Analyse différentielle des produits
  triangular_results <- process_triangular_segments(segments_processed, file_path)
  
  non_triangular_segments <- segments_processed[!sapply(segments_processed, function(seg) {
    !is.na(seg$NomFonction[1]) && str_detect(seg$NomFonction[1], "Triangulaire|triangle")
  })]
  
  standard_results <- map2(non_triangular_segments, seq_along(non_triangular_segments), 
                           ~analyze_products(.x, .y, file_path)) %>%
    compact() %>%
    bind_rows()
  
  # Consolidation finale des résultats
  file_results <- bind_rows(
    triangular_results,
    standard_results
  )
  
  if (nrow(file_results) > 0) {
    all_results[[basename(file_path)]] <- file_results
    print(file_results)
  }
  
  # Création de la table de tracking des juges pour ce fichier
  if(length(judge_removal_info) > 0) {
    all_judge_info_df <- bind_rows(judge_removal_info)
    all_raw_data_df <- bind_rows(all_raw_data)
    judge_tracking_table <- create_judge_tracking_table(all_judge_info_df, all_raw_data_df)
  } else {
    judge_tracking_table <- tibble()
  }
  
  # ===== SAUVEGARDE DANS LES BASES DE DONNÉES =====
  
  # 1. Sauvegarder les données brutes avec SOURCE_NAME
  if(save_raw_data_to_db(file_data, source_name)) {
    message(" Données brutes sauvegardées dans SA_RAW_DATA")
  }
  
  # 2. Déterminer le type de test et sauvegarder les résultats
  test_type <- determine_test_type(segments)
  if(exists("file_results") && nrow(file_results) > 0) {
    if(save_results_to_db(file_results, source_name, test_type)) {
      message(" Résultats sauvegardés dans SA_RESULTS_DATA (", test_type, ")")
    }
  }
  
  # 3. Sauvegarder le tracking des juges
  if(exists("judge_tracking_table") && nrow(judge_tracking_table) > 0) {
    if(save_judges_to_db(judge_tracking_table, source_name)) {
      message(" Tracking juges sauvegardé dans SA_JUDGES")
    }
  }
  
  # 4. Sauvegarder les couples métadonnées
  if(save_metadata_couples(file_data, source_name)) {
    message(" Couples métadonnées sauvegardés dans Metadata_SA")
  }
  
  # Mise à jour du tracking
  tracking_data <- update_tracking(file_path, "SUCCES", nrow(file_data), tracking_data)
  files_processed <- files_processed + 1
  
  # Génération des fichiers Excel individuels
  current_file <- as.character(file_path)
  
  raw_data_with_status <- file_data %>%
    mutate(
      JudgeStatus = "conserved",
      SourceFile = basename(current_file)
    )
  
  file_judge_changes <- judge_removal_info %>% 
    keep(~ .x$File == basename(current_file)) %>%
    map_df(~ .x)
  
  if(nrow(file_judge_changes) > 0) {
    raw_data_with_status <- raw_data_with_status %>% 
      mutate(JudgeStatus = case_when(
        CJ %in% unlist(strsplit(file_judge_changes$RemovedJudges, ", ")) &
          paste(AttributeName, NomFonction, sep = " - ") %in% file_judge_changes$Segment ~ "removed",
        TRUE ~ JudgeStatus
      ))
  }
  
  # Génération du fichier de sortie individuel
  # Génération du fichier de sortie individuel
  output_file_name <- paste0("ANALYSE_", tools::file_path_sans_ext(basename(current_file)), ".xlsx")
  output_file_path <- file.path(output_base_dir, output_file_name)
  
  tryCatch({
    # Préparer les données pour l'export
    export_data <- list()
    
    # 1. Données brutes avec statut des juges
    export_data$"Donnees_Brutes" <- raw_data_with_status %>%
      select(TrialName, CJ, ProductName, AttributeName, NomFonction, Value, JudgeStatus, SourceFile) %>%
      arrange(AttributeName, NomFonction, CJ, ProductName)
    
    # 2. Résultats d'analyse
    if(exists("file_results") && nrow(file_results) > 0) {
      export_data$"Resultats_Analyse" <- file_results
    } else {
      export_data$"Resultats_Analyse" <- tibble(
        Message = "Aucun résultat généré pour ce fichier",
        Raison = "Données insuffisantes ou erreur de traitement",
        Timestamp = Sys.time()
      )
    }
    
    # 3. Tracking des juges pour ce fichier
    if(exists("judge_tracking_table") && nrow(judge_tracking_table) > 0) {
      export_data$"Tracking_Juges" <- judge_tracking_table
    } else {
      export_data$"Tracking_Juges" <- tibble(
        Message = "Aucun tracking de juges disponible",
        Timestamp = Sys.time()
      )
    }
    
    # 4. Informations sur les juges retirés
    if(nrow(file_judge_changes) > 0) {
      export_data$"Juges_Retires" <- file_judge_changes
    } else {
      export_data$"Juges_Retires" <- tibble(
        Message = "Aucun juge retiré pour ce fichier",
        Timestamp = Sys.time()
      )
    }
    
    # 5. Résumé du fichier
    file_summary <- tibble(
      Fichier_Source = basename(current_file),
      Trial_Name = trial_name,
      Date_Traitement = Sys.time(),
      Nb_Lignes_Brutes = nrow(file_data),
      Nb_Segments = length(segments),
      Nb_Juges_Total = n_distinct(file_data$CJ),
      Nb_Produits_Total = n_distinct(file_data$ProductName),
      Nb_Attributs_Total = n_distinct(file_data$AttributeName),
      Type_Test_Detecte = test_type,
      Statut_Traitement = "SUCCES"
    )
    
    export_data$"Resume_Fichier" <- file_summary
    
    # 6. Log des problèmes pour ce fichier (si applicable)
    file_issues <- data_issues_log[str_detect(data_issues_log, basename(current_file))]
    if(length(file_issues) > 0) {
      export_data$"Problemes_Detectes" <- tibble(
        Probleme = file_issues,
        Timestamp = Sys.time()
      )
    }
    
    # Écriture du fichier Excel
    write_xlsx(export_data, output_file_path)
    message("Fichier individuel généré: ", output_file_path)
    
  }, error = function(e) {
    message("ERREUR génération fichier individuel: ", e$message)
    
    # Créer un fichier d'erreur minimal
    error_data <- list(
      "ERREUR" = tibble(
        Fichier = basename(current_file),
        Erreur = e$message,
        Timestamp = Sys.time(),
        Message = "Échec de génération du rapport complet"
      )
    )
    
    tryCatch({
      write_xlsx(error_data, output_file_path)
      message("Fichier d'erreur créé: ", output_file_path)
    }, error = function(e2) {
      message("Impossible de créer même le fichier d'erreur: ", e2$message)
    })
  })
}

# ===== GÉNÉRATION DU FICHIER CONSOLIDÉ GLOBAL =====
message("\n=== GÉNÉRATION DU FICHIER CONSOLIDÉ GLOBAL ===")

consolidated_file_path <- file.path(output_base_dir, "ANALYSE_CONSOLIDEE_GLOBALE.xlsx")

tryCatch({
  consolidated_data <- list()
  
  # 1. Résumé global de l'analyse
  global_summary <- tibble(
    Date_Analyse = Sys.time(),
    Fichiers_Detectes = length(excel_files),
    Fichiers_Deja_Traites = files_skipped,
    Nouveaux_Fichiers = files_new,
    Fichiers_Traites_Succes = files_processed,
    Fichiers_En_Erreur = files_new - files_processed,
    Taux_Succes = round((files_processed / files_new) * 100, 1),
    Dossier_Source = raw_data_dir,
    Dossier_Sortie = output_base_dir
  )
  
  consolidated_data$"Resume_Global" <- global_summary
  
  # 2. Consolidation de tous les résultats
  if(length(all_results) > 0) {
    all_results_df <- map_dfr(all_results, ~.x, .id = "Fichier_Source")
    consolidated_data$"Tous_Resultats" <- all_results_df
    
    # Statistiques par type de test
    test_stats <- all_results_df %>%
      group_by(TestType = ifelse("TestType" %in% names(.), TestType, "Standard")) %>%
      summarise(
        Nb_Tests = n(),
        Nb_Fichiers = n_distinct(Fichier_Source),
        .groups = 'drop'
      )
    
    consolidated_data$"Stats_Par_Type_Test" <- test_stats
    
  } else {
    consolidated_data$"Tous_Resultats" <- tibble(
      Message = "Aucun résultat généré",
      Raison = "Tous les fichiers ont échoué ou étaient déjà traités"
    )
  }
  
  # 3. Consolidation des données brutes
  if(length(all_raw_data) > 0) {
    all_raw_consolidated <- bind_rows(all_raw_data, .id = "Fichier_Source")
    
    # Statistiques des données brutes
    raw_stats <- all_raw_consolidated %>%
      group_by(Fichier_Source) %>%
      summarise(
        Trial_Name = first(TrialName),
        Nb_Lignes = n(),
        Nb_Juges = n_distinct(CJ),
        Nb_Produits = n_distinct(ProductName),
        Nb_Attributs = n_distinct(AttributeName),
        Valeur_Min = min(Value, na.rm = TRUE),
        Valeur_Max = max(Value, na.rm = TRUE),
        Valeur_Moyenne = round(mean(Value, na.rm = TRUE), 2),
        Nb_Valeurs_Manquantes = sum(is.na(Value)),
        .groups = 'drop'
      )
    
    consolidated_data$"Stats_Donnees_Brutes" <- raw_stats
    
    # Top 20 des lignes de données brutes (échantillon)
    sample_raw <- all_raw_consolidated %>%
      slice_head(n = 20) %>%
      select(Fichier_Source, TrialName, CJ, ProductName, AttributeName, NomFonction, Value)
    
    consolidated_data$"Echantillon_Donnees_Brutes" <- sample_raw
    
  } else {
    consolidated_data$"Stats_Donnees_Brutes" <- tibble(
      Message = "Aucune donnée brute disponible"
    )
  }
  
  # 4. Consolidation du tracking des juges
  if(length(judge_removal_info) > 0) {
    all_judge_info_consolidated <- bind_rows(judge_removal_info)
    
    # Statistiques des juges retirés
    judge_stats <- all_judge_info_consolidated %>%
      group_by(File) %>%
      summarise(
        Nb_Segments_Avec_Retraits = n(),
        Nb_Juges_Retires_Total = sum(str_count(RemovedJudges, ",") + 1, na.rm = TRUE),
        Juges_Initiaux_Moyen = round(mean(JudgesInitial, na.rm = TRUE), 1),
        Juges_Finaux_Moyen = round(mean(JudgesFinal, na.rm = TRUE), 1),
        Taux_Conservation_Moyen = round(mean(JudgesFinal / JudgesInitial, na.rm = TRUE), 3),
        .groups = 'drop'
      )
    
    consolidated_data$"Stats_Juges_Retires" <- judge_stats
    consolidated_data$"Detail_Juges_Retires" <- all_judge_info_consolidated
    
  } else {
    consolidated_data$"Stats_Juges_Retires" <- tibble(
      Message = "Aucun juge retiré dans cette analyse"
    )
  }
  
  # 5. Log consolidé des problèmes
  if(length(data_issues_log) > 0) {
    issues_df <- tibble(
      Probleme = data_issues_log,
      Timestamp = Sys.time()
    ) %>%
      separate(Probleme, into = c("Type", "Details"), sep = "] ", extra = "merge") %>%
      mutate(Type = str_remove(Type, "^\\["))
    
    # Statistiques des problèmes
    issues_stats <- issues_df %>%
      group_by(Type) %>%
      summarise(
        Nb_Occurrences = n(),
        .groups = 'drop'
      ) %>%
      arrange(desc(Nb_Occurrences))
    
    consolidated_data$"Stats_Problemes" <- issues_stats
    consolidated_data$"Detail_Problemes" <- issues_df
    
  } else {
    consolidated_data$"Stats_Problemes" <- tibble(
      Message = "Aucun problème détecté"
    )
  }
  
  # 6. Informations de tracking des fichiers
  consolidated_data$"Tracking_Fichiers" <- tracking_data %>%
    arrange(desc(Date_Traitement))
  
  # 7. Configuration utilisée
  config_info <- tibble(
    Parametre = c("Dossier_Source", "Dossier_Sortie", "Host_DB", "Port_DB", 
                  "Base_Raw_Data", "Base_Results", "Base_Judges", "Base_Metadata"),
    Valeur = c(raw_data_dir, output_base_dir, DB_CONFIG$host, DB_CONFIG$port,
               DATABASES$RAW_DATA, DATABASES$RESULTS, DATABASES$JUDGES, DATABASES$METADATA)
  )
  
  consolidated_data$"Configuration" <- config_info
  
  # Écriture du fichier consolidé
  write_xlsx(consolidated_data, consolidated_file_path)
  message("Fichier consolidé global généré: ", consolidated_file_path)
  
}, error = function(e) {
  message("ERREUR génération fichier consolidé: ", e$message)
  
  # Créer un fichier d'erreur minimal
  error_consolidated <- list(
    "ERREUR_CONSOLIDATION" = tibble(
      Erreur = e$message,
      Timestamp = Sys.time(),
      Message = "Échec de génération du rapport consolidé"
    ),
    "Resume_Partiel" = tibble(
      Fichiers_Detectes = length(excel_files),
      Fichiers_Traites = files_processed,
      Date_Analyse = Sys.time()
    )
  )
  
  tryCatch({
    write_xlsx(error_consolidated, consolidated_file_path)
    message("Fichier d'erreur consolidé créé: ", consolidated_file_path)
  }, error = function(e2) {
    message("Impossible de créer le fichier d'erreur consolidé: ", e2$message)
  })
})

# ===== NETTOYAGE ET SAUVEGARDE FINALE =====
save_tracking_data(tracking_data)

# ===== RÉSUMÉ FINAL DÉTAILLÉ =====
message("\n" + paste(rep("=", 80), collapse = ""))
message("=== RÉSUMÉ FINAL DE L'ANALYSE AVEC INTÉGRATION MULTI-DATABASES ===")
message(paste(rep("=", 80), collapse = ""))

message("\n FICHIERS TRAITÉS :")
message("   • Fichiers Excel détectés : ", length(excel_files))
message("   • Fichiers déjà traités (skippés) : ", files_skipped)
message("   • Nouveaux fichiers détectés : ", files_new)
message("   • Nouveaux fichiers traités avec succès : ", files_processed)
message("   • Fichiers en erreur : ", files_new - files_processed)

if(files_new > 0) {
  success_rate <- round((files_processed / files_new) * 100, 1)
  message("   • Taux de succès : ", success_rate, "%")
}

message("\n🗄️ BASES DE DONNÉES MISES À JOUR :")
message("    SA_RAW_DATA : Données brutes avec SOURCE_NAME et JudgeStatus")
message("    SA_RESULTS_DATA : Résultats par type de test (Strength/Proximity/Triangulaire/MO)")
message("    SA_JUDGES : Tracking détaillé des juges")
message("    Metadata_SA : Couples (SOURCE_NAME, ProductName) pour l'application")

message("\n📊 STATISTIQUES GLOBALES :")
if(length(all_results) > 0) {
  total_tests <- sum(sapply(all_results, nrow))
  message("   • Total de tests analysés : ", total_tests)
}

if(length(all_raw_data) > 0) {
  total_raw_lines <- sum(sapply(all_raw_data, nrow))
  total_judges <- length(unique(unlist(sapply(all_raw_data, function(x) unique(x$CJ)))))
  total_products <- length(unique(unlist(sapply(all_raw_data, function(x) unique(x$ProductName)))))
  
  message("   • Total de lignes de données brutes : ", total_raw_lines)
  message("   • Nombre total de juges uniques : ", total_judges)
  message("   • Nombre total de produits uniques : ", total_products)
}

if(length(judge_removal_info) > 0) {
  total_removed_judges <- sum(sapply(judge_removal_info, function(x) {
    sum(str_count(x$RemovedJudges, ",") + 1, na.rm = TRUE)
  }))
  message("   • Total de juges retirés : ", total_removed_judges)
}

message("\n PROBLÈMES DÉTECTÉS :")
if(length(data_issues_log) > 0) {
  message("   • Nombre total de problèmes : ", length(data_issues_log))
  
  # Compter par type de problème
  problem_types <- str_extract(data_issues_log, "\\[([^\\]]+)\\]") %>%
    str_remove_all("\\[|\\]") %>%
    table()
  
  for(i in seq_along(problem_types)) {
    message("     - ", names(problem_types)[i], " : ", problem_types[i])
  }
} else {
  message("   Aucun problème détecté")
}

message("\n FICHIERS GÉNÉRÉS :")
message("   • Dossier de sortie : ", output_base_dir)
message("   • Fichiers individuels : ", files_processed, " fichiers")
message("   • Fichier consolidé global : ANALYSE_CONSOLIDEE_GLOBALE.xlsx")
message("   • Fichier de tracking : TRACKING_FICHIERS.xlsx")

message("\n CONNEXIONS BASES DE DONNÉES :")
message("   • Serveur : ", DB_CONFIG$host, ":", DB_CONFIG$port)
message("   • Utilisateur : ", DB_CONFIG$user)
message("   • Bases utilisées : ", paste(unlist(DATABASES), collapse = ", "))

message("\n TEMPS D'EXÉCUTION :")
message("   • Début : ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
message("   • Fin : ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"))

message("\n" + paste(rep("=", 80), collapse = ""))
message("ANALYSE TERMINÉE AVEC SUCCÈS !")
message("   Les données sont maintenant disponibles dans les 4 bases PostgreSQL")
message("   et les fichiers Excel ont été générés dans : ", output_base_dir)
message(paste(rep("=", 80), collapse = ""))

# Nettoyage de l'environnement (optionnel)
# rm(list = ls())
# gc()

message("\ncript terminé à : ", Sys.time())

  
