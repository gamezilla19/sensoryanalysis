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
# ===== FONCTION DE DIAGNOSTIC MISE À JOUR =====
# ===== FONCTION DE DIAGNOSTIC MISE À JOUR =====
debug_database_connections <- function() {
  cat("\n🔍 DIAGNOSTIC COMPLET DES BASES DE DONNÉES\n")
  cat(paste(rep("=", 50), collapse = ""), "\n")
  
  # 1. Test connexion serveur
  cat("\n1. Test connexion serveur PostgreSQL...\n")
  con_server <- tryCatch({
    dbConnect(RPostgres::Postgres(),
              host = DB_CONFIG$host,
              port = DB_CONFIG$port,
              user = DB_CONFIG$user,
              password = DB_CONFIG$password,
              dbname = "postgres")
  }, error = function(e) {
    cat("❌ ERREUR serveur:", e$message, "\n")
    return(NULL)
  })
  
  if(is.null(con_server)) {
    cat("🚨 ARRÊT: Impossible de se connecter au serveur PostgreSQL\n")
    return(FALSE)
  }
  
  cat("✅ Connexion serveur OK\n")
  
  # 2. Lister les bases de données existantes
  cat("\n2. Bases de données existantes sur le serveur:\n")
  existing_dbs <- dbGetQuery(con_server, "SELECT datname FROM pg_database WHERE datistemplate = false;")
  for(db_name in existing_dbs$datname) {
    cat("  -", db_name, "\n")
  }
  
  # 3. Vérifier chaque base de données nécessaire
  cat("\n3. Vérification des bases de données nécessaires:\n")
  required_dbs <- c("SA_RAW_DATA", "SA_RESULTS_DATA", "SA_JUDGES", "SA_METADATA")
  
  for(db in required_dbs) {
    cat("\n--- Base de données:", db, "---\n")
    if(db %in% existing_dbs$datname) {
      cat("✅", db, "existe sur le serveur\n")
      
      # Test connexion à cette base de données
      con_test <- create_db_connection(db)
      if(!is.null(con_test)) {
        cat("✅ Connexion à", db, "réussie\n")
        
        # Lister les tables dans cette base de données
        tables <- dbListTables(con_test)
        if(length(tables) > 0) {
          cat("   Tables dans", db, ":", paste(tables, collapse = ", "), "\n")
          
          # Pour SA_RESULTS_DATA, vérifier les tables spécialisées
          if(db == "SA_RESULTS_DATA") {
            expected_tables <- c("strengthandmo_results", "proximity_results", "triangulaire_results")
            for(expected_table in expected_tables) {
              if(expected_table %in% tables) {
                cat("   ✅", expected_table, "présente\n")
              } else {
                cat("   ⚠️", expected_table, "manquante (sera créée automatiquement)\n")
              }
            }
          }
          
          # Pour SA_METADATA, vérifier les tables spécialisées
          if(db == "SA_METADATA") {
            expected_tables <- c("product_info", "test_info", "databrute")
            for(expected_table in expected_tables) {
              if(expected_table %in% tables) {
                cat("   ✅", expected_table, "présente\n")
              } else {
                cat("   ⚠️", expected_table, "manquante (sera créée automatiquement)\n")
              }
            }
          }
        } else {
          cat("   ⚠️  Base", db, "vide (aucune table)\n")
        }
        safe_disconnect(con_test)
      } else {
        cat("❌ Impossible de se connecter à la base", db, "\n")
      }
    } else {
      cat("❌ Base de données", db, "N'EXISTE PAS sur le serveur\n")
    }
  }
  
  safe_disconnect(con_server)
  return(TRUE)
}





# Lancer le diagnostic

debug_database_connections()


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
  METADATA = "SA_METADATA"
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
# ===== FONCTIONS DE CRÉATION DES TABLES CORRIGÉES =====

create_raw_data_table <- function(con) {
  
  if(is.null(con)) return(FALSE)
  
  
  
  tryCatch({
    
    # Vérifier si la table existe déjà
    
    if(dbExistsTable(con, "rawdata")) {
      
      message("Table rawdata existe déjà")
      
      return(TRUE)
      
    }
    
    
    
    # Créer la table rawdata avec des commandes séparées
    
    dbExecute(con, "CREATE TABLE IF NOT EXISTS rawdata (

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

    )")
    
    
    
    # Créer les index séparément
    
    dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_rawdata_source ON rawdata(source_name)")
    
    dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_rawdata_trial ON rawdata(trial_name)")
    
    dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_rawdata_product ON rawdata(product_name)")
    
    
    
    message("Table rawdata créée avec succès")
    
    return(TRUE)
    
    
    
  }, error = function(e) {
    
    message("Erreur création table rawdata : ", e$message)
    
    return(FALSE)
    
  })
  
}


# ===== FONCTIONS DE CRÉATION DES TABLES DE RÉSULTATS PAR TYPE =====

create_strength_results_table <- function(con) {
  if(is.null(con)) return(FALSE)
  
  tryCatch({
    if(!dbExistsTable(con, "strengthandmo_results")) {  # ✅ MINUSCULES
      dbExecute(con, "CREATE TABLE IF NOT EXISTS strengthandmo_results (
        id SERIAL PRIMARY KEY,
        source_name VARCHAR(255) NOT NULL,
        idtest VARCHAR(255),
        test_type VARCHAR(50),
        segment VARCHAR(500),
        segment_id INTEGER,
        product_name VARCHAR(255),
        classe VARCHAR(10),
        mean_value NUMERIC,
        sd_value NUMERIC,
        n_observations INTEGER,
        anova_5pct BOOLEAN,
        anova_10pct BOOLEAN,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )")
      
      # Créer les index séparément
      dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_strengthmo_source ON strengthandmo_results(source_name)")
      dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_strengthmo_type ON strengthandmo_results(test_type)")
      dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_strengthmo_product ON strengthandmo_results(product_name)")
      
      message("Table strengthandmo_results créée avec succès")
    }
    return(TRUE)
  }, error = function(e) {
    message("Erreur création table strengthandmo_results : ", e$message)
    return(FALSE)
  })
}


create_proximity_results_table <- function(con) {
  if(is.null(con)) return(FALSE)
  
  tryCatch({
    if(!dbExistsTable(con, "proximity_results")) {  # ✅ MINUSCULES
      dbExecute(con, "CREATE TABLE IF NOT EXISTS proximity_results (
        id SERIAL PRIMARY KEY,
        source_name VARCHAR(255) NOT NULL,
        idtest VARCHAR(255),
        test_type VARCHAR(50),
        segment VARCHAR(500),
        segment_id INTEGER,
        product_name VARCHAR(255),
        classe VARCHAR(10),
        mean_value NUMERIC,
        sd_value NUMERIC,
        n_observations INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )")
      
      # Créer les index séparément
      dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_proximity_source ON proximity_results(source_name)")
      dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_proximity_type ON proximity_results(test_type)")
      dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_proximity_product ON proximity_results(product_name)")
      
      message("Table proximity_results créée avec succès")
    }
    return(TRUE)
  }, error = function(e) {
    message("Erreur création table proximity_results : ", e$message)
    return(FALSE)
  })
}


create_triangular_results_table <- function(con) {
  if(is.null(con)) return(FALSE)
  
  tryCatch({
    if(!dbExistsTable(con, "triangulaire_results")) {  # ✅ MINUSCULES
      dbExecute(con, "CREATE TABLE IF NOT EXISTS triangulaire_results (
        id SERIAL PRIMARY KEY,
        source_name VARCHAR(255) NOT NULL,
        idtest VARCHAR(255),
        test_type VARCHAR(50),
        reference VARCHAR(255),
        candidate VARCHAR(255),
        n_total INTEGER,
        n_correct INTEGER,
        p_value NUMERIC,
        decision VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )")
      
      # Créer les index séparément
      dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_triangular_source ON triangulaire_results(source_name)")
      dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_triangular_type ON triangulaire_results(test_type)")
      dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_triangular_decision ON triangulaire_results(decision)")
      
      message("Table triangulaire_results créée avec succès")
    }
    return(TRUE)
  }, error = function(e) {
    message("Erreur création table triangulaire_results : ", e$message)
    return(FALSE)
  })
}



# ===== FONCTION POUR DÉTERMINER LE NOM DE LA TABLE =====
get_results_table_name <- function(test_type) {
  switch(test_type,
         "Strength" = "strengthandmo_results",           # ✅ MINUSCULES
         "Strength with Malodour" = "strengthandmo_results", # ✅ MINUSCULES
         "Proximity" = "proximity_results",              # ✅ MINUSCULES
         "Triangular" = "triangulaire_results",          # ✅ MINUSCULES
         "strengthandmo_results"  # Par défaut en minuscules
  )
}


# ===== FONCTION POUR CRÉER LA TABLE APPROPRIÉE =====
create_appropriate_results_table <- function(con, test_type) {
  switch(test_type,
         "Strength" = create_strength_results_table(con),
         "Strength with Malodour" = create_strength_results_table(con),
         "Proximity" = create_proximity_results_table(con),
         "Triangular" = create_triangular_results_table(con),
         create_strength_results_table(con)  # Par défaut
  )
}



create_judges_table <- function(con) {
  if(is.null(con)) return(FALSE)
  
  tryCatch({
    if(!dbExistsTable(con, "judge_tracking")) {
      dbExecute(con, "CREATE TABLE IF NOT EXISTS judge_tracking (
        id SERIAL PRIMARY KEY,
        cj VARCHAR(100),                          -- ✅ Pas de NOT NULL
        nb_fichiers_participes INTEGER,           -- ✅ NOUVEAU
        nb_evaluations_total INTEGER,             -- ✅ Total au lieu de par fichier
        moyenne_score_globale NUMERIC,            -- ✅ Moyenne globale
        attributes_evalues_total INTEGER,         -- ✅ Total
        produits_evalues_total INTEGER,           -- ✅ Total
        nb_segments_total INTEGER,                -- ✅ Total global
        nb_segments_retire_total INTEGER,         -- ✅ Total retraits
        nb_segments_conserve INTEGER,             -- ✅ Total conservés
        taux_conservation_global NUMERIC,         -- ✅ Taux global
        nb_fichiers_avec_retrait INTEGER,         -- ✅ NOUVEAU
        premier_fichier VARCHAR(255),             -- ✅ Référence
        dernier_fichier VARCHAR(255),             -- ✅ Référence
        date_analyse DATE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )")
      
      # ✅ INDEX OPTIMISÉS
      dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_judges_cj ON judge_tracking(cj)")
      dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_judges_taux_global ON judge_tracking(taux_conservation_global)")
      dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_judges_nb_fichiers ON judge_tracking(nb_fichiers_participes)")
      
      message("Table judge_tracking OPTIMISÉE créée avec succès")
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
      
      dbExecute(con, "CREATE TABLE IF NOT EXISTS databrute (

        id SERIAL PRIMARY KEY,

        idtest VARCHAR(255),

        productname VARCHAR(255),

        sourcefile VARCHAR(255),

        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP

      )")
      
      
      
      # Créer les index séparément
      
      dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_databrute_product ON databrute(productname)")
      
      dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_databrute_source ON databrute(sourcefile)")
      
      dbExecute(con, "CREATE UNIQUE INDEX IF NOT EXISTS idx_databrute_unique ON databrute(sourcefile, productname)")
      
      
      
      message("Table databrute créée avec succès dans SA_METADATA")
      
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
        judge_status = "conserved"  # Valeur par défaut au lieu de vérifier JudgeStatus
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
# ===== FONCTION DE SAUVEGARDE DES DONNÉES BRUTES AVEC STATUT JUGES =====
save_raw_data_with_judge_status <- function(raw_data, source_name, judge_removal_info_file = NULL) {
  con <- create_db_connection(DATABASES$RAW_DATA)
  if(is.null(con)) return(FALSE)
  
  tryCatch({
    # Créer la table si nécessaire
    create_raw_data_table(con)
    
    # Préparer les données de base
    raw_data_db <- raw_data %>%
      mutate(
        source_name = source_name,
        trial_name = TrialName,
        cj = CJ,
        product_name = ProductName,
        attribute_name = AttributeName,
        nom_fonction = NomFonction,
        value = as.numeric(Value),
        judge_status = "conserved"  # Valeur par défaut
      )
    
    # Si on a des informations sur les juges retirés, les appliquer
    if(!is.null(judge_removal_info_file) && nrow(judge_removal_info_file) > 0) {
      message("Application du statut des juges retirés...")
      
      # Pour chaque juge retiré, mettre à jour le statut
      for(i in 1:nrow(judge_removal_info_file)) {
        removed_judges <- unlist(strsplit(judge_removal_info_file$RemovedJudges[i], ", "))
        segment_name <- judge_removal_info_file$Segment[i]
        
        # Extraire AttributeName et NomFonction du segment
        segment_parts <- strsplit(segment_name, " - ")[[1]]
        if(length(segment_parts) >= 2) {
          attr_name <- segment_parts[1]
          nom_fonction <- segment_parts[2]
          
          # Mettre à jour le statut pour ces juges dans ce segment
          raw_data_db <- raw_data_db %>%
            mutate(judge_status = case_when(
              cj %in% removed_judges & 
                attribute_name == attr_name & 
                nom_fonction == nom_fonction ~ "removed",
              TRUE ~ judge_status
            ))
        }
      }
      
      nb_removed <- sum(raw_data_db$judge_status == "removed")
      message("Statut mis à jour pour ", nb_removed, " lignes (juges retirés)")
    }
    
    # Sélectionner les colonnes finales
    raw_data_db <- raw_data_db %>%
      select(source_name, trial_name, cj, product_name, attribute_name, 
             nom_fonction, value, judge_status)
    
    # Supprimer les données existantes pour ce fichier source
    dbExecute(con, "DELETE FROM rawdata WHERE source_name = $1", params = list(source_name))
    
    # Insérer les nouvelles données
    dbWriteTable(con, "rawdata", raw_data_db, append = TRUE, row.names = FALSE)
    
    nb_conserved <- sum(raw_data_db$judge_status == "conserved")
    nb_removed <- sum(raw_data_db$judge_status == "removed")
    
    message("Données brutes sauvegardées pour : ", source_name, 
            " (", nrow(raw_data_db), " lignes total, ",
            nb_conserved, " conservées, ", nb_removed, " retirées)")
    
    safe_disconnect(con)
    return(TRUE)
    
  }, error = function(e) {
    message("Erreur sauvegarde données brutes avec statut : ", e$message)
    safe_disconnect(con)
    return(FALSE)
  })
}


# ===== FONCTION DE SAUVEGARDE MODIFIÉE =====
save_results_to_db <- function(results_data, source_name, test_type) {
  con <- create_db_connection(DATABASES$RESULTS)
  if(is.null(con)) return(FALSE)
  
  tryCatch({
    # Créer la table appropriée selon le type de test
    create_appropriate_results_table(con, test_type)
    
    # Déterminer le nom de la table
    table_name <- get_results_table_name(test_type)
    
    # Adapter les données selon le type de test
    if(test_type == "Triangular") {
      results_db <- results_data %>%
        mutate(
          source_name = source_name,
          test_type = test_type,
          idtest = IDTEST,
          reference = REFERENCE,
          candidate = CANDIDATE,
          n_total = N,
          n_correct = CORRECT,
          p_value = P_VALUE,
          decision = DECISION
        ) %>%
        select(source_name, idtest, test_type, reference, candidate, 
               n_total, n_correct, p_value, decision)
      
    } else if(test_type == "Proximity") {
      # Tests de proximité (sans colonnes ANOVA)
      results_db <- results_data %>%
        mutate(
          source_name = source_name,
          test_type = test_type,
          idtest = IDTEST,
          segment = SEGMENT,
          segment_id = IDSEGMENT,
          product_name = PRODUCT,
          classe = CLASSE,
          mean_value = MEAN,
          sd_value = SD,
          n_observations = N
        ) %>%
        select(source_name, idtest, test_type, segment, segment_id, product_name,
               classe, mean_value, sd_value, n_observations)
      
    } else {
      # Tests standard (Strength, Strength with Malodour) - avec colonnes ANOVA
      results_db <- results_data %>%
        mutate(
          source_name = source_name,
          test_type = test_type,
          idtest = IDTEST,
          segment = SEGMENT,
          segment_id = IDSEGMENT,
          product_name = PRODUCT,
          classe = CLASSE,
          mean_value = MEAN,
          sd_value = SD,
          n_observations = N,
          anova_5pct = ifelse("ANOVA à 5%" %in% names(.), 
                              ifelse(`ANOVA à 5%` == "true", TRUE, FALSE), FALSE),
          anova_10pct = ifelse("ANOVA à 10%" %in% names(.), 
                               ifelse(`ANOVA à 10%` == "true", TRUE, FALSE), FALSE)
        ) %>%
        select(source_name, idtest, test_type, segment, segment_id, product_name,
               classe, mean_value, sd_value, n_observations, anova_5pct, anova_10pct)
    }
    
    # Supprimer les résultats existants pour ce fichier dans la table appropriée
    dbExecute(con, paste0("DELETE FROM ", table_name, " WHERE source_name = $1"), 
              params = list(source_name))
    
    # Insérer les nouveaux résultats dans la table appropriée
    dbWriteTable(con, table_name, results_db, append = TRUE, row.names = FALSE)
    
    message("Résultats sauvegardés (", test_type, ") dans ", table_name, " pour : ", 
            source_name, " (", nrow(results_db), " lignes)")
    safe_disconnect(con)
    return(TRUE)
    
  }, error = function(e) {
    message("Erreur sauvegarde résultats dans ", table_name, " : ", e$message)
    safe_disconnect(con)
    return(FALSE)
  })
}

save_judges_to_db <- function(judge_data, source_name = "GLOBAL") {
  con <- create_db_connection(DATABASES$JUDGES)
  if(is.null(con)) return(FALSE)
  
  tryCatch({
    create_judges_table(con)
    
    if(nrow(judge_data) == 0) {
      message("Aucune donnée de juge à sauvegarder")
      safe_disconnect(con)
      return(TRUE)
    }
    
    if(!"CJ" %in% names(judge_data)) {
      message("Colonne CJ manquante dans les données de juges")
      safe_disconnect(con)
      return(FALSE)
    }
    
    # ✅ PRÉPARATION DONNÉES AVEC VALIDATION RENFORCÉE
    judges_db <- judge_data %>%
      filter(!is.na(CJ) & CJ != "" & !is.null(CJ) & CJ != "NULL") %>%
      mutate(
        cj = as.character(CJ),
        nb_fichiers_participes = coalesce(nb_fichiers_participes, 1),
        nb_evaluations_total = coalesce(nb_evaluations_total, 0),
        moyenne_score_globale = coalesce(moyenne_score_globale, 0),
        attributes_evalues_total = coalesce(attributes_evalues_total, 0),
        produits_evalues_total = coalesce(produits_evalues_total, 0),
        nb_segments_total = coalesce(nb_segments_total, 0),
        nb_segments_retire_total = coalesce(nb_segments_retire_total, 0),
        nb_segments_conserve = coalesce(nb_segments_conserve, nb_segments_total),
        taux_conservation_global = coalesce(taux_conservation_global, 1.000),
        nb_fichiers_avec_retrait = coalesce(nb_fichiers_avec_retrait, 0),
        premier_fichier = coalesce(premier_fichier, ""),
        dernier_fichier = coalesce(dernier_fichier, ""),
        date_analyse = Sys.Date()
      ) %>%
      filter(!is.na(cj) & cj != "") %>%
      select(cj, nb_fichiers_participes, nb_evaluations_total, moyenne_score_globale,
             attributes_evalues_total, produits_evalues_total, nb_segments_total,
             nb_segments_retire_total, nb_segments_conserve, taux_conservation_global,
             nb_fichiers_avec_retrait, premier_fichier, dernier_fichier, date_analyse)
    
    if(nrow(judges_db) == 0) {
      message("Aucune donnée de juge valide après nettoyage")
      safe_disconnect(con)
      return(TRUE)
    }
    
    # ✅ AFFICHAGE DE CONTRÔLE AVANT SAUVEGARDE
    message("📊 Contrôle avant sauvegarde :")
    message("   • Nombre de juges : ", nrow(judges_db))
    message("   • Plage évaluations : ", min(judges_db$nb_evaluations_total), " - ", max(judges_db$nb_evaluations_total))
    message("   • Plage moyennes : ", round(min(judges_db$moyenne_score_globale, na.rm = TRUE), 2), " - ", round(max(judges_db$moyenne_score_globale, na.rm = TRUE), 2))
    
    # ✅ SUPPRESSION/INSERTION GLOBALE
    dbExecute(con, "DELETE FROM judge_tracking")
    dbWriteTable(con, "judge_tracking", judges_db, append = TRUE, row.names = FALSE)
    
    message("✅ Tracking juges sauvegardé : ", nrow(judges_db), " juges uniques")
    safe_disconnect(con)
    return(TRUE)
    
  }, error = function(e) {
    message("❌ Erreur sauvegarde tracking juges : ", e$message)
    safe_disconnect(con)
    return(FALSE)
  })
}





# ===== CRÉATION DES 2 TABLES POUR L'APPLICATION SHINY =====

# ===== CORRECTION DES NOMS DE TABLES METADATA =====
create_product_info_table <- function(con) {
  if(is.null(con)) return(FALSE)
  
  tryCatch({
    if(!dbExistsTable(con, "product_info")) {  # Minuscules
      dbExecute(con, "CREATE TABLE IF NOT EXISTS product_info (
        id SERIAL PRIMARY KEY,
        source_name VARCHAR(255) NOT NULL,
        product_name VARCHAR(255) NOT NULL,
        idtest VARCHAR(255),
        code_prod VARCHAR(255),
        base VARCHAR(255),
        ref VARCHAR(10),
        dosage VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )")
      
      dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_product_info_source ON product_info(source_name)")
      dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_product_info_product ON product_info(product_name)")
      dbExecute(con, "CREATE UNIQUE INDEX IF NOT EXISTS idx_product_info_unique ON product_info(source_name, product_name)")
      
      message("Table product_info créée avec succès dans SA_METADATA")
    }
    return(TRUE)
  }, error = function(e) {
    message("Erreur création table product_info : ", e$message)
    return(FALSE)
  })
}

create_test_info_table <- function(con) {
  if(is.null(con)) return(FALSE)
  
  tryCatch({
    if(!dbExistsTable(con, "test_info")) {  # Minuscules
      dbExecute(con, "CREATE TABLE IF NOT EXISTS test_info (
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
      )")
      
      dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_test_info_source ON test_info(source_name)")
      dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_test_info_test_name ON test_info(test_name)")
      dbExecute(con, "CREATE UNIQUE INDEX IF NOT EXISTS idx_test_info_unique ON test_info(source_name, test_name)")
      
      message("Table test_info créée avec succès dans SA_METADATA")
    }
    return(TRUE)
  }, error = function(e) {
    message("Erreur création table test_info : ", e$message)
    return(FALSE)
  })
}


# ===== FONCTIONS DE SAUVEGARDE AVEC ENREGISTREMENTS VIDES =====

# Sauvegarder Product_Info avec tous les champs (vides à remplir par l'app)
# ===== FONCTIONS DE SAUVEGARDE CORRIGÉES =====
save_product_info_complete <- function(raw_data, source_name) {
  con <- create_db_connection(DATABASES$METADATA)
  if(is.null(con)) return(FALSE)
  
  tryCatch({
    create_product_info_table(con)
    
    # Extraire les couples uniques (SOURCE_NAME, ProductName)
    product_info <- raw_data %>%
      select(TrialName, ProductName) %>%
      distinct() %>%
      mutate(
        source_name = source_name,
        product_name = ProductName,
        idtest = TrialName,
        code_prod = "",
        base = "",
        ref = "",
        dosage = ""
      ) %>%
      select(source_name, product_name, idtest, code_prod, base, ref, dosage)
    
    # Supprimer les entrées existantes pour ce source_name
    dbExecute(con, "DELETE FROM product_info WHERE source_name = $1", params = list(source_name))
    
    # Insérer les nouvelles données
    dbWriteTable(con, "product_info", product_info, append = TRUE, row.names = FALSE)
    
    message("Product_Info complet sauvegardé : ", source_name, " (", nrow(product_info), " produits)")
    safe_disconnect(con)
    return(TRUE)
    
  }, error = function(e) {
    message("Erreur sauvegarde product_info : ", e$message)
    safe_disconnect(con)
    return(FALSE)
  })
}

save_test_info_complete <- function(raw_data, source_name) {
  con <- create_db_connection(DATABASES$METADATA)
  if(is.null(con)) return(FALSE)
  
  tryCatch({
    create_test_info_table(con)
    
    # Extraire les test_names uniques
    test_info <- raw_data %>%
      select(TrialName) %>%
      distinct() %>%
      mutate(
        source_name = source_name,
        test_name = TrialName,
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
        scale=""
      ) %>%
      select(-TrialName)
    
    # Vérifier si l'enregistrement existe déjà
    for(i in 1:nrow(test_info)) {
      test_record <- test_info[i, ]
      
      existing <- dbGetQuery(con, 
                             "SELECT COUNT(*) as count FROM test_info WHERE source_name = $1 AND test_name = $2",
                             params = list(test_record$source_name, test_record$test_name))
      
      if(existing$count == 0) {
        # Insérer l'enregistrement vide
        dbWriteTable(con, "test_info", test_record, append = TRUE, row.names = FALSE)
      }
    }
    
    message("Test_Info complet sauvegardé : ", source_name, " (", nrow(test_info), " tests)")
    safe_disconnect(con)
    return(TRUE)
    
  }, error = function(e) {
    message("Erreur sauvegarde test_info : ", e$message)
    safe_disconnect(con)
    return(FALSE)
  })
}




# ===== INITIALISATION =====
# ===== INITIALISATION =====
message("Début analyse avec intégration multi-databases: ", Sys.time())

# ✅ DOSSIERS SPÉCIFIQUES À TRAITER
target_dirs <- c(
  "//emea/dfs/Fizzdata/CRP/Fizz_Manon",
  "//emea/dfs/Fizzdata/CRP/Fizz_Cecile",
  "//emea/dfs/Fizzdata/CRP/Fizz_Alizee"
)

output_base_dir <- "C:/ResultatsAnalyseSenso"

if(!dir.exists(output_base_dir)) {
  dir.create(output_base_dir, recursive = TRUE, showWarnings = FALSE)
  message("Création du dossier de sortie principal: ", output_base_dir)
}

# ✅ VÉRIFICATION DE L'EXISTENCE DES DOSSIERS CIBLES
existing_dirs <- target_dirs[dir.exists(target_dirs)]
missing_dirs <- target_dirs[!dir.exists(target_dirs)]

if(length(missing_dirs) > 0) {
  message("⚠️ Dossiers manquants :")
  for(dir in missing_dirs) {
    message("   - ", dir)
  }
}

if(length(existing_dirs) == 0) {
  stop("❌ ERREUR : Aucun des dossiers cibles n'existe !")
}

message("✅ Dossiers cibles trouvés :")
for(dir in existing_dirs) {
  message("   - ", dir)
}


# ===== SYSTÈME DE TRACKING =====
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

# ===== FONCTIONS UTILITAIRES ET D'ANALYSE =====

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

create_judge_tracking_table <- function(all_judge_info, all_raw_data) {
  tryCatch({
    if(nrow(all_raw_data) == 0) {
      message("Aucune donnée brute disponible pour le tracking des juges")
      return(tibble(Message = "Aucune donnée disponible", Timestamp = Sys.time()))
    }
    
    # ✅ CORRECTION : Filtrer les valeurs CJ NULL/vides dès le début
    all_raw_data_clean <- all_raw_data %>%
      filter(!is.na(CJ) & CJ != "" & !is.null(CJ) & CJ != "NULL") %>%
      filter(!is.na(Value) & is.numeric(Value))  # ✅ AJOUT : Filtrer les valeurs non-numériques
    
    if(nrow(all_raw_data_clean) == 0) {
      message("Aucune donnée avec CJ valide pour le tracking des juges")
      return(tibble(Message = "Aucun juge valide trouvé", Timestamp = Sys.time()))
    }
    
    message("✅ Données nettoyées : ", nrow(all_raw_data_clean), " lignes valides pour ", 
            n_distinct(all_raw_data_clean$CJ), " juges uniques")
    
    # ✅ CORRECTION : AGRÉGATION GLOBALE PAR JUGE (calculs corrects)
    judge_participation <- all_raw_data_clean %>%
      group_by(CJ) %>%
      summarise(
        nb_fichiers_participes = n_distinct(SourceFile, na.rm = TRUE),
        nb_evaluations_total = n(),  # ✅ Total réel par juge
        moyenne_score_globale = round(mean(Value, na.rm = TRUE), 3),  # ✅ Moyenne réelle par juge
        attributes_evalues_total = n_distinct(AttributeName, na.rm = TRUE),
        produits_evalues_total = n_distinct(ProductName, na.rm = TRUE),
        premier_fichier = min(SourceFile, na.rm = TRUE),
        dernier_fichier = max(SourceFile, na.rm = TRUE),
        .groups = 'drop'
      ) %>%
      filter(!is.na(CJ) & CJ != "") %>%
      mutate(date_analyse = Sys.Date())
    
    # ✅ CORRECTION : CALCUL CORRECT DES SEGMENTS PAR JUGE
    segments_per_judge <- all_raw_data_clean %>%
      group_by(CJ) %>%
      summarise(
        nb_segments_total = n_distinct(paste(SourceFile, AttributeName, NomFonction, sep = "_SEPARATOR_"), na.rm = TRUE),
        .groups = 'drop'
      ) %>%
      filter(!is.na(CJ) & CJ != "")
    
    # ✅ CORRECTION : CALCUL CORRECT DES RETRAITS
    if(nrow(all_judge_info) > 0 && "RemovedJudges" %in% names(all_judge_info)) {
      # Traitement plus robuste des juges retirés
      judge_removal_global <- all_judge_info %>%
        filter(!is.na(RemovedJudges) & RemovedJudges != "") %>%
        separate_rows(RemovedJudges, sep = ", ") %>%
        filter(RemovedJudges != "" & !is.na(RemovedJudges)) %>%
        rename(CJ = RemovedJudges) %>%
        filter(!is.na(CJ) & CJ != "") %>%
        group_by(CJ) %>%
        summarise(
          nb_segments_retire_total = n(),
          nb_fichiers_avec_retrait = n_distinct(File, na.rm = TRUE),
          .groups = 'drop'
        )
      
      # ✅ JOINTURE ET CALCULS CORRECTS
      segments_conservation <- segments_per_judge %>%
        left_join(judge_removal_global, by = "CJ") %>%
        mutate(
          nb_segments_retire_total = coalesce(nb_segments_retire_total, 0),
          nb_segments_conserve = pmax(0, nb_segments_total - nb_segments_retire_total),  # ✅ Éviter les valeurs négatives
          taux_conservation_global = round(
            ifelse(nb_segments_total > 0, nb_segments_conserve / nb_segments_total, 1), 
            3
          ),
          nb_fichiers_avec_retrait = coalesce(nb_fichiers_avec_retrait, 0)
        )
    } else {
      segments_conservation <- segments_per_judge %>%
        mutate(
          nb_segments_retire_total = 0,
          nb_segments_conserve = nb_segments_total,
          taux_conservation_global = 1.000,
          nb_fichiers_avec_retrait = 0
        )
    }
    
    # ✅ JOINTURE FINALE AVEC VÉRIFICATIONS
    judge_tracking <- judge_participation %>%
      left_join(segments_conservation, by = "CJ") %>%
      filter(!is.na(CJ) & CJ != "") %>%
      mutate(
        # ✅ Vérifications de cohérence
        nb_segments_total = coalesce(nb_segments_total, 0),
        nb_segments_retire_total = coalesce(nb_segments_retire_total, 0),
        nb_segments_conserve = coalesce(nb_segments_conserve, nb_segments_total),
        taux_conservation_global = coalesce(taux_conservation_global, 1.000),
        nb_fichiers_avec_retrait = coalesce(nb_fichiers_avec_retrait, 0)
      )
    
    if(nrow(judge_tracking) == 0) {
      message("Aucune donnée de tracking valide après nettoyage")
      return(tibble(Message = "Aucune donnée de tracking valide", Timestamp = Sys.time()))
    }
    
    # ✅ AFFICHAGE DE STATISTIQUES DE CONTRÔLE
    message("✅ Table de tracking créée avec ", nrow(judge_tracking), " juges uniques")
    message("   • Moyenne des évaluations par juge : ", round(mean(judge_tracking$nb_evaluations_total), 1))
    message("   • Moyenne des scores : ", round(mean(judge_tracking$moyenne_score_globale, na.rm = TRUE), 2))
    message("   • Taux de conservation moyen : ", round(mean(judge_tracking$taux_conservation_global, na.rm = TRUE), 3))
    
    return(judge_tracking)
    
  }, error = function(e) {
    message("❌ Erreur création table tracking juges: ", e$message)
    return(tibble(Erreur = paste("Échec création table tracking:", e$message), Timestamp = Sys.time()))
  })
}





# ===== FONCTION GESTION DES TESTS DE PROXIMITÉ =====
handle_proximity_test <- function(segment) {
  tryCatch({
    # Vérification initiale des données
    if(is.null(segment) || nrow(segment) == 0) {
      message("Segment vide pour test proximité")
      return(list(
        segment = segment,
        removed_judges = character(0),
        n_initial = 0,
        n_final = 0
      ))
    }
    
    # Validation des colonnes nécessaires
    if(!"ProductName" %in% names(segment) || !"Value" %in% names(segment) || !"CJ" %in% names(segment)) {
      message("Colonnes manquantes pour test proximité")
      return(list(
        segment = segment,
        removed_judges = character(0),
        n_initial = n_distinct(segment$CJ),
        n_final = n_distinct(segment$CJ)
      ))
    }
    
    # Nettoyer les données
    segment <- segment %>%
      filter(!is.na(ProductName) & !is.na(Value) & !is.na(CJ) & 
               ProductName != "" & CJ != "")
    
    if(nrow(segment) == 0) {
      message("Aucune donnée valide après nettoyage pour test proximité")
      return(list(
        segment = segment,
        removed_judges = character(0),
        n_initial = 0,
        n_final = 0
      ))
    }
    
    n_judges_initial <- n_distinct(segment$CJ)
    
    # Identifier le produit de référence (celui avec la moyenne la plus faible)
    bench_product <- segment %>%
      group_by(ProductName) %>%
      summarise(avg = mean(Value, na.rm = TRUE), .groups = 'drop') %>%
      filter(!is.na(avg)) %>%
      slice_min(avg, n = 1, with_ties = FALSE) %>%
      pull(ProductName)
    
    if(length(bench_product) == 0) {
      message("Impossible de déterminer le produit de référence")
      return(list(
        segment = segment,
        removed_judges = character(0),
        n_initial = n_judges_initial,
        n_final = n_judges_initial
      ))
    }
    
    message("Produit de référence identifié: ", bench_product)
    
    # Identifier les juges à filtrer avec gestion d'erreur robuste
    filtered_judges <- segment %>%
      group_by(CJ) %>%
      summarise(
        # Vérifier si le juge a évalué le produit de référence
        has_bench_score = any(ProductName %in% bench_product),
        # Calculer le score du produit de référence (avec valeur par défaut)
        bench_score = ifelse(
          any(ProductName %in% bench_product),
          Value[ProductName %in% bench_product][1],  # Prendre la première valeur si plusieurs
          NA
        ),
        # Vérifier les conditions de filtrage
        bench_too_high = ifelse(
          !is.na(bench_score),
          bench_score > 4,
          FALSE
        ),
        # Vérifier si d'autres produits ont des scores <= bench_score - 1
        other_products_low = ifelse(
          !is.na(bench_score) & any(!ProductName %in% bench_product),
          any(Value[!ProductName %in% bench_product] <= (bench_score - 1), na.rm = TRUE),
          FALSE
        ),
        .groups = 'drop'
      ) %>%
      # Filtrer les juges selon les critères
      filter(
        !has_bench_score |  # Juge n'a pas évalué le produit de référence
          bench_too_high |    # Score de référence > 4
          other_products_low  # Autres produits avec score <= bench_score - 1
      ) %>%
      pull(CJ)
    
    # Appliquer le filtrage
    filtered_segment <- segment %>%
      filter(!CJ %in% filtered_judges)
    
    n_judges_final <- n_distinct(filtered_segment$CJ)
    
    message("Juges filtrés pour proximité: ", length(filtered_judges), 
            " | Juges restants: ", n_judges_final)
    
    if(length(filtered_judges) > 0) {
      message("Juges retirés: ", paste(filtered_judges, collapse = ", "))
    }
    
    return(list(
      segment = filtered_segment,
      removed_judges = filtered_judges,
      n_initial = n_judges_initial,
      n_final = n_judges_final
    ))
    
  }, error = function(e) {
    message("Erreur dans handle_proximity_test: ", e$message)
    return(list(
      segment = segment,
      removed_judges = character(0),
      n_initial = n_distinct(segment$CJ),
      n_final = n_distinct(segment$CJ)
    ))
  })
}


# ===== FONCTION CORRIGÉE POUR LES SEGMENTS TRIANGULAIRES =====
process_triangular_segments <- function(segments, file_path, file_test_type) {
  # Vérification robuste avec gestion des erreurs
  triangular_indices <- c()
  
  for(i in seq_along(segments)) {
    seg <- segments[[i]]
    
    # Vérifications de sécurité
    if(is.null(seg) || length(seg) == 0 || nrow(seg) == 0) {
      next
    }
    
    if(!"NomFonction" %in% names(seg)) {
      next
    }
    
    nom_fonction <- seg$NomFonction[1]
    if(is.na(nom_fonction) || is.null(nom_fonction)) {
      next
    }
    
    # Test de détection triangulaire
    if(str_detect(nom_fonction, "Triangulaire|triangle")) {
      triangular_indices <- c(triangular_indices, i)
    }
  }
  
  if(length(triangular_indices) == 0) {
    return(NULL)
  }
  
  all_triangular_data <- bind_rows(segments[triangular_indices])
  message("Fusion de ", length(triangular_indices), " segments triangulaires en un seul")
  
  # Calcul du test triangulaire
  n_total <- sum(!is.na(all_triangular_data$Value))
  n_correct <- sum(all_triangular_data$Value == 1, na.rm = TRUE)
  
  # Test binomial
  test_result <- binom.test(n_correct, n_total, p = 1/3, alternative = "greater")
  p_value <- test_result$p.value
  decision <- ifelse(p_value < 0.05, "Significant", "Not Significant")
  
  # Déterminer référence et candidat
  available_products <- unique(all_triangular_data$ProductName)
  ref_product <- ifelse(length(available_products) > 0, available_products[1], "Unknown")
  candidat_product <- ifelse(length(available_products) > 1, available_products[2], "Unknown")
  
  result <- tibble(
    IDTEST = tools::file_path_sans_ext(basename(file_path)),
    REFERENCE = ref_product,
    CANDIDATE = candidat_product,
    N = n_total,
    CORRECT = n_correct,
    P_VALUE = round(p_value, 4),
    DECISION = decision,
    TESTTYPE = file_test_type
  )
  
  return(result)
}




# ===== FONCTION D'ANALYSE ITÉRATIVE DES JUGES (SUITE) =====
analyze_judges_iterative <- function(segment) {
  # Vérification initiale des données
  if (is.null(segment) || nrow(segment) == 0) {
    message("Segment vide ou NULL - Arrêt du traitement")
    return(list(
      segment = segment,
      removed_judges = character(0),
      n_initial = 0,
      n_final = 0
    ))
  }
  
  # Traitement spécial pour les tests MO (odeur corporelle)
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
  
  # Validation des données d'entrée
  if (!"CJ" %in% names(segment) || !"Value" %in% names(segment)) {
    message("Colonnes CJ ou Value manquantes - Arrêt du traitement")
    return(list(
      segment = segment,
      removed_judges = character(0),
      n_initial = n_distinct(segment$CJ),
      n_final = n_distinct(segment$CJ)
    ))
  }
  
  # Nettoyage des données CJ (éliminer les valeurs vides/NULL)
  segment <- segment %>%
    filter(!is.na(CJ) & CJ != "" & !is.na(Value))
  
  if (nrow(segment) == 0) {
    message("Aucune donnée valide après nettoyage - Arrêt du traitement")
    return(list(
      segment = segment,
      removed_judges = character(0),
      n_initial = 0,
      n_final = 0
    ))
  }
  
  n_judges_initial <- n_distinct(segment$CJ)
  removed_judges_total <- c()
  current_data <- segment
  
  # Protection contre les boucles infinies
  max_iterations <- 20
  iteration_count <- 0
  
  repeat {
    iteration_count <- iteration_count + 1
    
    # Protection contre les boucles infinies
    if (iteration_count > max_iterations) {
      message("Limite maximale d'itérations atteinte (", max_iterations, ") - Arrêt forcé du filtrage")
      break
    }
    
    n_judges_current <- n_distinct(current_data$CJ)
    
    # Vérification du seuil minimal de juges
    if (n_judges_current <= 8) {
      message("Seuil minimal atteint (8 juges) - Arrêt du filtrage")
      break
    }
    
    # Calcul de l'ANOVA avec gestion d'erreur
    model <- tryCatch({
      aov(Value ~ CJ, data = current_data)
    }, error = function(e) {
      message("Erreur lors du calcul ANOVA: ", e$message)
      return(NULL)
    })
    
    if (is.null(model)) {
      message("Impossible de calculer l'ANOVA - Arrêt du filtrage")
      break
    }
    
    anova_res <- tryCatch({
      anova(model)
    }, error = function(e) {
      message("Erreur lors de l'extraction des résultats ANOVA: ", e$message)
      return(NULL)
    })
    
    if (is.null(anova_res)) {
      message("Impossible d'extraire les résultats ANOVA - Arrêt du filtrage")
      break
    }
    
    p_value <- anova_res["CJ", "Pr(>F)"]
    
    if (is.na(p_value) || is.null(p_value)) {
      message("Problème de calcul ANOVA (p-value NA/NULL) - Arrêt de l'itération pour ce segment.")
      break
    }
    
    # Test de significativité
    if (p_value >= 0.05) {
      message("Effet juge non significatif (p=", round(p_value, 4), ") - Arrêt du filtrage")
      break
    }
    
    # Vérification du seuil de conservation (2/3 des juges initiaux)
    if (n_judges_current <= (2/3) * n_judges_initial) {
      message("Seuil de conservation atteint (2/3 des juges initiaux) - Arrêt du filtrage")
      break
    }
    
    # Calcul des statistiques des juges
    judge_stats <- current_data %>%
      group_by(CJ) %>%
      summarise(MeanScore = mean(Value, na.rm = TRUE), .groups = 'drop') %>%
      filter(!is.na(MeanScore)) %>%  # Éliminer les moyennes NA
      mutate(
        OverallMean = mean(MeanScore, na.rm = TRUE),
        AbsDeviation = abs(MeanScore - OverallMean)
      ) %>%
      filter(!is.na(AbsDeviation))  # Éliminer les déviations NA
    
    if (nrow(judge_stats) == 0) {
      message("Aucune statistique de juge calculable - Arrêt du filtrage")
      break
    }
    
    # Sélection du juge à retirer (celui avec la plus grande déviation)
    judge_to_remove <- judge_stats %>%
      slice_max(AbsDeviation, n = 1, with_ties = FALSE) %>%
      pull(CJ)
    
    # Vérification que le juge à retirer n'est pas vide
    if (length(judge_to_remove) == 0 || is.na(judge_to_remove) || judge_to_remove == "") {
      message("Impossible de déterminer le juge à retirer - Arrêt du filtrage")
      break
    }
    
    # Protection contre la suppression répétée du même juge
    if (judge_to_remove %in% removed_judges_total) {
      message("Juge déjà retiré précédemment (", judge_to_remove, ") - Arrêt pour éviter une boucle infinie")
      break
    }
    
    # Vérification que le juge existe encore dans les données
    if (!judge_to_remove %in% current_data$CJ) {
      message("Juge à retirer non trouvé dans les données actuelles - Arrêt du filtrage")
      break
    }
    
    # Ajout à la liste des juges retirés
    removed_judges_total <- c(removed_judges_total, judge_to_remove)
    
    # Suppression du juge des données
    current_data <- current_data %>% 
      filter(CJ != judge_to_remove)
    
    # Vérification que des données restent après suppression
    if (nrow(current_data) == 0) {
      message("Plus de données après suppression du juge - Arrêt du filtrage")
      # Restaurer les données précédentes
      current_data <- segment %>% 
        filter(!CJ %in% removed_judges_total[-length(removed_judges_total)])
      removed_judges_total <- removed_judges_total[-length(removed_judges_total)]
      break
    }
    
    message("Juge retiré: ", judge_to_remove,
            " | Déviation: ", round(max(judge_stats$AbsDeviation, na.rm = TRUE), 2),
            " | Nouveau n juges: ", n_judges_current - 1,
            " | Itération: ", iteration_count)
  }
  
  # Validation finale
  final_n_judges <- n_distinct(current_data$CJ)
  
  return(list(
    segment = current_data,
    removed_judges = unique(removed_judges_total),
    n_initial = n_judges_initial,
    n_final = final_n_judges
  ))
}


# ===== FONCTION D'ANALYSE DES PRODUITS (MODIFIÉE POUR NOUVEAUX FORMATS) =====
analyze_products <- function(segment, segment_index, file_path = NULL, file_test_type) {
  tryCatch({
    seg_data <- segment
    
    # Traitement spécial pour les tests triangulaires
    if(file_test_type == "Triangular") {
      message("Détection test triangulaire pour segment: ", segment$NomFonction[1])
      
      # Calcul des paramètres du test triangulaire
      n_total <- sum(!is.na(segment$Value))
      n_correct <- sum(segment$Value == 1, na.rm = TRUE)
      
      # Test binomial avec p = 1/3
      test_result <- binom.test(n_correct, n_total, p = 1/3, alternative = "greater")
      p_value <- test_result$p.value
      
      # Décision basée sur p-value < 0.05
      decision <- ifelse(p_value < 0.05, "Significant", "Not Significant")
      
      # Déterminer référence et candidat
      available_products <- unique(segment$ProductName)
      ref_product <- ifelse(length(available_products) > 0, available_products[1], "Unknown")
      candidat_product <- ifelse(length(available_products) > 1, available_products[2], "Unknown")
      
      result <- tibble(
        IDTEST = tools::file_path_sans_ext(basename(file_path)),
        REFERENCE = ref_product,
        CANDIDATE = candidat_product,
        N = n_total,
        CORRECT = n_correct,
        P_VALUE = round(p_value, 4),
        DECISION = decision,
        TESTTYPE = file_test_type
      )
      
      return(result)
    }
    
    # Pour tous les autres types de tests (Strength, Strength with Malodour, Proximity)
    if (n_distinct(seg_data$ProductName) < 2) {
      message("Analyse impossible : moins de 2 produits dans le segment")
      return(NULL)
    }
    
    # Calcul des statistiques par produit
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
    
    # ANOVA pour tester l'effet produit
    model <- aov(Value ~ ProductName, data = seg_data)
    anova_result <- anova(model)
    p_value_produit <- anova_result["ProductName", "Pr(>F)"]
    
    # Tests de significativité à 5% et 10%
    anova_5pct <- ifelse(!is.na(p_value_produit) && p_value_produit < 0.05, "true", "false")
    anova_10pct <- ifelse(!is.na(p_value_produit) && p_value_produit < 0.10, "true", "false")
    
    # Test post-hoc pour les groupes (uniquement si significatif à 10%)
    if(anova_10pct) {
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
    } else {
      # Si pas significatif, tous les produits dans le même groupe
      result_df <- stats_df %>%
        mutate(
          ProductName = ProductName,
          Classe = "a"  # Tous dans le même groupe
        )
    }
    
    # Créer l'output selon le type de test
    if(file_test_type %in% c("Strength", "Strength with Malodour")) {
      # STRENGTH et STRENGTH WITH MALODOUR : IDTEST, SEGMENT, IDSEGMENT, PRODUCT, CLASSE, MEAN, SD, N, TESTTYPE, ANOVA à 5%, ANOVA à 10%
      result_df <- result_df %>%
        mutate(
          IDTEST = tools::file_path_sans_ext(basename(file_path)),
          SEGMENT = paste(segment$AttributeName[1], segment$NomFonction[1], sep = " - "),
          IDSEGMENT = segment_index,
          PRODUCT = ProductName,
          CLASSE = Classe,
          MEAN = Mean,
          SD = Sd,
          N = n,
          TESTTYPE = file_test_type,
          `ANOVA à 5%` = anova_5pct,
          `ANOVA à 10%` = anova_10pct
        ) %>%
        select(IDTEST, SEGMENT, IDSEGMENT, PRODUCT, CLASSE, MEAN, SD, N, TESTTYPE, `ANOVA à 5%`, `ANOVA à 10%`)
      
    } else if(file_test_type == "Proximity") {
      # PROXIMITY : IDTEST, SEGMENT, IDSEGMENT, PRODUCT, CLASSE, MEAN, SD, N, TESTTYPE
      result_df <- result_df %>%
        mutate(
          IDTEST = tools::file_path_sans_ext(basename(file_path)),
          SEGMENT = paste(segment$AttributeName[1], segment$NomFonction[1], sep = " - "),
          IDSEGMENT = segment_index,
          PRODUCT = ProductName,
          CLASSE = Classe,
          MEAN = Mean,
          SD = Sd,
          N = n,
          TESTTYPE = file_test_type
        ) %>%
        select(IDTEST, SEGMENT, IDSEGMENT, PRODUCT, CLASSE, MEAN, SD, N, TESTTYPE)
    }
    
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

# ===== PROGRAMME PRINCIPAL MODIFIÉ =====
tracking_data <- load_tracking_data()

# ✅ COLLECTER LES FICHIERS EXCEL DEPUIS LES DOSSIERS SPÉCIFIQUES
excel_files <- c()
for(target_dir in existing_dirs) {
  message("🔍 Scan du dossier : ", target_dir)
  
  files_in_dir <- tryCatch({
    dir_ls(target_dir, regexp = "\\.xlsx$", ignore.case = TRUE, recurse = TRUE) %>%
      as.character()
  }, error = function(e) {
    message("⚠️ Erreur scan ", target_dir, " : ", e$message)
    return(character(0))
  })
  
  if(length(files_in_dir) > 0) {
    excel_files <- c(excel_files, files_in_dir)
    message("   → ", length(files_in_dir), " fichiers Excel trouvés")
  } else {
    message("   → Aucun fichier Excel trouvé")
  }
}

message("📊 TOTAL fichiers Excel détectés: ", length(excel_files))
message("   • Fizz_Manon : ", sum(str_detect(excel_files, "Fizz_Manon")))
message("   • Fizz_Cecile : ", sum(str_detect(excel_files, "Fizz_Cecile")))


all_results <- list()
judge_removal_info <- list()
all_raw_data <- list()
data_issues_log <- list()

files_processed <- 0
files_skipped <- 0
files_new <- 0
# ===== FONCTION DE DÉTERMINATION DU TYPE DE TEST (CORRIGÉE) =====
determine_test_type <- function(segments) {
  # Vérifier s'il y a des tests triangulaires
  triangular_count <- 0
  for(seg in segments) {
    if(!is.null(seg) && nrow(seg) > 0 && "NomFonction" %in% names(seg)) {
      nom_fonction <- seg$NomFonction[1]
      if(!is.na(nom_fonction) && str_detect(nom_fonction, "Triangulaire|triangle")) {
        triangular_count <- triangular_count + 1
      }
    }
  }
  
  if(triangular_count > 0) {
    return("Triangular")
  }
  
  # Vérifier s'il y a des tests de proximité (amélioration de la détection)
  proximity_count <- 0
  for(seg in segments) {
    if(!is.null(seg) && nrow(seg) > 0) {
      # Vérifier dans AttributeName
      if("AttributeName" %in% names(seg)) {
        attr_name <- seg$AttributeName[1]
        if(!is.na(attr_name) && str_detect(str_to_lower(attr_name), "prox")) {
          proximity_count <- proximity_count + 1
        }
      }
      
      # Vérifier aussi dans NomFonction
      if("NomFonction" %in% names(seg)) {
        nom_fonction <- seg$NomFonction[1]
        if(!is.na(nom_fonction) && str_detect(str_to_lower(nom_fonction), "prox")) {
          proximity_count <- proximity_count + 1
        }
      }
    }
  }
  
  if(proximity_count > 0) {
    return("Proximity")
  }
  
  # Vérifier s'il y a des tests MO (odeur corporelle)
  mo_count <- 0
  for(seg in segments) {
    if(!is.null(seg) && nrow(seg) > 0 && "AttributeName" %in% names(seg)) {
      attr_name <- seg$AttributeName[1]
      if(!is.na(attr_name) && str_detect(str_to_lower(attr_name), "odeur corporell")) {
        mo_count <- mo_count + 1
      }
    }
  }
  
  if(mo_count > 0) {
    return("Strength with Malodour")
  }
  
  # Par défaut, test de force (Strength)
  return("Strength")
}


# ===== BOUCLE PRINCIPALE COMPLÈTE MODIFIÉE =====
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
    
    # ===== DÉTERMINATION DU TYPE DE TEST POUR LE FICHIER =====
    file_test_type <- determine_test_type(segments)
    message("Type de test détecté pour le fichier: ", file_test_type)
    
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
    
    is_triangular <- file_test_type == "Triangular"
    
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
      
    } else if (file_test_type == "Proximity") {
      message("Application test PROXIMITE: ", seg_name)
      result <- handle_proximity_test(segments[[i]])
      
    } else if (file_test_type == "Strength with Malodour") {
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
  
  # ===== CORRECTION DANS LA BOUCLE PRINCIPALE =====
  # Analyse différentielle des produits avec le type de test du fichier
  triangular_results <- process_triangular_segments(segments_processed, file_path, file_test_type)
  
  # Correction : filtrer correctement les segments non-triangulaires
  non_triangular_segments <- list()
  for(i in seq_along(segments_processed)) {
    seg <- segments_processed[[i]]
    if(!is.null(seg) && nrow(seg) > 0) {
      # Si ce n'est pas un test triangulaire au niveau du fichier, inclure le segment
      if(file_test_type != "Triangular") {
        non_triangular_segments[[length(non_triangular_segments) + 1]] <- seg
      }
    }
  }
  
  standard_results <- map2(non_triangular_segments, seq_along(non_triangular_segments), 
                           ~analyze_products(.x, .y, file_path, file_test_type)) %>%
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
  
  
  # ===== SAUVEGARDE DANS LES BASES DE DONNÉES (ORDRE CORRIGÉ) =====
  
  # 1. Sauvegarder les résultats avec le type de test du fichier
  if(exists("file_results") && nrow(file_results) > 0) {
    if(save_results_to_db(file_results, source_name, file_test_type)) {
      message("✅ Résultats sauvegardés dans SA_RESULTS_DATA (", file_test_type, ")")
    }
  }
  
  
  # 3. Préparer les informations des juges retirés pour ce fichier
  file_judge_changes_for_db <- judge_removal_info %>% 
    keep(~ .x$File == basename(file_path)) %>%
    map_df(~ .x)
  
  # 4. Sauvegarder les données brutes AVEC le statut des juges (APRÈS traitement)
  if(save_raw_data_with_judge_status(file_data, source_name, file_judge_changes_for_db)) {
    message("✅ Données brutes sauvegardées dans SA_RAW_DATA avec statut juges")
  }
  
  # 5. Créer les enregistrements complets pour l'application Shiny
  if(save_product_info_complete(file_data, source_name)) {
    message("✅ Product_Info complet créé avec champs vides")
  }
  
  if(save_test_info_complete(file_data, source_name)) {
    message("✅ Test_Info complet créé avec champs vides")
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
      Type_Test_Detecte = file_test_type,
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
    message("📄 Fichier individuel généré: ", output_file_path)
    
  }, error = function(e) {
    message("❌ ERREUR génération fichier individuel: ", e$message)
    
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
      message("📄 Fichier d'erreur créé: ", output_file_path)
    }, error = function(e2) {
      message("❌ Impossible de créer même le fichier d'erreur: ", e2$message)
    })
  })
}

message("\n=== GÉNÉRATION DU TRACKING GLOBAL DES JUGES ===")

if(length(judge_removal_info) > 0 && length(all_raw_data) > 0) {
  tryCatch({
    # Consolidation de toutes les informations
    all_judge_info_df <- bind_rows(judge_removal_info)
    all_raw_data_df <- bind_rows(all_raw_data)
    
    message("Données consolidées : ", nrow(all_raw_data_df), " lignes brutes, ", 
            nrow(all_judge_info_df), " informations de retrait")
    
    # ✅ CRÉATION DE LA TABLE GLOBALE OPTIMISÉE
    global_judge_tracking <- create_judge_tracking_table(all_judge_info_df, all_raw_data_df)
    
    if(nrow(global_judge_tracking) > 0) {
      # ✅ SAUVEGARDE GLOBALE
      if(save_judges_to_db(global_judge_tracking, "GLOBAL")) {
        message("✅ Tracking global des juges sauvegardé dans SA_JUDGES")
        message("   → ", nrow(global_judge_tracking), " juges uniques trackés")
      }
    } else {
      message("⚠️ Aucune donnée de tracking global générée")
    }
    
  }, error = function(e) {
    message("❌ Erreur génération tracking global des juges : ", e$message)
  })
} else {
  message("⚠️ Pas assez de données pour générer le tracking global des juges")
  message("   Judge removal info : ", length(judge_removal_info), " entrées")
  message("   Raw data : ", length(all_raw_data), " fichiers")
}

# ===== GÉNÉRATION DU FICHIER CONSOLIDÉ =====
message("\n=== GÉNÉRATION DU FICHIER CONSOLIDÉ ===")

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
    
    # ===== GÉNÉRATION DU FICHIER CONSOLIDÉ GLOBAL (SUITE) =====
    # Statistiques par type de test
    test_stats <- all_results_df %>%
      group_by(TESTTYPE) %>%
      summarise(
        Nb_Tests = n(),
        Nb_Fichiers = n_distinct(Fichier_Source),
        .groups = 'drop'
      )
    
    consolidated_data$"Stats_Types_Tests" <- test_stats
    
    # Statistiques par produit (pour tests non-triangulaires)
    if("PRODUCT" %in% names(all_results_df)) {
      product_stats <- all_results_df %>%
        filter(!is.na(PRODUCT)) %>%
        group_by(PRODUCT) %>%
        summarise(
          Nb_Occurrences = n(),
          Nb_Fichiers = n_distinct(Fichier_Source),
          Moyenne_Score = round(mean(MEAN, na.rm = TRUE), 2),
          .groups = 'drop'
        ) %>%
        arrange(desc(Nb_Occurrences))
      
      consolidated_data$"Stats_Produits" <- product_stats
    }
    
    # Statistiques des tests triangulaires
    triangular_tests <- all_results_df %>%
      filter(TESTTYPE == "Triangular") %>%
      select(Fichier_Source, IDTEST, REFERENCE, CANDIDATE, N, CORRECT, P_VALUE, DECISION)
    
    if(nrow(triangular_tests) > 0) {
      consolidated_data$"Tests_Triangulaires" <- triangular_tests
    }
    
  } else {
    consolidated_data$"Tous_Resultats" <- tibble(
      Message = "Aucun résultat d'analyse disponible",
      Timestamp = Sys.time()
    )
  }
  
  # 3. Consolidation du tracking des juges
  if(length(judge_removal_info) > 0) {
    all_judge_info_df <- bind_rows(judge_removal_info)
    consolidated_data$"Tracking_Juges_Global" <- all_judge_info_df
    
    # Statistiques des juges retirés
    judge_stats <- all_judge_info_df %>%
      group_by(RemovedJudges) %>%
      summarise(
        Nb_Retraits = n(),
        Fichiers_Concernes = n_distinct(File),
        .groups = 'drop'
      ) %>%
      separate_rows(RemovedJudges, sep = ", ") %>%
      filter(RemovedJudges != "" & !is.na(RemovedJudges)) %>%
      group_by(RemovedJudges) %>%
      summarise(
        Total_Retraits = sum(Nb_Retraits),
        Total_Fichiers = sum(Fichiers_Concernes),
        .groups = 'drop'
      ) %>%
      arrange(desc(Total_Retraits))
    
    consolidated_data$"Stats_Juges_Retires" <- judge_stats
  } else {
    consolidated_data$"Tracking_Juges_Global" <- tibble(
      Message = "Aucun juge retiré dans cette analyse",
      Timestamp = Sys.time()
    )
  }
  
  # 4. Consolidation des données brutes
  if(length(all_raw_data) > 0) {
    all_raw_data_df <- bind_rows(all_raw_data)
    
    # Statistiques globales des données brutes
    raw_data_stats <- all_raw_data_df %>%
      group_by(SourceFile) %>%
      summarise(
        Nb_Lignes = n(),
        Nb_Juges = n_distinct(CJ),
        Nb_Produits = n_distinct(ProductName),
        Nb_Attributs = n_distinct(AttributeName),
        Nb_Fonctions = n_distinct(NomFonction),
        Valeur_Min = min(Value, na.rm = TRUE),
        Valeur_Max = max(Value, na.rm = TRUE),
        Valeur_Moyenne = round(mean(Value, na.rm = TRUE), 2),
        Nb_Valeurs_Manquantes = sum(is.na(Value)),
        .groups = 'drop'
      )
    
    consolidated_data$"Stats_Donnees_Brutes" <- raw_data_stats
  }
  
  # 5. Log des problèmes détectés
  if(length(data_issues_log) > 0) {
    issues_df <- tibble(
      Probleme = unlist(data_issues_log),
      Timestamp = Sys.time()
    ) %>%
      separate(Probleme, into = c("Type", "Details"), sep = "] ", extra = "merge") %>%
      mutate(Type = str_remove(Type, "^\\["))
    
    consolidated_data$"Problemes_Detectes" <- issues_df
    
    # Résumé des types de problèmes
    problem_summary <- issues_df %>%
      group_by(Type) %>%
      summarise(
        Nb_Occurrences = n(),
        .groups = 'drop'
      ) %>%
      arrange(desc(Nb_Occurrences))
    
    consolidated_data$"Resume_Problemes" <- problem_summary
  }
  
  # 6. Données de tracking des fichiers
  consolidated_data$"Tracking_Fichiers" <- tracking_data %>%
    arrange(desc(Date_Traitement))
  
  # Écriture du fichier consolidé
  write_xlsx(consolidated_data, consolidated_file_path)
  message("📊 Fichier consolidé global généré: ", consolidated_file_path)
  
}, error = function(e) {
  message("❌ ERREUR génération fichier consolidé: ", e$message)
  
  # Créer un fichier d'erreur minimal
  error_consolidated <- list(
    "ERREUR_CONSOLIDATION" = tibble(
      Erreur = e$message,
      Timestamp = Sys.time(),
      Message = "Échec de génération du fichier consolidé",
      Nb_Fichiers_Traites = files_processed
    )
  )
  
  tryCatch({
    write_xlsx(error_consolidated, consolidated_file_path)
    message("📊 Fichier d'erreur consolidé créé: ", consolidated_file_path)
  }, error = function(e2) {
    message("❌ Impossible de créer le fichier d'erreur consolidé: ", e2$message)
  })
})

# ===== SAUVEGARDE FINALE DU TRACKING =====
save_tracking_data(tracking_data)

# ===== RÉSUMÉ FINAL =====
message("\n", paste(rep("=", 60), collapse = ""))
message("ANALYSE SENSO TERMINÉE - RÉSUMÉ FINAL")
message(paste(rep("=", 60), collapse = ""))
message("📁 Dossier source: ", raw_data_dir)
message("📁 Dossier sortie: ", output_base_dir)
message("📊 Fichiers Excel détectés: ", length(excel_files))
message("⏭️  Fichiers déjà traités (skippés): ", files_skipped)
message("🆕 Nouveaux fichiers détectés: ", files_new)
message("✅ Fichiers traités avec succès: ", files_processed)
message("❌ Fichiers en erreur: ", files_new - files_processed)

if(files_new > 0) {
  message("📈 Taux de succès: ", round((files_processed / files_new) * 100, 1), "%")
}

message("🗃️  Résultats d'analyse générés: ", length(all_results))

if(length(data_issues_log) > 0) {
  message("⚠️  Problèmes détectés: ", length(data_issues_log))
  message("   Consultez les fichiers de sortie pour plus de détails")
}

# Statistiques des bases de données
# ===== RÉSUMÉ FINAL MODIFIÉ =====
# Dans la section finale, remplacer :
message("\n📊 SAUVEGARDE DANS LES BASES DE DONNÉES:")
message("   • SA_RAW_DATA: Données brutes sauvegardées")
message("   • SA_RESULTS_DATA: Résultats d'analyse sauvegardés dans tables spécialisées:")
message("     - strengthandmo_results: Tests Strength et Strength with Malodour")
message("     - proximity_results: Tests de proximité") 
message("     - triangulaire_results: Tests triangulaires")
message("   • SA_JUDGES: Tracking des juges sauvegardé")
message("   • SA_METADATA: Métadonnées sauvegardées:")
message("     - product_info: Informations produits")
message("     - test_info: Informations tests")
message("     - databrute: Couples métadonnées")


message("\n📄 FICHIERS GÉNÉRÉS:")
message("   • Fichiers individuels: ", files_processed, " fichiers ANALYSE_*.xlsx")
message("   • Fichier consolidé: ANALYSE_CONSOLIDEE_GLOBALE.xlsx")
message("   • Fichier de tracking: TRACKING_FICHIERS.xlsx")

message("\n🕐 Analyse terminée: ", Sys.time())
message(paste(rep("=", 60), collapse = ""))

# ===== NETTOYAGE FINAL =====
# Fermer toutes les connexions ouvertes (sécurité)
tryCatch({
  # Nettoyer l'environnement des gros objets
  if(exists("all_raw_data_df")) rm(all_raw_data_df)
  if(exists("all_results_df")) rm(all_results_df)
  
  # Forcer le garbage collection
  gc()
  
  message("🧹 Nettoyage de l'environnement terminé")
}, error = function(e) {
  message("⚠️  Erreur lors du nettoyage: ", e$message)
})

message("\n🎉 PROCESSUS D'ANALYSE SENSO COMPLÈTEMENT TERMINÉ! 🎉")
