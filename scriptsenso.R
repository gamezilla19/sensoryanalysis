# PROJET ANALYSE SENSO - VERSION AVEC TRACKING
# Auteur: Version optimisée avec système de tracking des fichiers traités
# Date: 2025-08-05

# ===== CHARGEMENT DES BIBLIOTHÈQUES =====
# Cette section charge toutes les bibliothèques nécessaires pour l'analyse sensorielle
# suppressPackageStartupMessages() évite l'affichage des messages de démarrage pour un log plus propre

suppressPackageStartupMessages({
  # --- MANIPULATION ET ANALYSE DE DONNÉES ---
  library(tidyverse)    # Suite complète pour manipulation de données (dplyr, ggplot2, etc.)
  # Utilisé pour : filter(), mutate(), group_by(), summarise(), pipes (%>%)
  
  library(readxl)       # Lecture des fichiers Excel (.xlsx, .xls)
  # Utilisé pour : read_excel(), excel_sheets()
  
  # --- ANALYSES STATISTIQUES SENSORIELLES ---
  library(agricolae)    # Package spécialisé pour analyses agricoles et sensorielles
  # Utilisé pour : SNK.test() (Student-Newman-Keuls), duncan.test()
  # Permet les comparaisons multiples entre produits avec groupes homogènes
  
  # --- CONNECTIVITÉ BASE DE DONNÉES ---
  library(DBI)          # Interface générique pour bases de données
  # Utilisé pour : dbConnect(), dbDisconnect(), dbWriteTable()
  
  library(RPostgreSQL)  # Driver PostgreSQL (ancienne version, maintenu pour compatibilité)
  # Utilisé pour : connexion à la base PostgreSQL pour sauvegarde des résultats
  
  library(odbc)         # Driver ODBC générique (alternative pour autres SGBD)
  # Utilisé comme fallback si PostgreSQL n'est pas disponible
  
  # --- GESTION DE FICHIERS ET EXPORT ---
  library(fs)           # Manipulation moderne du système de fichiers
  # Utilisé pour : dir_ls() (liste des fichiers), path manipulation
  
  library(writexl)      # Export vers Excel (.xlsx) - plus rapide que openxlsx
  # Utilisé pour : write_xlsx() pour générer les rapports finaux
  
  # --- MANIPULATION DE TEXTE ---
  library(stringr)      # Manipulation avancée des chaînes de caractères
  # Utilisé pour : str_detect(), str_replace_all(), str_extract()
  # Essentiel pour identifier les types de tests (triangulaire, proximité, etc.)
  
  # --- SÉCURITÉ ET TRACKING ---
  library(digest)       # Calcul de hash cryptographiques (MD5, SHA1, etc.)
  # Utilisé pour : calculate_file_hash() - détection des modifications de fichiers
  # Permet d'éviter de retraiter des fichiers identiques (système de cache)
})


# ===== INITIALISATION =====
message("Début analyse: ", Sys.time())

raw_data_dir <- "C:/testManon"                    # Dossier source des fichiers Excel
output_base_dir <- "C:/ResultatsAnalyseSenso"    # Dossier de destination pour tous les outputs

# ===== PRÉPARATION DE L'ENVIRONNEMENT DE TRAVAIL =====

# Création du dossier de sortie principal s'il n'existe pas
# Cette vérification évite les erreurs lors de la sauvegarde des résultats
if(!dir.exists(output_base_dir)) {
  # recursive = TRUE : crée tous les dossiers parents nécessaires dans le chemin
  # showWarnings = FALSE : évite les messages d'avertissement si le dossier existe déjà
  dir.create(output_base_dir, recursive = TRUE, showWarnings = FALSE)
  message("Création du dossier de sortie principal: ", output_base_dir)
}

# ===== SYSTÈME DE TRACKING DES FICHIERS =====
# Ce système évite de retraiter des fichiers déjà analysés (gain de temps considérable)
# Le fichier de tracking est stocké dans le dossier de sortie pour persistance

# Définition du chemin du fichier de tracking Excel
tracking_file <- file.path(output_base_dir, "TRACKING_FICHIERS.xlsx")

# Fonction pour créer/lire le fichier de tracking
# Cette fonction gère l'initialisation et le chargement de l'historique des traitements
load_tracking_data <- function() {
  
  # ÉTAPE 1 : Vérifier si le fichier de tracking existe déjà
  if(file.exists(tracking_file)) {
    
    # ÉTAPE 2 : Tentative de lecture du fichier existant avec gestion d'erreur
    tryCatch({
      # Chargement des données de tracking depuis Excel
      tracking_data <- read_excel(tracking_file)
      message("Fichier de tracking chargé: ", nrow(tracking_data), " entrées")
      return(tracking_data)
      
    }, error = function(e) {
      # En cas d'erreur (fichier corrompu, format incorrect, etc.)
      message("Erreur lecture tracking, création nouveau fichier: ", e$message)
      
      # STRUCTURE DE DONNÉES DE TRACKING - Colonnes essentielles :
      return(tibble(
        Fichier = character(0),              # Nom du fichier Excel traité
        Chemin_Complet = character(0),       # Chemin absolu complet du fichier
        Hash_MD5 = character(0),             # Empreinte MD5 pour détecter les modifications
        Date_Traitement = as.POSIXct(character(0)), # Timestamp précis du traitement
        Statut = character(0),               # État : "SUCCES", "ERREUR", "SKIP", etc.
        Taille_Fichier = numeric(0),         # Taille en octets (détection changements)
        Nb_Lignes_Results = numeric(0)       # Nombre de lignes traitées (métrique qualité)
      ))
    })
    
  } else {
    # ÉTAPE 3 : Première exécution - création d'un tracking vide
    message("Création nouveau fichier de tracking")
    
    # INITIALISATION D'UN TRACKING VIDE avec la même structure
    # Cette structure garantit la cohérence des données même lors de la première exécution
    return(tibble(
      Fichier = character(0),              # Vecteur vide de type caractère
      Chemin_Complet = character(0),       # Vecteur vide de type caractère  
      Hash_MD5 = character(0),             # Vecteur vide de type caractère
      Date_Traitement = as.POSIXct(character(0)), # Vecteur vide de type datetime
      Statut = character(0),               # Vecteur vide de type caractère
      Taille_Fichier = numeric(0),         # Vecteur vide de type numérique
      Nb_Lignes_Results = numeric(0)       # Vecteur vide de type numérique
    ))
  }
}

# ===== FONCTIONS DE GESTION DU SYSTÈME DE TRACKING =====
# Ces fonctions implémentent un système de cache intelligent basé sur les empreintes MD5

# Fonction pour calculer le hash MD5 d'un fichier
# Le hash MD5 est une empreinte unique qui change si le contenu du fichier est modifié
calculate_file_hash <- function(file_path) {
  tryCatch({
    # digest() avec file = TRUE lit directement le fichier et calcule son empreinte MD5
    # Avantage : détecte même les plus petites modifications dans le fichier Excel
    # algo = "md5" : algorithme rapide et suffisant pour détecter les changements
    digest(file_path, algo = "md5", file = TRUE)
    
  }, error = function(e) {
    # Gestion des erreurs : fichier inexistant, permissions insuffisantes, etc.
    message("Erreur calcul hash pour ", basename(file_path), ": ", e$message)
    return(NA_character_)  # Retourne NA en cas d'erreur pour éviter les plantages
  })
}

# Fonction pour vérifier si un fichier a déjà été traité avec succès
# Cette fonction est le cœur du système de cache - évite les retraitements inutiles
is_file_already_processed <- function(file_path, tracking_data) {
  
  # ÉTAPE 1 : Si aucun fichier n'a jamais été traité, retourner FALSE
  if(nrow(tracking_data) == 0) return(FALSE)
  
  # ÉTAPE 2 : Calculer le hash actuel du fichier à vérifier
  current_hash <- calculate_file_hash(file_path)
  if(is.na(current_hash)) return(FALSE)  # Si erreur de calcul, considérer comme non traité
  
  # ÉTAPE 3 : Rechercher dans l'historique une entrée correspondante
  # Critères de correspondance :
  # - Même nom de fichier (basename)
  # - Même empreinte MD5 (contenu identique)
  # - Statut "SUCCES" (traitement réussi précédemment)
  existing_entry <- tracking_data %>%
    filter(
      Fichier == basename(file_path) &     # Même nom de fichier
        Hash_MD5 == current_hash &           # Contenu identique (hash MD5)
        Statut == "SUCCES"                   # Traitement précédent réussi
    )
  
  # RÉSULTAT : TRUE si une entrée correspondante existe, FALSE sinon
  return(nrow(existing_entry) > 0)
}

# Fonction pour mettre à jour le fichier de tracking après traitement
# Cette fonction enregistre le résultat du traitement pour les futures exécutions
update_tracking <- function(file_path, statut, nb_lignes = NA, tracking_data) {
  
  # ÉTAPE 1 : Collecter les métadonnées du fichier traité
  current_hash <- calculate_file_hash(file_path)    # Empreinte MD5 du fichier
  file_size <- file.info(file_path)$size            # Taille en octets
  
  # ÉTAPE 2 : Créer une nouvelle entrée de tracking
  new_entry <- tibble(
    Fichier = basename(file_path),          # Nom du fichier (sans le chemin)
    Chemin_Complet = as.character(file_path), # Chemin absolu complet
    Hash_MD5 = current_hash,                # Empreinte MD5 pour détection changements
    Date_Traitement = Sys.time(),           # Timestamp précis du traitement
    Statut = statut,                        # "SUCCES", "ERREUR", "SKIP", etc.
    Taille_Fichier = file_size,             # Taille du fichier (métrique supplémentaire)
    Nb_Lignes_Results = nb_lignes           # Nombre de lignes traitées (qualité)
  )
  
  # ÉTAPE 3 : Supprimer l'ancienne entrée si elle existe (évite les doublons)
  # On supprime les entrées avec le même nom ET le même hash
  tracking_data_updated <- tracking_data %>%
    filter(!(
      Fichier == basename(file_path) & 
        Hash_MD5 == current_hash
    ))
  
  # ÉTAPE 4 : Ajouter la nouvelle entrée au tracking
  # bind_rows() combine les données existantes avec la nouvelle entrée
  tracking_data_updated <- bind_rows(tracking_data_updated, new_entry)
  
  # RÉSULTAT : Retourner le tracking mis à jour
  return(tracking_data_updated)
}


# ===== FONCTIONS DE PERSISTANCE ET RÉCUPÉRATION DES DONNÉES =====
# Ces fonctions gèrent la sauvegarde du tracking et la récupération des résultats existants

# Fonction pour sauvegarder le fichier de tracking
# Cette fonction persiste l'état du système de cache sur disque
save_tracking_data <- function(tracking_data) {
  tryCatch({
    # Sauvegarde au format Excel pour faciliter la consultation manuelle
    # Le fichier de tracking devient un tableau de bord des traitements
    write_xlsx(tracking_data, tracking_file)
    message("Fichier de tracking sauvegardé: ", tracking_file)
    
  }, error = function(e) {
    # Gestion des erreurs : permissions, espace disque, fichier verrouillé, etc.
    message("Erreur sauvegarde tracking: ", e$message)
    # Note : Pas d'arrêt du script - le tracking est un bonus, pas critique
  })
}

# Fonction pour charger les résultats existants depuis le rapport consolidé
# Cette fonction implémente un système de récupération intelligent des données déjà traitées
load_existing_results <- function() {
  
  # ÉTAPE 1 : Construire le chemin du fichier consolidé du jour
  # Format : RAPPORT_SENSO_CONSOLIDE_YYYYMMDD.xlsx
  consolidated_file <- file.path(
    output_base_dir, 
    "RAPPORT_CONSOLIDE", 
    paste0("RAPPORT_SENSO_CONSOLIDE_", format(Sys.Date(), "%Y%m%d"), ".xlsx")
  )
  
  # ÉTAPE 2 : Initialiser les structures de données vides
  # Ces listes contiendront les données récupérées, organisées par fichier source
  existing_results <- list()      # Résultats d'analyse par fichier
  existing_judge_info <- list()   # Informations sur les juges (actuellement non utilisé)
  existing_raw_data <- list()     # Données brutes par fichier
  
  # ÉTAPE 3 : Vérifier l'existence du fichier consolidé
  if(file.exists(consolidated_file)) {
    tryCatch({
      
      # SOUS-ÉTAPE 3A : Charger les résultats d'analyse consolidés
      # Vérifier d'abord que la feuille "Resultats_Tous" existe
      if("Resultats_Tous" %in% excel_sheets(consolidated_file)) {
        # Lire la feuille des résultats consolidés
        consolidated_results <- read_excel(consolidated_file, sheet = "Resultats_Tous")
        
        # Si des données existent, les organiser par fichier source
        if(nrow(consolidated_results) > 0) {
          # split() divise le dataframe en liste basée sur la colonne SOURCEFILE
          # Résultat : une entrée par fichier source avec ses résultats
          existing_results <- split(consolidated_results, consolidated_results$SOURCEFILE)
          message("Résultats existants chargés: ", length(existing_results), " fichiers")
        }
      }
      
      # SOUS-ÉTAPE 3B : Charger les données brutes consolidées
      # Vérifier d'abord que la feuille "Donnees_Brutes_Toutes" existe
      if("Donnees_Brutes_Toutes" %in% excel_sheets(consolidated_file)) {
        # Lire la feuille des données brutes consolidées
        raw_data <- read_excel(consolidated_file, sheet = "Donnees_Brutes_Toutes")
        
        # Si des données existent, les organiser par fichier source
        if(nrow(raw_data) > 0) {
          # Organisation identique : une entrée par fichier source
          existing_raw_data <- split(raw_data, raw_data$SOURCEFILE)
          message("Données brutes existantes chargées: ", length(existing_raw_data), " fichiers")
        }
      }
      
    }, error = function(e) {
      # Gestion des erreurs de lecture : fichier corrompu, format incorrect, etc.
      message("Erreur chargement fichier consolidé existant: ", e$message)
      # Les listes restent vides en cas d'erreur - pas de plantage du script
    })
  }
  
  # ÉTAPE 4 : Retourner une structure organisée des données récupérées
  # Cette structure permet un accès facile aux données par type et par fichier source
  return(list(
    results = existing_results,        # Liste des résultats par fichier source
    judge_info = existing_judge_info,  # Informations juges (réservé pour usage futur)
    raw_data = existing_raw_data       # Liste des données brutes par fichier source
  ))
}

# ===== CHARGEMENT DU TRACKING ET DES DONNÉES EXISTANTES =====
# Point d'entrée principal : initialisation du système de cache et récupération des données

# Chargement du système de tracking (historique des traitements)
tracking_data <- load_tracking_data()
# Résultat : tibble contenant l'historique complet des fichiers traités
# Colonnes : Fichier, Hash_MD5, Date_Traitement, Statut, etc.

# Chargement des données déjà consolidées (évite les retraitements)
existing_data <- load_existing_results()
# Résultat : liste structurée avec 3 composants :
# - $results : résultats d'analyse par fichier source
# - $judge_info : informations sur les juges (réservé)
# - $raw_data : données brutes par fichier source

# ===== FONCTIONS UTILITAIRES (INCHANGÉES) =====
# Ces fonctions supportent la qualité des données et le debugging

# Fonction de logging centralisé pour les problèmes de données
# Cette fonction maintient un journal global des anomalies détectées
log_probleme <- function(type, details, fichier) {
  
  # CONSTRUCTION DU MESSAGE : Format standardisé pour traçabilité
  # [TYPE] Description | Fichier: nom_fichier.xlsx
  msg <- paste0(
    "[", type, "] ",                    # Catégorie du problème (ex: DONNEES, FORMAT, VALIDATION)
    details,                            # Description détaillée du problème
    " | Fichier: ", basename(fichier)   # Nom du fichier concerné (sans chemin)
  )
  
  # ENREGISTREMENT GLOBAL : Ajout au journal global des problèmes
  # L'opérateur <<- modifie la variable globale data_issues_log
  # Cette approche centralise tous les problèmes pour rapport final
  data_issues_log <<- append(data_issues_log, msg)
  
  # AFFICHAGE IMMÉDIAT : Notification en temps réel pour debugging
  message("PROBLEME: ", msg)
  
  # Note : Cette fonction ne retourne rien - elle fait du logging pur
}

# Fonction de validation de la cohérence des données
# Cette fonction détecte les anomalies dans les données chargées
validate_data_consistency <- function(file_data) {
  
  # INITIALISATION : Vecteur pour collecter les problèmes détectés
  issues <- character()
  
  # VALIDATION 1 : Détection des valeurs non numériques inattendues
  # Logique : compter les valeurs qui ne sont ni numériques ni NA
  invalid_values <- sum(
    is.na(as.numeric(file_data$Value)) &  # Conversion échoue (non numérique)
      !is.na(file_data$Value)               # Mais la valeur originale n'est pas NA
  )
  
  # Si des valeurs invalides sont trouvées, ajouter au rapport
  if(invalid_values > 0) {
    issues <- c(issues, paste("Valeurs non numériques détectées:", invalid_values))
  }
  
  
  # RÉSULTAT : Vecteur des problèmes détectés (vide si tout va bien)
  return(issues)
}


# ===== FONCTION CRÉATION TABLE TRACKING JUGES CORRIGÉE =====
# Cette fonction génère un tableau de bord complet du comportement des juges
# across tous les fichiers traités avec gestion robuste des erreurs

create_judge_tracking_table <- function(all_judge_info, all_raw_data) {
  tryCatch({
    
    # ===== ÉTAPE 1 : VALIDATION DES DONNÉES D'ENTRÉE =====
    # Vérification préalable pour éviter les erreurs downstream
    if(nrow(all_raw_data) == 0) {
      message("Aucune donnée brute disponible pour le tracking des juges")
      # Retour gracieux avec message informatif
      return(tibble(
        Message = "Aucune donnée disponible",
        Timestamp = Sys.time()
      ))
    }
    
    # ===== ÉTAPE 2 : COMPILATION DES PARTICIPATIONS PAR JUGE =====
    # Agrégation des statistiques de performance par juge et par fichier
    judge_participation <- all_raw_data %>%
      group_by(SourceFile, CJ) %>%                    # Groupement par fichier et juge
      summarise(
        # MÉTRIQUES DE VOLUME : Quantité d'évaluations
        NbEvaluations = n(),                          # Nombre total d'évaluations du juge
        
        # MÉTRIQUES DE QUALITÉ : Performance moyenne
        MoyenneScore = mean(Value, na.rm = TRUE),     # Score moyen attribué par le juge
        
        # MÉTRIQUES DE COUVERTURE : Diversité des évaluations
        AttributesEvalues = n_distinct(AttributeName, na.rm = TRUE),  # Nb attributs évalués
        ProduitsEvalues = n_distinct(ProductName, na.rm = TRUE),      # Nb produits évalués
        
        .groups = 'drop'                              # CORRECTION CRITIQUE : évite les warnings
        # .groups = 'drop' supprime explicitement le groupement après summarise()
        # Sans cela, dplyr génère des warnings sur les opérations suivantes
      ) %>%
      mutate(
        DateAnalyse = Sys.Date()                      # Horodatage de l'analyse
      )
    
    # ===== ÉTAPE 3 : INTÉGRATION DES INFORMATIONS DE SUPPRESSION =====
    # Ajout des données sur les juges exclus (si disponibles)
    if(nrow(all_judge_info) > 0 && "RemovedJudges" %in% names(all_judge_info)) {
      
      # SOUS-ÉTAPE 3A : Préparation des données de suppression
      judge_removal_summary <- all_judge_info %>%
        # separate_rows() transforme "Juge1, Juge2" en deux lignes distinctes
        separate_rows(RemovedJudges, sep = ", ") %>%  # Séparation des juges multiples
        filter(RemovedJudges != "" & !is.na(RemovedJudges)) %>%  # Exclusion des valeurs vides
        select(File, Segment, RemovedJudges) %>%      # Sélection des colonnes pertinentes
        rename(SourceFile = File, CJ = RemovedJudges) %>%  # Harmonisation des noms
        mutate(JudgeStatus = "removed")               # Marquage du statut de suppression
      
      # SOUS-ÉTAPE 3B : Jointure avec les données de participation
      judge_tracking <- judge_participation %>%
        left_join(judge_removal_summary, by = c("SourceFile", "CJ")) %>%
        mutate(
          # coalesce() remplace les NA par "conserved" pour les juges non supprimés
          JudgeStatus = coalesce(JudgeStatus, "conserved")
        )
      
    } else {
      # CAS ALTERNATIF : Pas d'informations de suppression disponibles
      # Tous les juges sont considérés comme conservés
      judge_tracking <- judge_participation %>%
        mutate(JudgeStatus = "conserved")
    }
    
    # ===== ÉTAPE 4 : CALCULS DE SYNTHÈSE GLOBALE PAR JUGE =====
    # Agrégation des statistiques across tous les fichiers pour chaque juge
    judge_tracking <- judge_tracking %>%
      group_by(CJ) %>%                                # Regroupement par juge
      mutate(
        # MÉTRIQUES DE PARTICIPATION GLOBALE
        NbTestsTotal = n(),                           # Nombre total de tests du juge
        NbTestsConserve = sum(JudgeStatus == "conserved", na.rm = TRUE),  # Tests conservés
        
        # MÉTRIQUES DE FIABILITÉ
        TauxConservation = round(NbTestsConserve / NbTestsTotal, 3)  # Taux de conservation
        # Un taux élevé indique un juge fiable (rarement exclu)
      ) %>%
      ungroup()                                       # Suppression du groupement
    
    # ===== ÉTAPE 5 : RETOUR DU TABLEAU DE BORD COMPLET =====
    return(judge_tracking)
    
  }, error = function(e) {
    # ===== GESTION D'ERREUR ROBUSTE =====
    # En cas d'échec, retour d'un tibble informatif plutôt qu'un crash
    message("Erreur création table tracking juges: ", e$message)
    return(tibble(
      Erreur = paste("Échec création table tracking:", e$message),
      Details = "Vérifiez les colonnes dans vos données",
      Timestamp = Sys.time()
    ))
  })
}


# ===== TOUTES LES AUTRES FONCTIONS (INCHANGÉES) =====

# ===== FONCTION GESTION DES TESTS DE PROXIMITÉ =====
# Cette fonction filtre les juges selon des critères de performance spécifiques
# pour les tests de proximité (mesure de distance perceptuelle)

handle_proximity_test <- function(segment) {
  
  # ===== ÉTAPE 1 : IDENTIFICATION DU BENCHMARK =====
  # Le benchmark est le produit de référence (score le plus bas = plus proche)
  bench_product <- segment %>%
    group_by(ProductName) %>%                         # Groupement par produit
    summarise(avg = mean(Value, na.rm = TRUE), .groups = 'drop') %>%  # Moyenne par produit
    slice_min(avg, n = 1) %>%                        # Sélection du minimum (plus proche)
    pull(ProductName)                                 # Extraction du nom du produit
  
  # LOGIQUE : En test de proximité, le score le plus bas indique la plus grande similarité
  # Le benchmark sert de référence pour évaluer la performance des juges
  
  # ===== ÉTAPE 2 : FILTRAGE DES JUGES SELON CRITÈRES CORRIGÉS =====
  # Application de critères de qualité pour identifier les juges à exclure
  filtered_judges <- segment %>%
    group_by(CJ) %>%                                  # Analyse par juge
    mutate(bench_score = Value[ProductName %in% bench_product]) %>%  # Score du benchmark pour ce juge
    filter(
      # CRITÈRE 1 : Performance sur le benchmark
      # Si le juge donne une note > 4 au benchmark, il est exclu
      # LOGIQUE : Un score élevé au benchmark indique une mauvaise perception de la proximité
      max(Value[ProductName %in% bench_product], na.rm = TRUE) > 4 |
        
        # CRITÈRE 2 : Discrimination entre produits
        # Si au moins un autre produit a un score ≥ 1 point sous le benchmark, juge exclu
        # LOGIQUE : Le juge doit percevoir les autres produits comme plus distants
        any(Value[!ProductName %in% bench_product] <= (bench_score - 1))
    ) %>%
    distinct(CJ) %>%                                  # Juges uniques à exclure
    pull(CJ)                                          # Extraction des identifiants
  
  # ===== ÉTAPE 3 : CONSTRUCTION DU RÉSULTAT =====
  # Retour d'une liste structurée avec toutes les informations nécessaires
  list(
    # DONNÉES NETTOYÉES : Segment sans les juges filtrés
    segment = segment %>% filter(!CJ %in% filtered_judges),
    
    # TRAÇABILITÉ : Liste des juges exclus pour audit
    removed_judges = filtered_judges,
    
    # MÉTRIQUES : Effectifs avant/après filtrage
    n_initial = n_distinct(segment$CJ),               # Nombre initial de juges
    n_final = n_distinct(segment$CJ) - length(filtered_judges)  # Nombre final de juges
  )
}

# ===== FONCTION GESTION DES TESTS TRIANGULAIRES COMPLETS =====
# Cette fonction traite les tests triangulaires avec analyse statistique complète
# Tests triangulaires : identification d'un échantillon différent parmi 3

handle_triangular_test_complete <- function(segment, file_path) {
  tryCatch({
    message("Traitement test triangulaire complet: ", unique(segment$NomFonction))
    
    # ===== ÉTAPE 1 : CONSOLIDATION DES SEGMENTS =====
    # Fusion de tous les segments triangulaires en un dataset unifié
    combined_segment <- segment
    # NOTE : Cette étape permet de traiter plusieurs segments triangulaires ensemble
    
    # ===== ÉTAPE 2 : IDENTIFICATION AUTOMATIQUE REF/CANDIDAT =====
    # Détection automatique des produits de référence et candidat
    ref_product <- NA_character_
    candidat_product <- NA_character_
    
    # Extraction des produits disponibles dans le segment
    available_products <- unique(combined_segment$ProductName)
    message("Produits disponibles dans le segment: ", paste(available_products, collapse = ", "))
    
    # ATTRIBUTION PAR DÉFAUT : Premier produit = référence
    if(length(available_products) > 0) {
      ref_product <- available_products[1]
      message("Référence par défaut: ", ref_product)
    }
    
    # ATTRIBUTION PAR DÉFAUT : Deuxième produit = candidat
    if(length(available_products) > 1) {
      candidat_product <- available_products[2]
      message("Candidat par défaut: ", candidat_product)
    }
    
    # ===== ÉTAPE 3 : CALCULS STATISTIQUES COMPLETS =====
    # Analyse statistique du test triangulaire
    
    # MÉTRIQUES DE BASE
    n_total <- sum(!is.na(combined_segment$Value))    # Nombre total de réponses valides
    n_correct <- sum(combined_segment$Value == 1, na.rm = TRUE)  # Nombre de bonnes réponses
    ratio <- round(n_correct / n_total, 3)            # Taux de réussite
    
    # TEST STATISTIQUE BINOMIAL
    # H0 : p = 1/3 (performance au hasard en test triangulaire)
    # H1 : p > 1/3 (performance significativement supérieure au hasard)
    test_result <- binom.test(n_correct, n_total, p = 1/3)
    p_value <- round(test_result$p.value, 4)          # Significativité statistique
    
    # LOGGING DES RÉSULTATS
    message("Test triangulaire - Total: ", n_total, " | Correct: ", n_correct, 
            " | Ratio: ", ratio, " | p-value: ", p_value)
    
    # ===== ÉTAPE 4 : FORMATAGE DU RÉSULTAT FINAL =====
    # Construction du tibble de résultats au format standardisé
    result <- tibble(
      IDTEST = unique(combined_segment$TrialName)[1], # Identifiant du test
      Type = "Triangulaire",                          # Type de test
      Ref = ref_product,                              # Produit de référence
      Candidat = candidat_product,                    # Produit candidat
      Ratio = ratio,                                  # Taux de réussite
      N_tot = n_total,                               # Effectif total
      N_correct = n_correct,                         # Nombre de bonnes réponses
      `P-value` = p_value                            # Significativité statistique
    )
    
    return(result)
    
  }, error = function(e) {
    # ===== GESTION D'ERREUR ROBUSTE =====
    # En cas d'échec, retour d'un résultat avec marqueurs d'erreur
    message("ERREUR dans handle_triangular_test_complete: ", e$message)
    return(tibble(
      IDTEST = "ERREUR",                             # Marqueur d'erreur
      Type = "Triangulaire",
      Ref = NA_character_,                           # Valeurs manquantes
      Candidat = NA_character_,
      Ratio = NA_real_,
      N_tot = NA_integer_,
      N_correct = NA_integer_,
      `P-value` = NA_real_
    ))
  })
}

# ===== FONCTION DE TRAITEMENT DES SEGMENTS TRIANGULAIRES =====
# Cette fonction orchestre le traitement de tous les segments triangulaires d'un fichier
# en les fusionnant pour une analyse globale unifiée

process_triangular_segments <- function(segments, file_path) {
  
  # ===== ÉTAPE 1 : IDENTIFICATION DES SEGMENTS TRIANGULAIRES =====
  # Détection automatique des segments contenant des tests triangulaires
  triangular_indices <- which(sapply(segments, function(seg) {
    # CRITÈRES DE DÉTECTION :
    # 1. NomFonction n'est pas NA
    # 2. NomFonction contient "Triangulaire" ou "triangle" (insensible à la casse)
    !is.na(seg$NomFonction[1]) && str_detect(seg$NomFonction[1], "Triangulaire|triangle")
  }))
  
  # ===== VALIDATION : VÉRIFICATION DE LA PRÉSENCE DE TESTS TRIANGULAIRES =====
  if(length(triangular_indices) == 0) {
    # RETOUR GRACIEUX : Aucun test triangulaire détecté
    return(NULL)
  }
  
  # ===== ÉTAPE 2 : FUSION DES SEGMENTS TRIANGULAIRES =====
  # Consolidation de tous les segments triangulaires en un dataset unique
  all_triangular_data <- bind_rows(segments[triangular_indices])
  # bind_rows() combine les tibbles en préservant toutes les colonnes
  # Cette approche permet une analyse statistique plus robuste (effectifs plus importants)
  
  # LOGGING : Information sur la fusion réalisée
  message("Fusion de ", length(triangular_indices), " segments triangulaires en un seul")
  
  # ===== ÉTAPE 3 : TRAITEMENT UNIFIÉ =====
  # Appel de la fonction de traitement complet sur les données fusionnées
  result <- handle_triangular_test_complete(all_triangular_data, file_path)
  
  return(result)
}

# ===== FONCTION DE TRAITEMENT TRIANGULAIRE SIMPLE =====
# Cette fonction traite un segment triangulaire individuel avec analyse statistique détaillée
# Version alternative pour traitement segment par segment (sans fusion)

handle_triangular_test <- function(segment) {
  tryCatch({
    message("Traitement test triangulaire: ", unique(segment$NomFonction))
    
    # ===== ÉTAPE 1 : CALCULS STATISTIQUES DE BASE =====
    # Métriques fondamentales du test triangulaire
    n_total <- sum(!is.na(segment$Value))             # Effectif total (réponses valides)
    n_correct <- sum(segment$Value == 1, na.rm = TRUE)  # Nombre de bonnes identifications
    ratio <- round(n_correct / n_total, 3)            # Taux de réussite observé
    
    # ===== ÉTAPE 2 : TEST STATISTIQUE BINOMIAL AVANCÉ =====
    # Test de significativité contre la performance au hasard (p = 1/3)
    test_result <- binom.test(n_correct, n_total, p = 1/3)
    p_value <- test_result$p.value                    # Probabilité sous H0
    
    # SEUIL DE SIGNIFICATIVITÉ ADAPTÉ : 10% (plus permissif que 5% standard)
    # Justification : Tests sensoriels souvent avec effectifs réduits
    significatif <- p_value < 0.10
    
    # LOGGING DÉTAILLÉ : Traçabilité complète des calculs
    message("Test triangulaire - Total: ", n_total, " | Correct: ", n_correct, 
            " | Ratio: ", ratio, " | p-value: ", round(p_value, 4))
    
    # ===== ÉTAPE 3 : CONSTRUCTION DU RÉSULTAT ENRICHI =====
    # Tibble de résultats avec toutes les métriques statistiques
    result <- tibble(
      # IDENTIFICATION
      IDTest = unique(segment$TrialName)[1],          # Identifiant unique du test
      Segment = paste("Triangulaire", unique(segment$NomFonction)[1], sep = " - "),  # Description
      `ID segment` = 1,                               # Numéro de segment
      TestType = "Triangulaire",                      # Type de test
      
      # MÉTRIQUES DE PERFORMANCE
      N_total = n_total,                              # Effectif total
      N_correct = n_correct,                          # Bonnes réponses
      Ratio = ratio,                                  # Taux de réussite
      
      # ANALYSE STATISTIQUE
      P_value = round(p_value, 4),                    # Significativité
      Significatif_10pct = significatif,              # Test à 10%
      
      # INTERVALLES DE CONFIANCE
      # Bornes de l'intervalle de confiance pour le taux de réussite
      IC_inf = round(test_result$conf.int[1], 3),     # Borne inférieure
      IC_sup = round(test_result$conf.int[2], 3)      # Borne supérieure
    )
    
    return(result)
    
  }, error = function(e) {
    # ===== GESTION D'ERREUR ROBUSTE =====
    message("ERREUR dans handle_triangular_test: ", e$message)
    return(NULL)                                      # Retour NULL en cas d'échec
  })
}

# ===== FONCTION D'ANALYSE ITÉRATIVE DES JUGES =====
# Cette fonction applique un filtrage itératif des juges aberrants basé sur l'ANOVA
# Elle retire progressivement les juges les plus déviants jusqu'à convergence

analyze_judges_iterative <- function(segment) {
  
  # ===== ÉTAPE 1 : TRAITEMENT SPÉCIAL POUR LES TESTS MO (ODEUR CORPORELLE) =====
  # Exception méthodologique : Les tests d'odeur corporelle ne subissent pas de filtrage
  if (length(segment$AttributeName) > 0 && !is.na(segment$AttributeName[1]) && 
      str_detect(str_to_lower(segment$AttributeName[1]), "odeur corporell")) {
    
    message("Traitement MO détecté - Pas de filtrage des juges")
    
    # RETOUR SANS MODIFICATION : Préservation de tous les juges
    return(list(
      segment = segment,                              # Données inchangées
      removed_judges = character(0),                  # Aucun juge retiré
      n_initial = n_distinct(segment$CJ),            # Effectif initial
      n_final = n_distinct(segment$CJ)               # Effectif final (identique)
    ))
  }
  
  # ===== ÉTAPE 2 : INITIALISATION DU PROCESSUS ITÉRATIF =====
  n_judges_initial <- n_distinct(segment$CJ)         # Nombre initial de juges
  removed_judges_total <- c()                        # Vecteur des juges retirés
  current_data <- segment                            # Données de travail (copie)
  
  # ===== ÉTAPE 3 : BOUCLE ITÉRATIVE DE FILTRAGE =====
  repeat {
    # VÉRIFICATION 1 : Seuil minimal de juges (8 minimum)
    n_judges_current <- n_distinct(current_data$CJ)
    if (n_judges_current <= 8) {
      message("Seuil minimal atteint (8 juges) - Arrêt du filtrage")
      break
    }
    
    # ===== ANALYSE ANOVA : TEST DE L'EFFET JUGE =====
    # Modèle : Value ~ CJ (effet du juge sur les scores)
    model <- aov(Value ~ CJ, data = current_data)
    anova_res <- anova(model)
    p_value <- anova_res["CJ", "Pr(>F)"]             # P-value de l'effet juge
    
    # VÉRIFICATION 2 : Validité du calcul ANOVA
    if (is.na(p_value)) {
      message("Problème de calcul ANOVA. Arrêt de l'itération pour ce segment.")
      break
    }
    
    # VÉRIFICATION 3 : Significativité de l'effet juge
    # Si p ≥ 0.05 : Les juges ne diffèrent pas significativement → arrêt
    if (p_value >= 0.05) {
      message("Effet juge non significatif (p=", round(p_value, 4), ") - Arrêt du filtrage")
      break
    }
    
    # VÉRIFICATION 4 : Seuil de conservation (2/3 de l'effectif initial)
    # Protection contre un filtrage excessif
    if (n_judges_current <= (2/3) * n_judges_initial) {
      message("Seuil de conservation atteint (2/3 des juges initiaux) - Arrêt du filtrage")
      break
    }
    
    # ===== IDENTIFICATION DU JUGE LE PLUS DÉVIANT =====
    # Calcul des statistiques par juge pour identifier l'aberrant
    judge_stats <- current_data %>%
      group_by(CJ) %>%                                # Groupement par juge
      summarise(MeanScore = mean(Value, na.rm = TRUE), .groups = 'drop') %>%  # Score moyen par juge
      mutate(
        OverallMean = mean(MeanScore, na.rm = TRUE),  # Moyenne générale
        AbsDeviation = abs(MeanScore - OverallMean)   # Déviation absolue de chaque juge
      )
    
    # SÉLECTION : Juge avec la plus grande déviation absolue
    judge_to_remove <- judge_stats %>%
      slice_max(AbsDeviation, n = 1) %>%              # Maximum de déviation
      pull(CJ)                                        # Extraction de l'identifiant
    
    # ===== MISE À JOUR DES DONNÉES =====
    # Ajout du juge à la liste des exclus
    removed_judges_total <- c(removed_judges_total, judge_to_remove)
    
    # Suppression du juge des données courantes
    current_data <- current_data %>% filter(CJ != judge_to_remove)
    
    # LOGGING : Traçabilité de chaque retrait
    message("Juge retiré: ", judge_to_remove,
            " | Déviation: ", round(max(judge_stats$AbsDeviation), 2),
            " | Nouveau n juges: ", n_judges_current - 1)
  }
  
  # ===== ÉTAPE 4 : CONSTRUCTION DU RÉSULTAT FINAL =====
  return(list(
    # DONNÉES NETTOYÉES : Segment après filtrage itératif
    segment = current_data,
    
    # TRAÇABILITÉ : Liste unique des juges retirés (dédoublonnage)
    removed_judges = unique(removed_judges_total),
    
    # MÉTRIQUES : Effectifs avant/après pour évaluation de l'impact
    n_initial = n_judges_initial,                     # Nombre initial
    n_final = n_distinct(current_data$CJ)             # Nombre final
  ))
}

# ===== FONCTION D'ANALYSE DES PRODUITS =====
# Cette fonction orchestre l'analyse statistique complète des produits d'un segment
# Elle gère automatiquement les tests triangulaires et les tests sensoriels standard

analyze_products <- function(segment, segment_index, file_path = NULL) {
  tryCatch({
    seg_data <- segment  # Copie de travail des données
    
    # ===== ÉTAPE 1 : DÉTECTION ET ROUTAGE DES TESTS TRIANGULAIRES =====
    # Identification automatique des tests triangulaires par le nom de fonction
    if(str_detect(segment$NomFonction[1], "Triangulaire") || 
       str_detect(segment$NomFonction[1], "triangle")) {
      
      message("Détection test triangulaire pour segment: ", segment$NomFonction[1])
      
      # ROUTAGE CONDITIONNEL : Choix de la méthode selon la disponibilité du file_path
      if(!is.null(file_path)) {
        # MÉTHODE COMPLÈTE : Avec gestion des fichiers et analyse avancée
        return(handle_triangular_test_complete(segment, file_path))
      } else {
        # MÉTHODE FALLBACK : Analyse triangulaire basique
        return(handle_triangular_test(segment))
      }
    }
    
    # ===== ÉTAPE 2 : VALIDATION DE LA FAISABILITÉ DE L'ANALYSE =====
    # Vérification du nombre minimal de produits (≥ 2 pour comparaison)
    if (n_distinct(seg_data$ProductName) < 2) {
      message("Analyse impossible : moins de 2 produits dans le segment")
      return(NULL)  # Retour NULL si analyse impossible
    }
    
    # ===== ÉTAPE 3 : CALCUL DES STATISTIQUES DESCRIPTIVES PAR PRODUIT =====
    # Agrégation des métriques de base pour chaque produit
    stats_df <- seg_data %>%
      group_by(ProductName) %>%                       # Groupement par produit
      summarise(
        # MOYENNE : Gestion des cas vides (n=0)
        Mean = ifelse(n() == 0, NA, round(mean(Value, na.rm = TRUE), 2)),
        
        # ÉCART-TYPE : Nécessite au moins 2 observations
        Sd = ifelse(n() < 2, NA, round(sd(Value, na.rm = TRUE), 2)),
        
        # EFFECTIF : Nombre d'observations par produit
        n = n(),
        .groups = 'drop'                              # Suppression du groupement
      )
    
    # ===== VALIDATION : DÉTECTION DES GROUPES VIDES =====
    # Alerte en cas de produit sans données (problème de design expérimental)
    if (any(stats_df$n == 0)) {
      warning("Groupe produit vide détecté: ", segment$AttributeName[1])
    }
    
    # ===== ÉTAPE 4 : ANALYSE DE VARIANCE (ANOVA) =====
    # Test de l'effet produit sur les scores sensoriels
    model <- aov(Value ~ ProductName, data = seg_data)  # Modèle ANOVA à 1 facteur
    anova_result <- anova(model)                        # Table d'analyse de variance
    p_value_produit <- anova_result["ProductName", "Pr(>F)"]  # P-value de l'effet produit
    
    # ===== INNOVATION : GÉNÉRALISATION AU SEUIL 10% =====
    # Adaptation aux contraintes des tests sensoriels (effectifs réduits, variabilité élevée)
    significatif_10pct <- !is.na(p_value_produit) && p_value_produit < 0.10
    
    # ===== ÉTAPE 5 : TESTS POST-HOC AVEC STRATÉGIE DE FALLBACK =====
    # Comparaisons multiples pour identifier les groupes homogènes
    snk_result <- tryCatch({
      # MÉTHODE PRÉFÉRÉE : Student-Newman-Keuls (plus conservateur)
      SNK.test(model, "ProductName", group = TRUE, alpha = 0.10)
    }, error = function(e) {
      # MÉTHODE FALLBACK : Test de Duncan (plus permissif)
      message("SNK échoué, utilisation de Duncan avec alpha=0.10")
      duncan.test(model, "ProductName", group = TRUE, alpha = 0.10)
    })
    
    # ===== ÉTAPE 6 : CONSTRUCTION DU TABLEAU DE RÉSULTATS =====
    # Fusion des statistiques descriptives et des groupes homogènes
    result_df <- snk_result$groups %>%
      as.data.frame() %>%                             # Conversion en data.frame
      rownames_to_column("ProductName") %>%          # Noms des produits en colonne
      rename(Classe = groups) %>%                     # Renommage des groupes
      select(ProductName, Classe) %>%                 # Sélection des colonnes utiles
      left_join(stats_df, by = "ProductName")        # Jointure avec les statistiques
    
    # ===== ÉTAPE 7 : ENRICHISSEMENT ET FORMATAGE FINAL =====
    # Ajout des métadonnées et métriques d'analyse
    result_df <- result_df %>%
      mutate(
        # IDENTIFICATION
        IDTest = trial_name,                          # Identifiant du test (variable globale)
        Segment = paste(segment$AttributeName[1],     # Description du segment
                        segment$NomFonction[1], sep = " - "),
        `ID segment` = segment_index,                 # Numéro de segment
        
        # MÉTRIQUES STATISTIQUES
        P_value_produit = round(p_value_produit, 4),  # P-value formatée
        Significatif_10pct = significatif_10pct,      # Test de significativité
        TestType = "Standard"                         # Type de test (vs Triangulaire)
      ) %>%
      
      # RÉORGANISATION DES COLONNES POUR LA LISIBILITÉ
      select(
        IDTest,                                       # Identifiant
        Segment,                                      # Description
        `ID segment`,                                 # Numéro
        Product = ProductName,                        # Nom du produit
        Classe,                                       # Groupe homogène (a, b, c...)
        Mean,                                         # Moyenne
        Sd,                                          # Écart-type
        n,                                           # Effectif
        P_value_produit,                             # Significativité globale
        Significatif_10pct,                          # Test à 10%
        TestType                                     # Type d'analyse
      )
    
    return(result_df)
    
  }, error = function(e) {
    # ===== GESTION D'ERREUR ROBUSTE =====
    # Logging détaillé pour le debugging
    warning("Erreur analyse produits: ", e$message,
            " dans ", segment$AttributeName[1], " - ", segment$NomFonction[1])
    return(NULL)  # Retour gracieux en cas d'échec
  })
}

# ===== PROGRAMME PRINCIPAL MODIFIÉ AVEC TRACKING =====
# Ce programme orchestre le traitement batch des fichiers Excel avec suivi intelligent
# Il évite le retraitement des fichiers déjà analysés et maintient un état persistant

# ===== ÉTAPE 1 : DÉCOUVERTE DES FICHIERS EXCEL =====
# Recherche récursive dans l'arborescence des données brutes
excel_files <- dir_ls(raw_data_dir,                    # Répertoire racine des données
                      regexp = "\\.xlsx$",             # Filtre : fichiers Excel uniquement
                      ignore.case = TRUE,              # Insensible à la casse (.XLSX, .xlsx)
                      recurse = TRUE) %>%              # Recherche dans les sous-dossiers
  as.character()                                       # Conversion en vecteur de chaînes

message("Fichiers Excel détectés: ", length(excel_files))

# ===== ÉTAPE 2 : INITIALISATION DE L'ÉTAT PERSISTANT =====
# Récupération des données existantes pour éviter la duplication de traitement
all_results <- existing_data$results                   # Résultats d'analyses précédentes
judge_removal_info <- existing_data$judge_info         # Historique des filtrages de juges
all_raw_data <- existing_data$raw_data                 # Données brutes consolidées
data_issues_log <- list()                              # Log des problèmes rencontrés

# ===== ÉTAPE 3 : INITIALISATION DES COMPTEURS DE SUIVI =====
# Métriques pour le monitoring du processus batch
files_processed <- 0                                   # Total des fichiers traités
files_skipped <- 0                                     # Fichiers ignorés (déjà traités)
files_new <- 0                                         # Nouveaux fichiers analysés

# ===== ÉTAPE 4 : BOUCLE PRINCIPALE DE TRAITEMENT =====
# Traitement intelligent avec vérification du statut de chaque fichier
for (file_path in excel_files) {
  file_basename <- basename(file_path)                 # Nom du fichier pour affichage
  
  # ===== VÉRIFICATION DU TRACKING : ÉVITER LE RETRAITEMENT =====
  # Consultation de la base de tracking pour déterminer si le fichier a déjà été analysé
  if(is_file_already_processed(file_path, tracking_data)) {
    message("\n=== FICHIER DÉJÀ TRAITÉ (SKIP): ", file_basename, " ===")
    files_skipped <- files_skipped + 1               # Incrémentation du compteur
    next                                              # Passage au fichier suivant
  }
  
  # ===== TRAITEMENT D'UN NOUVEAU FICHIER =====
  message("\n=== TRAITEMENT NOUVEAU FICHIER: ", file_basename, " ===")
  files_new <- files_new + 1                         # Incrémentation des nouveaux fichiers
  
  # ===== ÉTAPE 4.1 : VÉRIFICATION PRÉALABLE DES ONGLETS =====
  # Validation de la structure du fichier Excel avant traitement
  sheet_names <- tryCatch({
    excel_sheets(file_path)                           # Lecture de la liste des onglets
  }, error = function(e) {
    # GESTION D'ERREUR : Fichier corrompu ou inaccessible
    log_probleme("ERREUR_LECTURE", 
                 paste("Impossible de lire les onglets:", e$message), 
                 file_path)
    
    # MISE À JOUR DU TRACKING : Marquage comme erreur
    tracking_data <<- update_tracking(file_path, "ERREUR_LECTURE", NA, tracking_data)
    return(NULL)                                      # Retour NULL pour signaler l'échec
  })
  
  # VALIDATION : Vérification de la réussite de la lecture
  if(is.null(sheet_names)) next                       # Passage au fichier suivant si échec
  
  # ===== ÉTAPE 4.2 : VALIDATION DE LA STRUCTURE REQUISE =====
  # Vérification de la présence de l'onglet obligatoire "Results"
  if(!"Results" %in% sheet_names) {
    # LOGGING : Enregistrement du problème structurel
    log_probleme("ONGLET_MANQUANT", "Onglet 'Results' manquant", file_path)
    
    # TRACKING : Mise à jour du statut d'erreur
    tracking_data <- update_tracking(file_path, "ONGLET_MANQUANT", NA, tracking_data)
    next                                              # Passage au fichier suivant
  }
  

# ===== SECTION 1.2-3 : LECTURE ET VALIDATION DES DONNÉES =====
# Cette section gère la lecture sécurisée des données Excel et leur validation
# Elle inclut des contrôles de cohérence et la préparation pour l'analyse

# ===== ÉTAPE 1.2 : LECTURE SÉCURISÉE DE L'ONGLET "RESULTS" =====
# Lecture des données avec gestion d'erreur et enrichissement métadonnées
file_data <- tryCatch({
  read_excel(file_path, sheet = "Results") %>%        # Lecture de l'onglet spécifique
    mutate(SourceFile = basename(file_path))          # Ajout du nom de fichier source
}, error = function(e) {
  # GESTION D'ERREUR : Problème de lecture des données
  log_probleme("ERREUR_LECTURE_RESULTS", 
               paste("Erreur lecture Results:", e$message), 
               file_path)
  
  # MISE À JOUR DU TRACKING : Marquage de l'échec
  tracking_data <<- update_tracking(file_path, "ERREUR_LECTURE_RESULTS", NA, tracking_data)
  return(NULL)                                        # Retour NULL pour signaler l'échec
})

# VALIDATION : Vérification de la réussite de la lecture
if(is.null(file_data)) next                          # Passage au fichier suivant si échec

# ===== ÉTAPE 1.3 : VALIDATION DE L'UNICITÉ DU TRIAL =====
# Contrôle critique : Un fichier = Un seul essai (TrialName unique)
tryCatch({
  # COMPTAGE DES ESSAIS DISTINCTS
  n_trials <- n_distinct(file_data$TrialName)        # Nombre d'essais uniques
  
  if (n_trials != 1) {
    # PROBLÈME DÉTECTÉ : Fichier multi-essais (structure invalide)
    issue_msg <- paste("MULTIPLE TRIALNAMES (", n_trials, 
                       ") | Fichier:", basename(file_path))
    
    # LOGGING : Enregistrement du problème
    data_issues_log[[length(data_issues_log) + 1]] <- issue_msg
    
    # TRACKING : Mise à jour avec le statut d'erreur
    tracking_data <- update_tracking(file_path, "MULTIPLE_TRIALNAMES", 
                                     nrow(file_data), tracking_data)
    next                                              # Abandon de ce fichier
  } else {
    # SUCCÈS : Extraction du nom d'essai unique
    trial_name <- unique(file_data$TrialName)         # Variable globale pour analyses
  }
  
  # ===== VALIDATION DE LA COHÉRENCE DES DONNÉES =====
  # Contrôles additionnels de qualité des données
  consistency_issues <- validate_data_consistency(file_data)
  
  if(length(consistency_issues) > 0) {
    # LOGGING : Enregistrement de tous les problèmes de cohérence
    walk(consistency_issues, ~log_probleme("COHERENCE", .x, file_path))
  }
  
  # ===== ÉTAPE 2 : PRÉPARATION ET NETTOYAGE DES DONNÉES =====
  # Transformation des données pour l'analyse statistique
  df <- file_data %>%
    # CONVERSION NUMÉRIQUE : Gestion des décimales avec virgule européenne
    mutate(Value = suppressWarnings(as.numeric(gsub(",", ".", Value)))) %>%
    
    # NETTOYAGE : Suppression des colonnes inutiles
    select(-any_of("NR")) %>%                         # Suppression colonne "NR" si présente
    
    # INITIALISATION : Statut des juges (avant filtrage éventuel)
    mutate(JudgeStatus = "conserved")                 # Tous conservés par défaut
  
  # ===== STOCKAGE DES DONNÉES BRUTES =====
  # Archivage pour traçabilité et debugging
  all_raw_data[[basename(file_path)]] <- df          # Stockage indexé par nom de fichier
  
  # ===== ÉTAPE 3 : SEGMENTATION POUR ANALYSE =====
  # Division des données en segments d'analyse (AttributeName × NomFonction)
  segments <- df %>%
    group_by(AttributeName, NomFonction) %>%         # Groupement double critère
    group_split()                                     # Division en liste de data.frames
  
  message("Nombre de segments dans ce fichier: ", length(segments))
  
}, error = function(e) {
  # GESTION D'ERREUR GLOBALE : Capture des erreurs non anticipées
  log_probleme("ERREUR_TRAITEMENT", paste("Erreur générale:", e$message), file_path)
  tracking_data <- update_tracking(file_path, "ERREUR_TRAITEMENT", NA, tracking_data)
  next
})

# ===== SECTION 4-6 : VÉRIFICATION ET TRAITEMENT DES SEGMENTS =====
# Cette section orchestre la validation, le filtrage et l'analyse des segments de données
# Elle gère différents types de tests avec des logiques spécialisées

# ===== ÉTAPE 4 : FONCTION DE VÉRIFICATION DES SEGMENTS =====
# Validation adaptative selon le type de test (triangulaire vs standard)
verify_segments <- function(segment) {
  # DÉTECTION DU TYPE DE TEST
  nom_fonction <- na.omit(segment$NomFonction)
  is_triangulaire <- length(nom_fonction) > 0 && 
    any(str_detect(nom_fonction, "Triangulaire|triangle"))
  
  if (is_triangulaire) {
    # ===== LOGIQUE SPÉCIALE : TESTS TRIANGULAIRES =====
    # Les tests triangulaires ont des critères de validation différents
    return(list(
      Products = NA_integer_,                         # Pas de notion de produits
      Judges = n_distinct(segment$CJ),               # Nombre de juges participants
      MissingValues = sum(is.na(segment$Value)),     # Valeurs manquantes
      MinJudgesOK = TRUE,                            # Pas de seuil minimum
      MinProductsOK = TRUE,                          # Pas de seuil minimum
      OutOfRange = sum(segment$Value < 0 | segment$Value > 1, na.rm = TRUE) # Échelle 0-1
    ))
  }
  
  # ===== LOGIQUE STANDARD : TESTS CLASSIQUES =====
  # Calculs des métriques de base
  n_products <- n_distinct(segment$ProductName)      # Nombre de produits testés
  n_judges <- n_distinct(segment$CJ)                 # Nombre de juges participants
  non_numeric <- suppressWarnings(                   # Valeurs non-numériques
    sum(is.na(as.numeric(segment$Value)) & !is.na(segment$Value))
  )
  
  # RETOUR DES MÉTRIQUES COMPLÈTES
  list(
    Products = n_products,
    Judges = n_judges,
    MissingValues = sum(is.na(segment$Value)),
    NonNumericValues = non_numeric,
    ValuesOver10 = sum(segment$Value > 10, na.rm = TRUE),    # Échelle attendue 0-10
    NegativeValues = sum(segment$Value < 0, na.rm = TRUE),
    OutOfRange = sum(segment$Value < 0 | segment$Value > 10, na.rm = TRUE),
    MinJudgesOK = n_judges >= 3,                     # Seuil minimum : 3 juges
    MinProductsOK = n_products >= 2                  # Seuil minimum : 2 produits
  )
}

# ===== APPLICATION DE LA VÉRIFICATION À TOUS LES SEGMENTS =====
verification_results <- map(segments, verify_segments)

# ===== LOGGING INTELLIGENT DES PROBLÈMES DÉTECTÉS =====
# Journalisation systématique des violations de critères
for(i in seq_along(verification_results)) {
  res <- verification_results[[i]]
  seg <- segments[[i]]
  seg_name <- paste(seg$AttributeName[1], seg$NomFonction[1], sep = " - ")
  
  # CONTRÔLE : Nombre insuffisant de juges
  if(!isTRUE(res$MinJudgesOK)) {
    issue_msg <- paste("TROP PEU DE JUGES (", res$Judges, "/3) | Segment:", seg_name)
    data_issues_log[[length(data_issues_log) + 1]] <- issue_msg
  }
  
  # CONTRÔLE : Nombre insuffisant de produits
  if(!isTRUE(res$MinProductsOK)) {
    issue_msg <- paste("TROP PEU DE PRODUITS (", res$Products, "/2) | Segment:", seg_name)
    data_issues_log[[length(data_issues_log) + 1]] <- issue_msg
  }
  
  # CONTRÔLE : Valeurs hors limites acceptables
  if(res$OutOfRange > 0) {
    issue_msg <- paste("VALEURS HORS LIMITES (", res$OutOfRange, ") | Segment:", seg_name)
    data_issues_log[[length(data_issues_log) + 1]] <- issue_msg
  }
}

# ===== ÉTAPE 5 : TRAITEMENT ADAPTATIF DES SEGMENTS =====
# Application de logiques de traitement spécialisées selon le type de test
segments_processed <- list()

for (i in seq_along(segments)) {
  seg_name <- paste(segments[[i]]$AttributeName[1], segments[[i]]$NomFonction[1], sep = " - ")
  seg_verif <- verification_results[[i]]
  
  # ===== DÉTECTION DU TYPE DE TEST =====
  is_triangular <- !is.na(segments[[i]]$NomFonction[1]) && 
    str_detect(segments[[i]]$NomFonction[1], "Triangulaire|triangle")
  
  # ===== FILTRAGE : EXCLUSION DES SEGMENTS INVALIDES =====
  # Les tests triangulaires sont toujours conservés
  if(!isTRUE(is_triangular) && 
     (!isTRUE(seg_verif$MinProductsOK) || !isTRUE(seg_verif$MinJudgesOK))) {
    next  # Abandon du segment si critères non respectés
  }
  
  # ===== ROUTAGE VERS LE TRAITEMENT APPROPRIÉ =====
  if (is_triangular) {
    # TRAITEMENT TRIANGULAIRE : Pas de filtrage de juges
    message("Application test TRIANGULAIRE: ", seg_name)
    result <- list(
      segment = segments[[i]],
      removed_judges = character(0),              # Aucun juge retiré
      n_initial = n_distinct(segments[[i]]$CJ),
      n_final = n_distinct(segments[[i]]$CJ)
    )
    
  } else if (str_detect(seg_name, regex("prox", ignore_case = TRUE))) {
    # TRAITEMENT PROXIMITÉ : Logique spécialisée
    message("Application test PROXIMITE: ", seg_name)
    result <- handle_proximity_test(segments[[i]])
    
  } else if (str_detect(seg_name, regex("odeur corporell", ignore_case = TRUE))) {
    # TRAITEMENT ODEUR CORPORELLE : Analyse itérative spéciale
    message("Application test MO (odeur corporell): ", seg_name)
    result <- analyze_judges_iterative(segments[[i]])
    
  } else {
    # TRAITEMENT STANDARD : Analyse itérative classique
    message("Application test STANDARD: ", seg_name)
    result <- analyze_judges_iterative(segments[[i]])
  }
  
  # ===== STOCKAGE DU SEGMENT TRAITÉ =====
  segments_processed[[i]] <- result$segment
  
  # ===== LOGGING DES JUGES RETIRÉS =====
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
  
  # AFFICHAGE DU RÉSUMÉ DE TRAITEMENT
  message("Juges initiaux: ", result$n_initial,
          " | Juges finaux: ", result$n_final,
          " | Juges retirés: ", ifelse(length(result$removed_judges) > 0,
                                       paste(result$removed_judges, collapse = ", "), "aucun"))
}

# ===== CONSOLIDATION DES SEGMENTS TRAITÉS =====
final_data <- bind_rows(segments_processed)

# ===== ÉTAPE 6 : ANALYSE DIFFÉRENTIELLE DES PRODUITS =====
# Traitement séparé pour les tests triangulaires et standards

# TRAITEMENT SPÉCIALISÉ : Tests triangulaires
triangular_results <- process_triangular_segments(segments_processed, file_path)

# IDENTIFICATION DES SEGMENTS NON-TRIANGULAIRES
non_triangular_segments <- segments_processed[!sapply(segments_processed, function(seg) {
  !is.na(seg$NomFonction[1]) && str_detect(seg$NomFonction[1], "Triangulaire|triangle")
})]

# TRAITEMENT STANDARD : Analyse produits classique
standard_results <- map2(non_triangular_segments, seq_along(non_triangular_segments), 
                         ~analyze_products(.x, .y, file_path)) %>%
  compact() %>%                                   # Suppression des résultats NULL
  bind_rows()

# ===== CONSOLIDATION FINALE DES RÉSULTATS =====
file_results <- bind_rows(
  triangular_results,
  standard_results
)

# ===== STOCKAGE ET AFFICHAGE =====
if (nrow(file_results) > 0) {
  all_results[[basename(file_path)]] <- file_results
  print(file_results)                             # Affichage pour monitoring
}

# ===== SECTION 7-8 : FINALISATION ET GÉNÉRATION DES SORTIES =====
# Cette section gère la mise à jour des statuts, la génération des fichiers de sortie
# et la gestion optimisée de la mémoire avec sauvegarde du tracking

# ===== ÉTAPE 7 : MISE À JOUR DU STATUT DES JUGES =====
# Enrichissement des données brutes avec les informations de filtrage
current_file <- as.character(file_path)

# INITIALISATION : Statut par défaut pour tous les juges
raw_data_with_status <- file_data %>%
  mutate(
    JudgeStatus = "conserved",                    # Statut initial : tous conservés
    SourceFile = basename(current_file)          # Traçabilité du fichier source
  )

# EXTRACTION : Changements de juges pour le fichier courant
file_judge_changes <- judge_removal_info %>% 
  keep(~ .x$File == basename(current_file)) %>%  # Filtrage par fichier
  map_df(~ .x)                                   # Conversion en data.frame

# ===== MISE À JOUR CONDITIONNELLE DES STATUTS =====
if(nrow(file_judge_changes) > 0) {
  raw_data_with_status <- raw_data_with_status %>% 
    mutate(JudgeStatus = case_when(
      # CONDITION COMPLEXE : Juge retiré ET segment correspondant
      CJ %in% unlist(strsplit(file_judge_changes$RemovedJudges, ", ")) &
        paste(AttributeName, NomFonction, sep = " - ") %in% file_judge_changes$Segment ~ "removed",
      TRUE ~ JudgeStatus                          # Sinon, conserver le statut actuel
    ))
}

# ===== ÉTAPE 8 : GÉNÉRATION DES FICHIERS DE SORTIE INDIVIDUELS =====
# Création sécurisée de la structure de répertoires
output_dir <- file.path(output_base_dir, "Fichiers_Individuels")
output_name <- tools::file_path_sans_ext(basename(current_file))

# CRÉATION SÉCURISÉE DU RÉPERTOIRE DE SORTIE
if(!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE, showWarnings = TRUE)
  
  # VALIDATION : Vérification de la création réussie
  if(!dir.exists(output_dir)) {
    log_probleme("ERREUR_SORTIE", paste("Échec création répertoire:", output_dir), file_path)
    tracking_data <- update_tracking(file_path, "ERREUR_CREATION_DOSSIER", nrow(file_data), tracking_data)
    next  # Abandon du fichier courant
  }
}

# ===== GÉNÉRATION ROBUSTE DU FICHIER EXCEL =====
tryCatch({
  # STRUCTURATION DES DONNÉES DE SORTIE
  output_data <- list(
    # ONGLET 1 : Données brutes enrichies
    "DonneesBrutes" = raw_data_with_status,
    
    # ONGLET 2 : Résultats d'analyse
    "Resultats" = file_results,
    
    # ONGLET 3 : Journal des opérations
    "Logs" = tibble(
      Date = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      Fichier = basename(file_path),
      Message = if(length(data_issues_log) > 0) 
        unlist(data_issues_log) 
      else 
        "Aucun problème détecté"
    )
  )
  
  # DÉFINITION DU CHEMIN DE SORTIE
  output_path <- file.path(output_dir, paste0(output_name, "_RESULTATS.xlsx"))
  
  # ===== GESTION DES FICHIERS EXISTANTS =====
  # Création automatique de backup avec horodatage
  if(file.exists(output_path)) {
    backup_path <- paste0(output_path, ".backup_", format(Sys.time(), "%H%M%S"))
    file.rename(output_path, backup_path)
    message("Backup existant : ", backup_path)
  }
  
  # ===== VALIDATIONS PRÉ-ÉCRITURE =====
  # Contrôle d'intégrité des données
  if(any(sapply(output_data, is.null))) {
    stop("Données de sortie corrompues - éléments NULL détectés")
  }
  
  # Gestion des résultats vides
  if(nrow(output_data$Resultats) == 0) {
    log_probleme("ALERTE", "Aucun résultat à exporter", file_path)
    output_data$Resultats <- tibble(Statut = "Aucun résultat généré")
  }
  
  # ===== OPTIMISATIONS EXCEL =====
  # Limitation du nombre de lignes (contrainte Excel)
  excel_row_limit <- 1048576
  output_data <- lapply(output_data, function(df) {
    if(nrow(df) > excel_row_limit) {
      log_probleme("TRONCATURE", 
                   paste("Dépassement limite Excel (1,048,576 lignes) - Troncation à", excel_row_limit),
                   file_path)
      head(df, excel_row_limit)
    } else {
      df
    }
  })
  
  # ===== NETTOYAGE POUR COMPATIBILITÉ EXCEL =====
  output_data <- lapply(output_data, function(df) {
    # NETTOYAGE DES NOMS DE COLONNES
    names(df) <- iconv(names(df), to = "ASCII//TRANSLIT") %>%  # Suppression accents
      str_remove_all("[^[:alnum:]_]") %>%                      # Caractères alphanumériques uniquement
      str_to_upper()                                           # Uniformisation en majuscules
    
    # LIMITATION DES CHAÎNES DE CARACTÈRES (limite Excel : 32,767 caractères)
    df %>% 
      mutate(across(where(is.character), ~str_sub(., 1, 32767)))
  })
  
  # ===== ÉCRITURE FINALE DU FICHIER EXCEL =====
  write_xlsx(
    output_data,
    path = output_path,
    col_names = TRUE,                             # Inclusion des en-têtes
    format_headers = TRUE                         # Formatage des en-têtes
  )
  
  # ===== VALIDATION POST-ÉCRITURE =====
  if(file.exists(output_path)) {
    message("Fichier généré : ", output_path)
    # MISE À JOUR DU TRACKING - SUCCÈS
    tracking_data <- update_tracking(file_path, "SUCCES", nrow(file_data), tracking_data)
    files_processed <- files_processed + 1       # Incrémentation compteur global
  } else {
    stop("Échec écriture sans message d'erreur")
  }
  
}, error = function(e) {
  # GESTION D'ERREUR : Problèmes de génération de fichier
  log_probleme("ERREUR_SORTIE", paste("Erreur génération fichier :", e$message), file_path)
  tracking_data <- update_tracking(file_path, "ERREUR_SORTIE", nrow(file_data), tracking_data)
})

# ===== OPTIMISATION MÉMOIRE =====
# Nettoyage explicite des variables volumineuses
rm(list = c("output_data", "raw_data_with_status", "file_results"))
gc()                                              # Garbage collection forcé

}

# ===== SAUVEGARDE FINALE DU TRACKING =====
# Persistance des données de suivi après traitement complet
save_tracking_data(tracking_data)

# ===== GÉNÉRATION DES RAPPORTS CONSOLIDÉS =====
message("\n=== GÉNÉRATION DES RAPPORTS CONSOLIDÉS ===")

# ===== SECTION FINALE : CONSOLIDATION ET RAPPORT GLOBAL =====
# Cette section agrège tous les résultats, génère les statistiques globales
# et produit un rapport consolidé complet avec tracking intégré

# ===== ÉTAPE 1 : CONSOLIDATION DE TOUS LES RÉSULTATS =====
# Agrégation de tous les résultats individuels avec identification de source
consolidated_results <- bind_rows(all_results, .id = "SourceFile")

# ===== ÉTAPE 2 : CRÉATION DE LA TABLE DE TRACKING DES JUGES =====
# Construction d'une vue globale des modifications de juges
all_judge_info_df <- bind_rows(judge_removal_info)      # Agrégation des infos de retrait
all_raw_data_df <- bind_rows(all_raw_data)              # Agrégation des données brutes
judge_tracking_table <- create_judge_tracking_table(all_judge_info_df, all_raw_data_df)

# ===== ÉTAPE 3 : GÉNÉRATION DES STATISTIQUES GLOBALES =====
# Calculs robustes avec vérification de l'existence des colonnes
global_stats <- tibble(
  Date_Analyse = Sys.Date(),
  
  # ===== MÉTRIQUES DE TRAITEMENT =====
  Nb_Fichiers_Detectes = length(excel_files),
  Nb_Fichiers_Deja_Traites = files_skipped,          # Nouveauté : tracking des skips
  Nb_Fichiers_Nouveaux = files_new,                  # Nouveauté : tracking des nouveaux
  Nb_Fichiers_Succes = files_processed,
  Nb_Tests_Total = nrow(consolidated_results),
  
  # ===== CALCULS SÉCURISÉS AVEC VÉRIFICATION DES COLONNES =====
  # Gestion adaptative des différentes structures de données
  Nb_Tests_Triangulaires = if("Type" %in% names(consolidated_results)) {
    sum(consolidated_results$Type == "Triangulaire", na.rm = TRUE)
  } else if("TestType" %in% names(consolidated_results)) {
    sum(consolidated_results$TestType == "Triangulaire", na.rm = TRUE)
  } else { 0 },
  
  Nb_Tests_Standard = if("TestType" %in% names(consolidated_results)) {
    sum(consolidated_results$TestType == "Standard", na.rm = TRUE)
  } else { nrow(consolidated_results) },              # Fallback : tous les tests
  
  Nb_Tests_Significatifs_10pct = if("Significatif_10pct" %in% names(consolidated_results)) {
    sum(consolidated_results$Significatif_10pct == TRUE, na.rm = TRUE)
  } else { NA_integer_ },                             # Information non disponible
  
  # ===== MÉTRIQUES DES JUGES =====
  Nb_Juges_Total = n_distinct(all_raw_data_df$CJ),
  
  # CALCUL COMPLEXE : Nombre de juges uniques retirés
  Nb_Juges_Retires = if(nrow(all_judge_info_df) > 0 && "RemovedJudges" %in% names(all_judge_info_df)) {
    removed_judges <- all_judge_info_df$RemovedJudges[
      all_judge_info_df$RemovedJudges != "" & !is.na(all_judge_info_df$RemovedJudges)
    ]
    length(unique(unlist(strsplit(removed_judges, ", "))))
  } else { 0 },
  
  # ===== INDICATEUR DE PERFORMANCE =====
  Taux_Reussite_Nouveaux = if(files_new > 0) {
    round(files_processed / files_new, 3)
  } else { 1.000 }                                    # 100% si aucun nouveau fichier
)

# ===== ÉTAPE 4 : RAPPORT STRUCTURÉ DES PROBLÈMES =====
# Extraction et catégorisation des problèmes détectés
problems_report <- tibble(
  Date = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
  
  # EXTRACTION DU TYPE DE PROBLÈME (entre crochets)
  Type_Probleme = map_chr(data_issues_log, 
                          ~str_extract(.x, "\\[([^\\]]+)\\]") %>% str_remove_all("\\[|\\]")),
  
  # EXTRACTION DE LA DESCRIPTION (après les crochets)
  Description = map_chr(data_issues_log, 
                        ~str_remove(.x, "^\\[[^\\]]+\\]\\s*")),
  
  # EXTRACTION DU FICHIER SOURCE
  Fichier = map_chr(data_issues_log, 
                    ~str_extract(.x, "Fichier: (.+)$") %>% str_remove("Fichier: "))
) %>%
  filter(!is.na(Type_Probleme))                       # Suppression des entrées malformées

# ===== ÉTAPE 5 : EXPORT DU RAPPORT CONSOLIDÉ =====
# Création sécurisée du répertoire de sortie
consolidated_output_dir <- file.path(output_base_dir, "RAPPORT_CONSOLIDE")
if(!dir.exists(consolidated_output_dir)) {
  dir.create(consolidated_output_dir, recursive = TRUE)
}

# Génération du nom de fichier avec horodatage
consolidated_output_path <- file.path(
  consolidated_output_dir, 
  paste0("RAPPORT_SENSO_CONSOLIDE_", format(Sys.Date(), "%Y%m%d"), ".xlsx")
)

# ===== GÉNÉRATION ROBUSTE DU RAPPORT EXCEL =====
tryCatch({
  # STRUCTURATION COMPLÈTE DES DONNÉES DE SORTIE
  consolidated_output <- list(
    # ONGLET 1 : Tous les résultats d'analyse
    "Resultats_Tous" = consolidated_results,
    
    # ONGLET 2 : Statistiques de synthèse
    "Statistiques_Globales" = global_stats,
    
    # ONGLET 3 : Suivi détaillé des juges
    "Tracking_Juges" = judge_tracking_table,
    
    # ONGLET 4 : NOUVEAUTÉ - Tracking des fichiers
    "Tracking_Fichiers" = tracking_data,
    
    # ONGLET 5 : Données brutes consolidées
    "Donnees_Brutes_Toutes" = all_raw_data_df,
    
    # ONGLET 6 : Rapport des problèmes
    "Rapport_Problemes" = problems_report,
    
    # ONGLET 7 : Résumé d'exécution
    "Resume_Execution" = tibble(
      Date_Execution = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      Repertoire_Source = raw_data_dir,
      Nb_Fichiers_Detectes = length(excel_files),
      Nb_Fichiers_Skipped = files_skipped,
      Nb_Fichiers_Nouveaux = files_new,
      Nb_Fichiers_Traites_Succes = files_processed,
      Fichiers_Traites = paste(names(all_results), collapse = "; "),
      Version_Script = "2025-08-05_AVEC_TRACKING"     # Versioning du script
    )
  )
  
  # ===== NETTOYAGE SÉCURISÉ POUR COMPATIBILITÉ EXCEL =====
  consolidated_output <- lapply(consolidated_output, function(df) {
    # VÉRIFICATION DE LA VALIDITÉ DES DONNÉES
    if(is.data.frame(df) && nrow(df) > 0 && ncol(df) > 0) {
      
      # NETTOYAGE DES NOMS DE COLONNES
      original_names <- names(df)
      clean_names <- iconv(original_names, to = "ASCII//TRANSLIT") %>%  # Suppression accents
        str_remove_all("[^[:alnum:]_]") %>%                            # Caractères autorisés
        str_to_upper()                                                 # Uniformisation
      
      # GESTION DES DOUBLONS avec make.unique
      clean_names <- make.unique(clean_names, sep = "_")
      names(df) <- clean_names
      
      # LIMITATION DES CHAÎNES (contrainte Excel : 32,767 caractères)
      df %>% 
        mutate(across(where(is.character), ~str_sub(., 1, 32767)))
      
    } else if(is.data.frame(df)) {
      # Data frame vide - conservation tel quel
      df
    } else {
      # FORMAT INCORRECT - Création d'un tibble informatif
      tibble(
        Information = "Données non disponibles ou format incorrect",
        Timestamp = Sys.time()
      )
    }
  })
  
  # ===== VÉRIFICATION FINALE AVANT EXPORT =====
  message("Vérification des données avant export...")
  for(i in seq_along(consolidated_output)) {
    sheet_name <- names(consolidated_output)[i]
    df <- consolidated_output[[i]]
    
    if(is.data.frame(df)) {
      message("  - ", sheet_name, " : ", nrow(df), " lignes, ", ncol(df), " colonnes")
      
      # DÉTECTION ET CORRECTION DES NOMS DUPLIQUÉS
      if(any(duplicated(names(df)))) {
        message("    ATTENTION: Noms de colonnes dupliqués détectés dans ", sheet_name)
        names(df) <- make.unique(names(df), sep = "_")
        consolidated_output[[i]] <- df
      }
    } else {
      message("  - ", sheet_name, " : Format non-standard")
    }
  }
  
  # ===== EXPORT VERS EXCEL =====
  write_xlsx(
    consolidated_output,
    path = consolidated_output_path,
    col_names = TRUE,                                 # Inclusion des en-têtes
    format_headers = TRUE                             # Formatage des en-têtes
  )
  
  message("RAPPORT CONSOLIDÉ GÉNÉRÉ : ", consolidated_output_path)
  
}, error = function(e) {
  # ===== GESTION D'ERREUR AVEC DIAGNOSTIC APPROFONDI =====
  message("ERREUR génération rapport consolidé : ", e$message)
  message("Détails de l'erreur : ", toString(e))
  
  # DIAGNOSTIC AUTOMATIQUE DES DONNÉES
  message("\n=== DIAGNOSTIC DES DONNÉES ===")
  tryCatch({
    for(i in seq_along(consolidated_output)) {
      sheet_name <- names(consolidated_output)[i]
      df <- consolidated_output[[i]]
      
      if(exists("df") && is.data.frame(df)) {
        message("  - ", sheet_name, " : ", class(df)[1], " avec ", nrow(df), " lignes")
        message("    Colonnes : ", paste(names(df)[1:min(5, ncol(df))], collapse = ", "))
        if(ncol(df) > 5) message("    ... et ", ncol(df) - 5, " autres colonnes")
        
        # IDENTIFICATION DES PROBLÈMES DE NOMS
        if(any(duplicated(names(df)))) {
          dup_names <- names(df)[duplicated(names(df))]
          message("    PROBLÈME: Colonnes dupliquées : ", paste(unique(dup_names), collapse = ", "))
        }
      } else {
        message("  - ", sheet_name, " : Problème de format ou données manquantes")
      }
    }
  }, error = function(e2) {
    message("Impossible de diagnostiquer les données : ", e2$message)
  })
})

# ===== RÉSUMÉ FINAL AVEC TRACKING INTÉGRÉ =====
message("\n=== RÉSUMÉ DE L'ANALYSE AVEC TRACKING ===")
message("Fichiers détectés : ", length(excel_files))
message("Fichiers déjà traités (skippés) : ", files_skipped)        # NOUVEAUTÉ
message("Nouveaux fichiers détectés : ", files_new)                 # NOUVEAUTÉ
message("Nouveaux fichiers traités avec succès : ", files_processed)
message("Tests analysés au total : ", nrow(consolidated_results))
message("Juges suivis : ", n_distinct(all_raw_data_df$CJ))
message("Problèmes détectés : ", length(data_issues_log))

# ===== ANALYSE DES PROBLÈMES PAR CATÉGORIE =====
if(length(data_issues_log) > 0) {
  message("\nTypes de problèmes rencontrés :")
  problem_summary <- table(map_chr(data_issues_log, 
                                   ~str_extract(.x, "\\[([^\\]]+)\\]") %>% str_remove_all("\\[|\\]")))
  
  for(i in seq_along(problem_summary)) {
    message("  - ", names(problem_summary)[i], " : ", problem_summary[i], " occurrences")
  }
}

# ===== AFFICHAGE DU STATUT DU TRACKING =====
message("\n=== STATUT DU TRACKING ===")
if(nrow(tracking_data) > 0) {
  tracking_summary <- tracking_data %>%
    count(Statut, name = "Nb_Fichiers") %>%
    arrange(desc(Nb_Fichiers))
  
  for(i in 1:nrow(tracking_summary)) {
    message("  - ", tracking_summary$Statut[i], " : ", tracking_summary$Nb_Fichiers[i], " fichiers")
  }
}

message("\nFichier de tracking sauvegardé : ", tracking_file)
message("Analyse terminée: ", Sys.time())

# ===== NETTOYAGE FINAL OPTIMISÉ =====
# Conservation des objets essentiels uniquement
rm(list = ls()[!ls() %in% c("consolidated_results", "global_stats", "judge_tracking_table", "tracking_data")])
gc()                                                  # Garbage collection final

message("\n=== ANALYSE SENSO AVEC TRACKING TERMINÉE AVEC SUCCÈS ===")
