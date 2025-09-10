import os
import hashlib
from datetime import datetime, date, time as tm, timedelta
import base64
import io
from PIL import Image

import pandas as pd
import psycopg2
from psycopg2 import pool
import streamlit as st
import plotly.express as px

# =========================
# Configuration de la page
# =========================
st.set_page_config(
    page_title="Système de Pointage du Personnel",
    page_icon="⏰",
    layout="wide",
    initial_sidebar_state="expanded",
)

# =========================
# Gestion des connexions avec pool
# =========================
connection_pool = None

def init_connection_pool():
    global connection_pool
    try:
        connection_pool = psycopg2.pool.SimpleConnectionPool(
            1, 20,
            host=st.secrets["postgres"]["host"],
            database=st.secrets["postgres"]["dbname"],
            user=st.secrets["postgres"]["user"],
            password=st.secrets["postgres"]["password"],
            port=st.secrets["postgres"]["port"]
        )
        return True
    except Exception as e:
        st.error(f"Erreur d'initialisation du pool de connexions: {e}")
        return False

def get_connection():
    global connection_pool
    if connection_pool is None:
        if not init_connection_pool():
            return None
    try:
        return connection_pool.getconn()
    except Exception as e:
        st.error(f"Erreur d'obtention de connexion: {e}")
        return None

def return_connection(conn):
    global connection_pool
    if connection_pool and conn:
        connection_pool.putconn(conn)

# =========================
# Paramètres / Sécurité
# =========================
DEFAULT_ADMIN_USER = "admin"
DEFAULT_ADMIN_PASS = "admin123"

# =========================
# Authentification & Utilisateurs
# =========================

def sha256(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()

def create_users_table():
    conn = get_connection()
    if conn is None:
        return False

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS users (
                        id SERIAL PRIMARY KEY,
                        username VARCHAR(50) UNIQUE NOT NULL,
                        password_hash VARCHAR(255) NOT NULL,
                        role VARCHAR(20) DEFAULT 'user',
                        email VARCHAR(100),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )

                # Créer un admin par défaut si absent
                cur.execute("SELECT COUNT(*) FROM users WHERE username = %s", (DEFAULT_ADMIN_USER,))
                exists = cur.fetchone()[0]
                if exists == 0:
                    cur.execute(
                        "INSERT INTO users (username, password_hash, role, email) VALUES (%s, %s, %s, %s)",
                        (
                            DEFAULT_ADMIN_USER,
                            sha256(DEFAULT_ADMIN_PASS),
                            "admin",
                            f"{DEFAULT_ADMIN_USER}@example.com",
                        ),
                    )
        return True
    except Exception as e:
        st.error(f"Erreur création table users: {e}")
        return False
    finally:
        if conn:
            return_connection(conn)

def authenticate_user(username, password):
    conn = get_connection()
    if conn is None:
        return False

    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, username, role FROM users WHERE username = %s AND password_hash = %s",
                (username, sha256(password)),
            )
            user = cur.fetchone()
            return user if user else False
    except Exception as e:
        st.error(f"Erreur authentification: {e}")
        return False
    finally:
        if conn:
            return_connection(conn)

def get_all_users():
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        return pd.read_sql_query(
            "SELECT id, username, role, email, created_at FROM users ORDER BY username",
            conn,
        )
    except Exception as e:
        st.error(f"Erreur récupération utilisateurs: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            return_connection(conn)

def create_user(username, password, role, email):
    conn = get_connection()
    if conn is None:
        return False
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO users (username, password_hash, role, email) VALUES (%s, %s, %s, %s)",
                    (username, sha256(password), role, email),
                )
        return True
    except Exception as e:
        st.error(f"Erreur création utilisateur: {e}")
        return False
    finally:
        if conn:
            return_connection(conn)

# =========================
# Modèle de données
# =========================

def create_tables():
    conn = get_connection()
    if conn is None:
        return False

    try:
        with conn:
            with conn.cursor() as cur:
                # Table personnels
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS personnels (
                        id SERIAL PRIMARY KEY,
                        nom VARCHAR(100) NOT NULL,
                        prenom VARCHAR(100) NOT NULL,
                        service VARCHAR(100) NOT NULL,
                        poste VARCHAR(50) NOT NULL CHECK (poste IN ('Jour', 'Nuit')),
                        heure_entree_prevue TIME NOT NULL,
                        heure_sortie_prevue TIME NOT NULL,
                        actif BOOLEAN DEFAULT TRUE,
                        date_creation TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )

                # Table congés
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS conges (
                        id SERIAL PRIMARY KEY,
                        personnel_id INTEGER REFERENCES personnels(id) ON DELETE CASCADE,
                        date_debut DATE NOT NULL,
                        date_fin DATE NOT NULL,
                        type_conge VARCHAR(50) NOT NULL,
                        motif TEXT,
                        statut VARCHAR(20) DEFAULT 'En attente' CHECK (statut IN ('En attente', 'Approuvé', 'Rejeté')),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )

                # Table pointages
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS pointages (
                        id SERIAL PRIMARY KEY,
                        personnel_id INTEGER REFERENCES personnels(id) ON DELETE CASCADE,
                        date_pointage DATE NOT NULL,
                        heure_arrivee TIME,
                        heure_depart TIME,
                        statut_arrivee VARCHAR(50) DEFAULT 'Present',
                        statut_depart VARCHAR(50) DEFAULT 'Present',
                        retard_minutes INTEGER DEFAULT 0,
                        depart_avance_minutes INTEGER DEFAULT 0,
                        motif_retard TEXT,
                        motif_depart_avance TEXT,
                        notes TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(personnel_id, date_pointage)
                    )
                    """
                )

                # Table retards
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS retards (
                        id SERIAL PRIMARY KEY,
                        personnel_id INTEGER REFERENCES personnels(id) ON DELETE CASCADE,
                        date_retard DATE NOT NULL,
                        retard_minutes INTEGER NOT NULL,
                        motif TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )

                # Table absences
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS absences (
                        id SERIAL PRIMARY KEY,
                        personnel_id INTEGER REFERENCES personnels(id) ON DELETE CASCADE,
                        date_absence DATE NOT NULL,
                        motif TEXT,
                        justifie BOOLEAN DEFAULT FALSE,
                        certificat_justificatif BYTEA,
                        type_certificat VARCHAR(10),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(personnel_id, date_absence)
                    )
                    """
                )

                # Données d'exemple s'il n'y a personne
                cur.execute("SELECT COUNT(*) FROM personnels")
                if cur.fetchone()[0] == 0:
                    cur.execute(
                        """
                        INSERT INTO personnels (nom, prenom, service, poste, heure_entree_prevue, heure_sortie_prevue) VALUES
                        ('Dupont', 'Jean', 'Reception', 'Jour', '08:00:00', '16:00:00'),
                        ('Martin', 'Marie', 'Radiologie', 'Nuit', '20:00:00', '04:00:00'),
                        ('Bernard', 'Pierre', 'Urgence', 'Jour', '07:30:00', '15:30:00'),
                        ('Dubois', 'Sophie', 'Maternité', 'Nuit', '21:00:00', '05:00:00'),
                        ('Moreau', 'Luc', 'Administration', 'Jour', '09:00:00', '17:00:00')
                        """
                    )
        # Crée la table users et l'admin par défaut
        ok = create_users_table()
        return True
    except Exception as e:
        st.error(f"Erreur création tables: {e}")
        return False
    finally:
        if conn:
            return_connection(conn)

# =========================
# Fonctions utilitaires
# =========================

def _as_time(value) -> tm:
    if isinstance(value, tm):
        return value
    s = str(value)
    for fmt in ("%H:%M:%S", "%H:%M:%S.%f", "%H:%M"):
        try:
            return datetime.strptime(s, fmt).time()
        except ValueError:
            continue
    return tm(8, 0)

def get_services_disponibles():
    conn = get_connection()
    if conn is None:
        return []
    try:
        df = pd.read_sql_query("SELECT DISTINCT service FROM personnels WHERE actif = TRUE ORDER BY service", conn)
        return df['service'].tolist()
    except Exception as e:
        st.error(f"Erreur récupération services: {e}")
        return []
    finally:
        if conn:
            return_connection(conn)

def filtrer_personnel(recherche, filtre_service):
    personnel_par_service = get_personnel_par_service()
    result = {}
    
    for service, employes in personnel_par_service.items():
        if filtre_service != "Tous les services" and service != filtre_service:
            continue
            
        employes_filtres = []
        for emp in employes:
            nom_complet = f"{emp['prenom']} {emp['nom']}".lower()
            if not recherche or recherche.lower() in nom_complet or recherche.lower() in emp['service'].lower() or recherche.lower() in emp['poste'].lower():
                employes_filtres.append(emp)
        
        if employes_filtres:
            result[service] = employes_filtres
    
    return result

def get_pointage_employe_jour(personnel_id, date_pointage):
    conn = get_connection()
    if conn is None:
        return None
    try:
        df = pd.read_sql_query(
            """
            SELECT * FROM pointages 
            WHERE personnel_id = %s AND date_pointage = %s
            """,
            conn,
            params=(personnel_id, date_pointage)
        )
        if not df.empty:
            return df.iloc[0]
        return None
    except Exception as e:
        st.error(f"Erreur récupération pointage: {e}")
        return None
    finally:
        if conn:
            return_connection(conn)

# =========================
# Requêtes métier
# =========================

def get_personnel():
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        return pd.read_sql_query(
            "SELECT id, nom, prenom, service, poste, heure_entree_prevue, heure_sortie_prevue, actif FROM personnels ORDER BY nom, prenom",
            conn,
        )
    except Exception as e:
        st.error(f"Erreur récupération personnel: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            return_connection(conn)

def ajouter_personnel(nom, prenom, service, poste, heure_entree_prevue, heure_sortie_prevue):
    conn = get_connection()
    if conn is None:
        return False
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO personnels (nom, prenom, service, poste, heure_entree_prevue, heure_sortie_prevue)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (nom, prenom, service, poste, heure_entree_prevue, heure_sortie_prevue),
                )
        return True
    except Exception as e:
        st.error(f"Erreur ajout personnel: {e}")
        return False
    finally:
        if conn:
            return_connection(conn)

def modifier_personnel(personnel_id, nom, prenom, service, poste, heure_entree_prevue, heure_sortie_prevue, actif):
    conn = get_connection()
    if conn is None:
        return False
    try:
        personnel_id = int(personnel_id)
        
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE personnels 
                    SET nom = %s, prenom = %s, service = %s, poste = %s, 
                        heure_entree_prevue = %s, heure_sortie_prevue = %s, actif = %s
                    WHERE id = %s
                    """,
                    (nom, prenom, service, poste, heure_entree_prevue, heure_sortie_prevue, actif, personnel_id),
                )
        return True
    except Exception as e:
        st.error(f"Erreur modification personnel: {e}")
        return False
    finally:
        if conn:
            return_connection(conn)

def calculer_statut_arrivee(heure_pointage, heure_prevue):
    """
    Calcule le statut de pointage selon les règles spécifiques:
    - Plage normale: 15min avant à 5min avant l'heure prévue (07:45 à 07:55 pour 08:00)
    - En retard: après 5min avant l'heure prévue jusqu'à 29 minutes de retard
    - Absent: 30 minutes ou plus de retard (après 08:30 pour 08:00)
    """
    if not heure_pointage or not heure_prevue:
        return "Non pointé", 0, False
    
    heure_prevue = _as_time(heure_prevue)
    heure_pointage = _as_time(heure_pointage)
    
    # Convertir en datetime pour les calculs
    dt_prevue = datetime.combine(date.today(), heure_prevue)
    dt_pointage = datetime.combine(date.today(), heure_pointage)
    
    # Calcul de la différence en minutes
    difference_minutes = (dt_pointage - dt_prevue).total_seconds() / 60
    
    # Définition des plages horaires spécifiques
    debut_plage = dt_prevue - timedelta(minutes=15)  # 07:45 pour 08:00
    fin_plage = dt_prevue - timedelta(minutes=5)     # 07:55 pour 08:00
    limite_retard = dt_prevue + timedelta(minutes=30) # 08:30 pour 08:00
    
    if debut_plage <= dt_pointage <= fin_plage:
        return "Présent à l'heure", 0, False
    elif fin_plage < dt_pointage < limite_retard:
        retard = (dt_pointage - fin_plage).total_seconds() / 60
        return "En retard", int(retard), False
    elif dt_pointage >= limite_retard:
        return "Absent", 30, True  # Retourne 30 minutes de retard et marque comme absent
    elif dt_pointage < debut_plage:
        avance = (debut_plage - dt_pointage).total_seconds() / 60
        return "En avance", int(-avance), False
    
    return "Non pointé", 0, False

def enregistrer_pointage_arrivee(personnel_id, date_pointage, heure_arrivee, motif_retard=None, notes=None, est_absent=False):
    # Vérifier si l'employé est en congé
    if est_en_conge(personnel_id, date_pointage):
        st.error("❌ Cet employé est en congé aujourd'hui. Pointage impossible.")
        return False, 0
    
    conn = get_connection()
    if conn is None:
        return False, 0
    try:
        personnel_id = int(personnel_id)
        
        with conn:
            with conn.cursor() as cur:
                # Heure prévue
                cur.execute("SELECT heure_entree_prevue FROM personnels WHERE id = %s", (personnel_id,))
                res = cur.fetchone()
                if not res:
                    return False, 0
                heure_prevue = _as_time(res[0])

                # Calcul statut selon les nouvelles règles
                statut_arrivee, retard_minutes, est_absent_calc = calculer_statut_arrivee(heure_arrivee, heure_prevue)
                
                # Si le système détecte une absence (retard >= 30min), enregistrer dans la table absences
                if est_absent or est_absent_calc:
                    cur.execute(
                        """
                        INSERT INTO absences (personnel_id, date_absence, motif, justifie)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (personnel_id, date_absence) DO NOTHING
                        """,
                        (personnel_id, date_pointage, motif_retard or f"Absence automatique (retard de {retard_minutes} minutes)", False)
                    )
                    # Ne pas enregistrer le pointage d'arrivée si absent
                    return True, retard_minutes
                
                # Enregistrer le retard si applicable (seulement si < 30 minutes)
                if retard_minutes > 0 and retard_minutes < 30:
                    cur.execute(
                        """
                        INSERT INTO retards (personnel_id, date_retard, retard_minutes, motif)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                        """,
                        (personnel_id, date_pointage, retard_minutes, motif_retard),
                    )

                # Vérifier si un pointage existe déjà pour cette journée
                cur.execute(
                    "SELECT id FROM pointages WHERE personnel_id = %s AND date_pointage = %s",
                    (personnel_id, date_pointage)
                )
                existing = cur.fetchone()

                arr = _as_time(heure_arrivee)

                if existing:
                    # Mettre à jour l'arrivée
                    cur.execute(
                        """
                        UPDATE pointages 
                        SET heure_arrivee = %s, statut_arrivee = %s, retard_minutes = %s, 
                            motif_retard = %s, notes = COALESCE(%s, notes)
                        WHERE id = %s
                        """,
                        (arr, statut_arrivee, retard_minutes, motif_retard, notes, existing[0])
                    )
                else:
                    # Nouveau pointage
                    cur.execute(
                        """
                        INSERT INTO pointages (personnel_id, date_pointage, heure_arrivee, statut_arrivee, retard_minutes, motif_retard, notes)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (personnel_id, date_pointage, arr, statut_arrivee, retard_minutes, motif_retard, notes),
                    )
        return True, retard_minutes
    except Exception as e:
        st.error(f"Erreur enregistrement pointage arrivée: {e}")
        return False, 0
    finally:
        if conn:
            return_connection(conn)

def est_en_conge(personnel_id, date_check):
    """Vérifie si l'employé est en congé à une date donnée"""
    conn = get_connection()
    if conn is None:
        return False
    
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) FROM conges 
                WHERE personnel_id = %s 
                AND statut = 'Approuvé'
                AND date_debut <= %s 
                AND date_fin >= %s
                """,
                (personnel_id, date_check, date_check)
            )
            count = cur.fetchone()[0]
            return count > 0
    except Exception as e:
        st.error(f"Erreur vérification congé: {e}")
        return False
    finally:
        if conn:
            return_connection(conn)

def enregistrer_pointage_depart(personnel_id, date_pointage, heure_depart, motif_depart_avance=None, notes=None):
    # Vérifier si l'employé est en congé
    if est_en_conge(personnel_id, date_pointage):
        st.error("❌ Cet employé est en congé aujourd'hui. Pointage impossible.")
        return False, 0
    
    conn = get_connection()
    if conn is None:
        return False, 0
    try:
        personnel_id = int(personnel_id)
        
        with conn:
            with conn.cursor() as cur:
                # Heure de sortie prévue
                cur.execute("SELECT heure_sortie_prevue FROM personnels WHERE id = %s", (personnel_id,))
                res = cur.fetchone()
                if not res:
                    return False, 0
                heure_sortie_prevue = _as_time(res[0])

                # Calcul départ en avance
                depart_avance_minutes = 0
                statut_depart = "Present"
                dep = _as_time(heure_depart)
                
                # Calculer la différence en minutes
                delta_minutes = (datetime.combine(date.today(), heure_sortie_prevue) - datetime.combine(date.today(), dep)).total_seconds() / 60
                
                # Départ en avance seulement si plus de 5 minutes
                if delta_minutes > 5:
                    depart_avance_minutes = int(delta_minutes)
                    statut_depart = "Départ anticipé"

                # Vérifier si un pointage existe déjà pour cette journée
                cur.execute(
                    "SELECT id FROM pointages WHERE personnel_id = %s AND date_pointage = %s",
                    (personnel_id, date_pointage)
                )
                existing = cur.fetchone()

                if existing:
                    # Mettre à jour le départ
                    cur.execute(
                        """
                        UPDATE pointages 
                        SET heure_depart = %s, statut_depart = %s, depart_avance_minutes = %s, 
                            motif_depart_avance = %s, notes = COALESCE(%s, notes)
                        WHERE id = %s
                        """,
                        (dep, statut_depart, depart_avance_minutes, motif_depart_avance, notes, existing[0])
                    )
                else:
                    # Nouveau pointage (cas rare où on pointerait le départ sans l'arrivée)
                    cur.execute(
                        """
                        INSERT INTO pointages (personnel_id, date_pointage, heure_depart, statut_depart, depart_avance_minutes, motif_depart_avance, notes)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (personnel_id, date_pointage, dep, statut_depart, depart_avance_minutes, motif_depart_avance, notes),
                    )
        return True, depart_avance_minutes
    except Exception as e:
        st.error(f"Erreur enregistrement pointage départ: {e}")
        return False, 0
    finally:
        if conn:
            return_connection(conn)

def get_pointages_periode(date_debut, date_fin):
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        return pd.read_sql_query(
            """
            SELECT p.nom, p.prenom, p.service, p.poste, p.heure_entree_prevue, p.heure_sortie_prevue,
                   pt.date_pointage, pt.heure_arrivee, pt.heure_depart, pt.statut_arrivee, pt.statut_depart, 
                   pt.retard_minutes, pt.depart_avance_minutes, pt.motif_retard, pt.motif_depart_avance, pt.notes
            FROM pointages pt
            JOIN personnels p ON pt.personnel_id = p.id
            WHERE pt.date_pointage BETWEEN %s AND %s
            ORDER BY pt.date_pointage DESC, p.nom, p.prenom
            """,
            conn,
            params=(date_debut, date_fin),
        )
    except Exception as e:
        st.error(f"Erreur récupération pointages: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            return_connection(conn)

def get_retards_periode(date_debut, date_fin):
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        return pd.read_sql_query(
            """
            SELECT p.nom, p.prenom, p.service, p.poste, p.heure_entree_prevue,
                   r.date_retard, r.retard_minutes, r.motif, r.created_at
            FROM retards r
            JOIN personnels p ON r.personnel_id = p.id
            WHERE r.date_retard BETWEEN %s AND %s
            ORDER BY r.date_retard DESC, r.retard_minutes DESC
            """,
            conn,
            params=(date_debut, date_fin),
        )
    except Exception as e:
        st.error(f"Erreur récupération retards: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            return_connection(conn)

def get_absences_du_jour():
    """Récupère les absences du jour actuel"""
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        return pd.read_sql_query(
            """
            SELECT p.id, p.nom, p.prenom, p.service, p.poste, p.heure_entree_prevue,
                   a.motif, a.justifie, a.created_at
            FROM personnels p
            LEFT JOIN absences a ON p.id = a.personnel_id AND a.date_absence = %s
            WHERE p.actif = TRUE 
            AND p.id NOT IN (
                SELECT personnel_id FROM pointages WHERE date_pointage = %s AND heure_arrivee IS NOT NULL
            )
            AND p.id NOT IN (
                SELECT personnel_id FROM conges 
                WHERE statut = 'Approuvé' 
                AND date_debut <= %s 
                AND date_fin >= %s
            )
            ORDER BY p.nom, p.prenom
            """,
            conn,
            params=(date.today(), date.today(), date.today(), date.today()),
        )
    except Exception as e:
        st.error(f"Erreur récupération absences du jour: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            return_connection(conn)

def get_absences_periode(date_debut, date_fin):
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        return pd.read_sql_query(
            """
            SELECT a.date_absence, p.nom, p.prenom, p.service, p.poste, 
                   p.heure_entree_prevue, a.motif, a.justifie, a.certificat_justificatif IS NOT NULL as has_certificat,
                   a.created_at
            FROM absences a
            JOIN personnels p ON a.personnel_id = p.id
            WHERE a.date_absence BETWEEN %s AND %s
            ORDER BY a.date_absence DESC, p.nom, p.prenom
            """,
            conn,
            params=(date_debut, date_fin),
        )
    except Exception as e:
        st.error(f"Erreur récupération absences: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            return_connection(conn)

def get_stats_mensuelles():
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        return pd.read_sql_query(
            """
            SELECT 
                p.nom, p.prenom, p.service,
                COUNT(pt.id) as jours_presents,
                SUM(CASE WHEN pt.statut_arrivee = 'Retard' THEN 1 ELSE 0 END) as jours_retard,
                SUM(CASE WHEN pt.statut_depart = 'Départ anticipé' THEN 1 ELSE 0 END) as jours_depart_anticipé,
                COALESCE(SUM(pt.retard_minutes),0) as total_retard_minutes,
                COALESCE(SUM(pt.depart_avance_minutes),0) as total_depart_avance_minutes
            FROM personnels p
            LEFT JOIN pointages pt ON p.id = pt.personnel_id 
                AND pt.date_pointage >= DATE_TRUNC('month', CURRENT_DATE)
            WHERE p.actif = TRUE
            GROUP BY p.id, p.nom, p.prenom, p.service
            ORDER BY p.nom, p.prenom
            """,
            conn,
        )
    except Exception as e:
        st.error(f"Erreur stats mensuelles: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            return_connection(conn)

def marquer_absence_automatique():
    conn = get_connection()
    if conn is None:
        return False
    
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT p.id, p.nom, p.prenom, p.heure_entree_prevue
                    FROM personnels p
                    WHERE p.actif = TRUE 
                    AND p.id NOT IN (
                        SELECT personnel_id FROM pointages WHERE date_pointage = %s AND heure_arrivee IS NOT NULL
                    )
                    AND p.id NOT IN (
                        SELECT personnel_id FROM conges 
                        WHERE statut = 'Approuvé' 
                        AND date_debut <= %s 
                        AND date_fin >= %s
                    )
                    """,
                    (date.today(), date.today(), date.today())
                )
                employes_absents = cur.fetchall()
                
                maintenant = datetime.now().time()
                
                for emp in employes_absents:
                    emp_id, nom, prenom, heure_prevue = emp
                    heure_prevue = _as_time(heure_prevue)
                    
                    heure_limite = (datetime.combine(date.today(), heure_prevue) + timedelta(minutes=30)).time()
                    
                    if maintenant > heure_limite:
                        cur.execute(
                            """
                            INSERT INTO absences (personnel_id, date_absence, motif, justifie)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (personnel_id, date_absence) DO NOTHING
                            """,
                            (emp_id, date.today(), "Absence non justifiée (automatique)", False)
                        )
        return True
    except Exception as e:
        st.error(f"Erreur marquage automatique des absences: {e}")
        return False
    finally:
        if conn:
            return_connection(conn)

def get_personnel_par_service():
    conn = get_connection()
    if conn is None:
        return {}
    try:
        df = pd.read_sql_query(
            "SELECT id, nom, prenom, service, poste, heure_entree_prevue, heure_sortie_prevue, actif FROM personnels WHERE actif = TRUE ORDER BY service, nom, prenom",
            conn,
        )
        
        personnel_par_service = {}
        for _, row in df.iterrows():
            service = row['service']
            if service not in personnel_par_service:
                personnel_par_service[service] = []
            personnel_par_service[service].append(row.to_dict())
            
        return personnel_par_service
    except Exception as e:
        st.error(f"Erreur récupération personnel par service: {e}")
        return {}
    finally:
        if conn:
            return_connection(conn)

def get_pointages_du_jour():
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        return pd.read_sql_query(
            """
            SELECT p.id, p.nom, p.prenom, p.service, p.poste, p.heure_entree_prevue, p.heure_sortie_prevue,
                   pt.heure_arrivee, pt.heure_depart, pt.statut_arrivee, pt.statut_depart, 
                   pt.retard_minutes, pt.depart_avance_minutes, pt.motif_retard, pt.motif_depart_avance, pt.notes
            FROM pointages pt
            JOIN personnels p ON pt.personnel_id = p.id
            WHERE pt.date_pointage = %s
            ORDER BY p.service, p.nom, p.prenom
            """,
            conn,
            params=(date.today(),),
        )
    except Exception as e:
        st.error(f"Erreur récupération pointages du jour: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            return_connection(conn)

def enregistrer_absence(personnel_id, date_absence, motif, justifie=False, certificat_file=None):
    conn = get_connection()
    if conn is None:
        return False
    try:
        # Conversion de numpy.int64 en int Python standard
        personnel_id = int(personnel_id) if hasattr(personnel_id, 'item') else int(personnel_id)
        
        with conn:
            with conn.cursor() as cur:
                if certificat_file:
                    # Lire le fichier et déterminer le type
                    file_data = certificat_file.read()
                    file_type = certificat_file.type.split('/')[-1]
                    
                    cur.execute(
                        """
                        INSERT INTO absences (personnel_id, date_absence, motif, justifie, certificat_justificatif, type_certificat)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (personnel_id, date_absence)
                        DO UPDATE SET 
                            motif = EXCLUDED.motif,
                            justifie = EXCLUDED.justifie,
                            certificat_justificatif = EXCLUDED.certificat_justificatif,
                            type_certificat = EXCLUDED.type_certificat
                        """,
                        (personnel_id, date_absence, motif, justifie, file_data, file_type),
                    )
                else:
                    cur.execute(
                        """
                        INSERT INTO absences (personnel_id, date_absence, motif, justifie)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (personnel_id, date_absence)
                        DO UPDATE SET 
                            motif = EXCLUDED.motif,
                            justifie = EXCLUDED.justifie
                        """,
                        (personnel_id, date_absence, motif, justifie),
                    )
        return True
    except Exception as e:
        st.error(f"Erreur enregistrement absence: {e}")
        return False
    finally:
        if conn:
            return_connection(conn)

def get_certificat_absence(absence_id):
    conn = get_connection()
    if conn is None:
        return None, None
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT certificat_justificatif, type_certificat FROM absences WHERE id = %s",
                (absence_id,)
            )
            result = cur.fetchone()
            if result:
                return result[0], result[1]
            return None, None
    except Exception as e:
        st.error(f"Erreur récupération certificat: {e}")
        return None, None
    finally:
        if conn:
            return_connection(conn)

# =========================
# FONCTIONS CONGES
# =========================

def demander_conge(personnel_id, date_debut, date_fin, type_conge, motif):
    """Enregistre une nouvelle demande de congé"""
    conn = get_connection()
    if conn is None:
        return False
    
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO conges (personnel_id, date_debut, date_fin, type_conge, motif)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (personnel_id, date_debut, date_fin, type_conge, motif)
                )
        return True
    except Exception as e:
        st.error(f"Erreur lors de la demande de congé: {e}")
        return False
    finally:
        if conn:
            return_connection(conn)

def get_conges_employe(personnel_id):
    """Récupère tous les congés d'un employé"""
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    
    try:
        return pd.read_sql_query(
            """
            SELECT c.id, c.date_debut, c.date_fin, c.type_conge, c.motif, c.statut, c.created_at
            FROM conges c
            WHERE c.personnel_id = %s
            ORDER BY c.date_debut DESC
            """,
            conn,
            params=(personnel_id,)
        )
    except Exception as e:
        st.error(f"Erreur récupération congés employé: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            return_connection(conn)

def get_tous_les_conges(filtre_statut="Tous"):
    """Récupère tous les congés avec option de filtre par statut"""
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    
    try:
        query = """
            SELECT c.id, p.nom, p.prenom, p.service, c.date_debut, c.date_fin, 
                   c.type_conge, c.motif, c.statut, c.created_at
            FROM conges c
            JOIN personnels p ON c.personnel_id = p.id
        """
        
        params = []
        if filtre_statut != "Tous":
            query += " WHERE c.statut = %s"
            params.append(filtre_statut)
        
        query += " ORDER BY c.created_at DESC"
        
        return pd.read_sql_query(query, conn, params=params)
    except Exception as e:
        st.error(f"Erreur récupération tous les congés: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            return_connection(conn)

def modifier_statut_conge(conge_id, nouveau_statut):
    """Modifie le statut d'une demande de congé"""
    conn = get_connection()
    if conn is None:
        return False
    
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE conges 
                    SET statut = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                    """,
                    (nouveau_statut, conge_id)
                )
        return True
    except Exception as e:
        st.error(f"Erreur modification statut congé: {e}")
        return False
    finally:
        if conn:
            return_connection(conn)

def get_conges_en_cours():
    """Récupère les congés en cours (aujourd'hui dans la période)"""
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    
    try:
        return pd.read_sql_query(
            """
            SELECT p.nom, p.prenom, p.service, c.date_debut, c.date_fin, c.type_conge
            FROM conges c
            JOIN personnels p ON c.personnel_id = p.id
            WHERE c.statut = 'Approuvé'
            AND c.date_debut <= CURRENT_DATE
            AND c.date_fin >= CURRENT_DATE
            ORDER BY p.service, p.nom
            """,
            conn
        )
    except Exception as e:
        st.error(f"Erreur récupération congés en cours: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            return_connection(conn)

def verifier_disponibilite_conge(personnel_id, date_debut, date_fin):
    """Vérifie si l'employé n'a pas déjà des congés qui se chevauchent"""
    conn = get_connection()
    if conn is None:
        return False
    
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) FROM conges 
                WHERE personnel_id = %s 
                AND statut IN ('En attente', 'Approuvé')
                AND (
                    (date_debut BETWEEN %s AND %s) OR
                    (date_fin BETWEEN %s AND %s) OR
                    (date_debut <= %s AND date_fin >= %s)
                )
                """,
                (personnel_id, date_debut, date_fin, date_debu, date_fin, date_debut, date_fin)
            )
            count = cur.fetchone()[0]
            return count == 0
    except Exception as e:
        st.error(f"Erreur vérification disponibilité congé: {e}")
        return False
    finally:
        if conn:
            return_connection(conn)

# =========================
# NOUVELLES FONCTIONS POUR ABSENCES ET RETARDS
# =========================

def show_absences_page():
    st.title("❌ Gestion des Absences")
    
    tab1, tab2 = st.tabs(["Liste des Absences", "Ajouter une Absence"])
    
    with tab1:
        st.subheader("📋 Liste des absences")
        
        col1, col2 = st.columns(2)
        with col1:
            date_debut = st.date_input("Date de début", value=date.today() - timedelta(days=30), key="abs_debut")
        with col2:
            date_fin = st.date_input("Date de fin", value=date.today(), key="abs_fin")
        
        if st.button("🔍 Charger les absences", key="btn_absences"):
            absences_df = get_absences_periode(date_debut, date_fin)
            
            if not absences_df.empty:
                # Filtrer les colonnes pour une meilleure lisibilité
                colonnes_affichees = ['date_absence', 'nom', 'prenom', 'service', 'poste', 'motif', 'justifie', 'has_certificat']
                colonnes_disponibles = [col for col in colonnes_affichees if col in absences_df.columns]
                
                st.dataframe(absences_df[colonnes_disponibles], use_container_width=True)
                
                # Statistiques des absences
                st.subheader("📊 Statistiques des absences")
                col_stat1, col_stat2, col_stat3 = st.columns(3)
                
                with col_stat1:
                    total_absences = len(absences_df)
                    st.metric("Total des absences", total_absences)
                
                with col_stat2:
                    absences_justifiees = len(absences_df[absences_df['justifie'] == True])
                    st.metric("Absences justifiées", absences_justifiees)
                
                with col_stat3:
                    absences_non_justifiees = len(absences_df[absences_df['justifie'] == False])
                    st.metric("Absences non justifiées", absences_non_justifiees)
                
                # Graphique des absences par service
                absences_par_service = absences_df.groupby('service').size().reset_index(name='count')
                if not absences_par_service.empty:
                    fig = px.bar(
                        absences_par_service,
                        x='service',
                        y='count',
                        title="Nombre d'absences par service"
                    )
                    st.plotly_chart(fig)
            else:
                st.info("Aucune absence trouvée pour la période sélectionnée")
    
    with tab2:
        st.subheader("➕ Ajouter une absence")
        
        personnel_df = get_personnel()
        if not personnel_df.empty:
            employe_selection = st.selectbox(
                "Sélectionner un employé",
                personnel_df.apply(lambda x: f"{x['prenom']} {x['nom']} - {x['service']}", axis=1)
            )
            
            if employe_selection:
                selected_index = personnel_df[
                    personnel_df.apply(lambda x: f"{x['prenom']} {x['nom']} - {x['service']}" == employe_selection, axis=1)
                ].index[0]
                
                emp_data = personnel_df.loc[selected_index]
                
                with st.form("ajouter_absence"):
                    col1, col2 = st.columns(2)
                    with col1:
                        date_absence = st.date_input("Date d'absence", value=date.today())
                        justifie = st.checkbox("Absence justifiée")
                    with col2:
                        motif = st.text_area("Motif de l'absence")
                        certificat = st.file_uploader("Certificat médical (si justifiée)", type=['pdf', 'jpg', 'jpeg', 'png'])
                    
                    if st.form_submit_button("✅ Enregistrer l'absence"):
                        if enregistrer_absence(emp_data['id'], date_absence, motif, justifie, certificat):
                            st.success("✅ Absence enregistrée avec succès")
                        else:
                            st.error("❌ Erreur lors de l'enregistrement de l'absence")
        else:
            st.info("Aucun employé disponible")

def show_retards_page():
    st.title("⏰ Gestion des Retards")
    
    tab1, tab2 = st.tabs(["Liste des Retards", "Statistiques des Retards"])
    
    with tab1:
        st.subheader("📋 Liste des retards")
        
        col1, col2 = st.columns(2)
        with col1:
            date_debut = st.date_input("Date de début", value=date.today() - timedelta(days=30), key="retard_debut")
        with col2:
            date_fin = st.date_input("Date de fin", value=date.today(), key="retard_fin")
        
        if st.button("🔍 Charger les retards", key="btn_retards"):
            retards_df = get_retards_periode(date_debut, date_fin)
            
            if not retards_df.empty:
                st.dataframe(retards_df, use_container_width=True)
                
                # Statistiques des retards
                st.subheader("📊 Statistiques des retards")
                col_stat1, col_stat2, col_stat3 = st.columns(3)
                
                with col_stat1:
                    total_retards = len(retards_df)
                    st.metric("Total des retards", total_retards)
                
                with col_stat2:
                    retard_moyen = retards_df['retard_minutes'].mean()
                    st.metric("Retard moyen (min)", f"{retard_moyen:.1f}")
                
                with col_stat3:
                    retard_max = retards_df['retard_minutes'].max()
                    st.metric("Retard maximum (min)", retard_max)
                
                # Top 5 des employés avec le plus de retards
                retards_par_employe = retards_df.groupby(['nom', 'prenom']).agg({
                    'retard_minutes': ['count', 'sum', 'mean']
                }).round(1)
                retards_par_employe.columns = ['nb_retards', 'total_minutes', 'moyenne_minutes']
                retards_par_employe = retards_par_employe.sort_values('nb_retards', ascending=False)
                
                st.subheader("🏆 Top 5 des employés avec le plus de retards")
                st.dataframe(retards_par_employe.head(), use_container_width=True)
                
                # Graphique des retards par service
                retards_par_service = retards_df.groupby('service').agg({
                    'retard_minutes': ['count', 'sum']
                }).round(1)
                retards_par_service.columns = ['nb_retards', 'total_minutes']
                retards_par_service = retards_par_service.sort_values('nb_retards', ascending=False)
                
                if not retards_par_service.empty:
                    fig = px.bar(
                        retards_par_service.reset_index(),
                        x='service',
                        y='nb_retards',
                        title="Nombre de retards par service"
                    )
                    st.plotly_chart(fig)
            else:
                st.info("Aucun retard trouvé pour la période sélectionnée")
    
    with tab2:
        st.subheader("📈 Analyse des retards")
        
        # Charger les données pour l'analyse
        retards_df = get_retards_periode(date.today() - timedelta(days=60), date.today())
        
        if not retards_df.empty:
            # Évolution des retards dans le temps
            retards_par_jour = retards_df.groupby('date_retard').agg({
                'retard_minutes': ['count', 'mean']
            }).round(1)
            retards_par_jour.columns = ['nb_retards', 'moyenne_minutes']
            
            fig = px.line(
                retards_par_jour.reset_index(),
                x='date_retard',
                y='nb_retards',
                title="Évolution du nombre de retards par jour"
            )
            st.plotly_chart(fig)
            
            # Répartition des retards par durée
            fig2 = px.histogram(
                retards_df,
                x='retard_minutes',
                nbins=20,
                title="Répartition des retards par durée (minutes)"
            )
            st.plotly_chart(fig2)
            
            # Retards par jour de la semaine
            retards_df['jour_semaine'] = pd.to_datetime(retards_df['date_retard']).dt.day_name()
            retards_par_jour_semaine = retards_df.groupby('jour_semaine').agg({
                'retard_minutes': ['count', 'mean']
            }).round(1)
            retards_par_jour_semaine.columns = ['nb_retards', 'moyenne_minutes']
            
            # Ordonner les jours de la semaine
            jours_ordre = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            retards_par_jour_semaine = retards_par_jour_semaine.reindex(jours_ordre)
            
            fig3 = px.bar(
                retards_par_jour_semaine.reset_index(),
                x='jour_semaine',
                y='nb_retards',
                title="Nombre de retards par jour de la semaine"
            )
            st.plotly_chart(fig3)
        else:
            st.info("Aucune donnée de retard disponible pour l'analyse")

# =========================
# Interface Streamlit
# =========================

def main():
    # Initialisation
    if not init_connection_pool():
        st.error("❌ Impossible de se connecter à la base de données. Vérifiez la configuration.")
        return
    
    if not create_tables():
        st.error("❌ Erreur lors de l'initialisation des tables.")
        return
    
    # Authentification
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
        st.session_state.user = None
        st.session_state.user_role = None
        st.session_state.user_id = None
    
    if not st.session_state.authenticated:
        show_login()
        return
    
    # Menu principal
    st.sidebar.title(f"👤 {st.session_state.user} ({st.session_state.user_role})")
    
    menu_options = [
        "🏠 Tableau de Bord",
        "⏰ Pointage du Jour", 
        "👥 Gestion du Personnel",
        "📊 Historique des Pointages",
        "📈 Statistiques",
        "📅 Gestion des Congés",
        "❌ Absences",
        "⏰ Retards",
        "👥 Gestion des Utilisateurs"
    ]
    
    if st.session_state.user_role != "admin":
        menu_options.remove("👥 Gestion des Utilisateurs")
        menu_options.remove("👥 Gestion du Personnel")
        menu_options.remove("❌ Absences")
        menu_options.remove("⏰ Retards")
    
    choice = st.sidebar.selectbox("Navigation", menu_options)
    
    if choice == "🏠 Tableau de Bord":
        show_dashboard()
    elif choice == "⏰ Pointage du Jour":
        show_pointage_du_jour()
    elif choice == "👥 Gestion du Personnel":
        show_gestion_personnel()
    elif choice == "📊 Historique des Pointages":
        show_historique_pointages()
    elif choice == "📈 Statistiques":
        show_statistiques()
    elif choice == "📅 Gestion des Congés":
        show_gestion_conges()
    elif choice == "❌ Absences":
        show_absences_page()
    elif choice == "⏰ Retards":
        show_retards_page()
    elif choice == "👥 Gestion des Utilisateurs" and st.session_state.user_role == "admin":
        show_gestion_utilisateurs()
    
    # Bouton de déconnexion
    if st.sidebar.button("🚪 Déconnexion"):
        st.session_state.authenticated = False
        st.session_state.user = None
        st.session_state.user_role = None
        st.session_state.user_id = None
        st.rerun()

def show_login():
    st.title("🔐 Connexion")
    with st.form("login_form"):
        username = st.text_input("Nom d'utilisateur")
        password = st.text_input("Mot de passe", type="password")
        submit = st.form_submit_button("Se connecter")
        
        if submit:
            user = authenticate_user(username, password)
            if user:
                st.session_state.authenticated = True
                st.session_state.user = user[1]  # username
                st.session_state.user_role = user[2]  # role
                st.session_state.user_id = user[0]  # id
                st.rerun()
            else:
                st.error("❌ Identifiants incorrects")

def show_dashboard():
    st.title("🏠 Tableau de Bord")
    
    # Marquage automatique des absences
    if st.button("🔄 Vérifier les absences automatiques"):
        if marquer_absence_automatique():
            st.success("✅ Absences automatiques vérifiées")
        else:
            st.error("❌ Erreur lors de la vérification des absences")
    
    col1, col2, col3, col4 = st.columns(4)
    
    # Statistiques rapides
    personnel_df = get_personnel()
    pointages_du_jour = get_pointages_du_jour()
    absences_du_jour = get_absences_du_jour()
    conges_en_cours = get_conges_en_cours()
    
    with col1:
        st.metric("Total Personnel", len(personnel_df[personnel_df['actif']]))
    with col2:
        st.metric("Pointages Aujourd'hui", len(pointages_du_jour))
    with col3:
        st.metric("Absences Aujourd'hui", len(absences_du_jour))
    with col4:
        st.metric("Congés en Cours", len(conges_en_cours))
    
    # Congés en cours
    st.subheader("🎯 Congés en cours aujourd'hui")
    if not conges_en_cours.empty:
        st.dataframe(conges_en_cours, use_container_width=True)
    else:
        st.info("Aucun congé en cours aujourd'hui")
    
    # Derniers pointages
    st.subheader("📋 Derniers pointages aujourd'hui")
    if not pointages_du_jour.empty:
        st.dataframe(pointages_du_jour[['nom', 'prenom', 'service', 'heure_arrivee', 'statut_arrivee']], 
                    use_container_width=True)
    else:
        st.info("Aucun pointage enregistré aujourd'hui")

def show_pointage_du_jour():
    st.title("⏰ Pointage du Jour")
    
    # Recherche et filtres
    col1, col2 = st.columns(2)
    with col1:
        recherche = st.text_input("🔍 Rechercher un employé")
    with col2:
        services = ["Tous les services"] + get_services_disponibles()
        filtre_service = st.selectbox("Filtrer par service", services)
    
    # Liste du personnel filtrée
    personnel_filtre = filtrer_personnel(recherche, filtre_service)
    
    for service, employes in personnel_filtre.items():
        st.subheader(f"🏥 {service}")
        
        for emp in employes:
            with st.expander(f"{emp['prenom']} {emp['nom']} - {emp['poste']}"):
                pointage = get_pointage_employe_jour(emp['id'], date.today())
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write(f"**Heure prévue:** {emp['heure_entree_prevue']} - {emp['heure_sortie_prevue']}")
                    
                    if pointage is not None and pointage.get('heure_arrivee'):
                        st.success(f"✅ Arrivée: {pointage['heure_arrivee']} ({pointage['statut_arrivee']})")
                        if pointage.get('retard_minutes', 0) > 0:
                            st.warning(f"⏰ Retard: {pointage['retard_minutes']} minutes")
                    else:
                        st.error("❌ Non pointé")
                
                with col2:
                    if pointage is not None and pointage.get('heure_depart'):
                        st.success(f"✅ Départ: {pointage['heure_depart']} ({pointage['statut_depart']})")
                        if pointage.get('depart_avance_minutes', 0) > 0:
                            st.warning(f"⏰ Départ anticipé: {pointage['depart_avance_minutes']} minutes")
                    else:
                        st.info("ℹ️ Départ non enregistré")
                
                # Formulaire de pointage
                with st.form(f"pointage_{emp['id']}"):
                    col_a, col_b = st.columns(2)
                    
                    with col_a:
                        heure_arrivee = st.time_input("Heure d'arrivée", value=datetime.now().time(), key=f"arrivee_{emp['id']}")
                        motif_retard = st.text_area("Motif retard/absence", key=f"motif_arr_{emp['id']}")
                    
                    with col_b:
                        heure_depart = st.time_input("Heure de départ", value=datetime.now().time(), key=f"depart_{emp['id']}")
                        motif_depart = st.text_area("Motif départ anticipé", key=f"motif_dep_{emp['id']}")
                    
                    notes = st.text_area("Notes", key=f"notes_{emp['id']}")
                    
                    col_btn1, col_btn2, col_btn3 = st.columns(3)
                    
                    with col_btn1:
                        if st.form_submit_button("✅ Pointer l'arrivée"):
                            success, retard = enregistrer_pointage_arrivee(
                                emp['id'], date.today(), heure_arrivee, motif_retard, notes
                            )
                            if success:
                                st.success("✅ Pointage d'arrivée enregistré")
                                if retard > 0:
                                    st.warning(f"⏰ Retard enregistré: {retard} minutes")
                    
                    with col_btn2:
                        if st.form_submit_button("🚪 Pointer le départ"):
                            success, avance = enregistrer_pointage_depart(
                                emp['id'], date.today(), heure_depart, motif_depart, notes
                            )
                            if success:
                                st.success("✅ Pointage de départ enregistré")
                                if avance > 0:
                                    st.warning(f"⏰ Départ anticipé: {avance} minutes")
                    
                    with col_btn3:
                        if st.form_submit_button("❌ Marquer absent"):
                            success = enregistrer_absence(
                                emp['id'], date.today(), motif_retard or "Absence non justifiée", False
                            )
                            if success:
                                st.success("✅ Absence enregistrée")

def show_gestion_personnel():
    st.title("👥 Gestion du Personnel")
    
    tab1, tab2, tab3 = st.tabs(["Liste du Personnel", "Ajouter un Employé", "Modifier un Employé"])
    
    with tab1:
        personnel_df = get_personnel()
        if not personnel_df.empty:
            st.dataframe(personnel_df, use_container_width=True)
        else:
            st.info("Aucun personnel enregistré")
    
    with tab2:
        with st.form("ajouter_personnel"):
            col1, col2 = st.columns(2)
            with col1:
                nom = st.text_input("Nom")
                prenom = st.text_input("Prénom")
                service = st.text_input("Service")
            with col2:
                poste = st.selectbox("Poste", ["Jour", "Nuit"])
                heure_entree = st.time_input("Heure d'entrée prévue", value=tm(8, 0))
                heure_sortie = st.time_input("Heure de sortie prévue", value=tm(16, 0))
            
            if st.form_submit_button("➕ Ajouter"):
                if nom and prenom and service:
                    if ajouter_personnel(nom, prenom, service, poste, heure_entree, heure_sortie):
                        st.success("✅ Employé ajouté avec succès")
                    else:
                        st.error("❌ Erreur lors de l'ajout")
                else:
                    st.warning("⚠️ Veuillez remplir tous les champs obligatoires")
    
    with tab3:
        personnel_actif = get_personnel()
        if not personnel_actif.empty:
            employe_selection = st.selectbox(
                "Sélectionner un employé",
                personnel_actif.apply(lambda x: f"{x['prenom']} {x['nom']} - {x['service']}", axis=1)
            )
            
            if employe_selection:
                selected_index = personnel_actif[
                    personnel_actif.apply(lambda x: f"{x['prenom']} {x['nom']} - {x['service']}" == employe_selection, axis=1)
                ].index[0]
                
                emp_data = personnel_actif.loc[selected_index]
                
                with st.form("modifier_personnel"):
                    col1, col2 = st.columns(2)
                    with col1:
                        nom = st.text_input("Nom", value=emp_data['nom'])
                        prenom = st.text_input("Prénom", value=emp_data['prenom'])
                        service = st.text_input("Service", value=emp_data['service'])
                    with col2:
                        poste = st.selectbox("Poste", ["Jour", 'Nuit'], index=0 if emp_data['poste'] == "Jour" else 1)
                        heure_entree = st.time_input("Heure d'entrée prévue", value=_as_time(emp_data['heure_entree_prevue']))
                        heure_sortie = st.time_input("Heure de sortie prévue", value=_as_time(emp_data['heure_sortie_prevue']))
                        actif = st.checkbox("Actif", value=emp_data['actif'])
                    
                    if st.form_submit_button("💾 Enregistrer les modifications"):
                        if modifier_personnel(emp_data['id'], nom, prenom, service, poste, heure_entree, heure_sortie, actif):
                            st.success("✅ Employé modifié avec succès")
                        else:
                            st.error("❌ Erreur lors de la modification")
        else:
            st.info("Aucun employé à modifier")

def show_historique_pointages():
    st.title("📊 Historique des Pointages")
    
    col1, col2 = st.columns(2)
    with col1:
        date_debut = st.date_input("Date de début", value=date.today() - timedelta(days=7))
    with col2:
        date_fin = st.date_input("Date de fin", value=date.today())
    
    if st.button("🔍 Charger l'historique"):
        pointages_df = get_pointages_periode(date_debut, date_fin)
        retards_df = get_retards_periode(date_debu, date_fin)
        absences_df = get_absences_periode(date_debut, date_fin)
        
        tab1, tab2, tab3 = st.tabs(["Pointages", "Retards", "Absences"])
        
        with tab1:
            if not pointages_df.empty:
                st.dataframe(pointages_df, use_container_width=True)
            else:
                st.info("Aucun pointage dans la période sélectionnée")
        
        with tab2:
            if not retards_df.empty:
                # Afficher seulement les colonnes disponibles
                colonnes_retards = ['nom', 'prenom', 'service', 'poste', 'date_retard', 'retard_minutes', 'motif']
                colonnes_disponibles = [col for col in colonnes_retards if col in retards_df.columns]
                st.dataframe(retards_df[colonnes_disponibles], use_container_width=True)
            else:
                st.info("Aucun retard dans la période sélectionnée")
        
        with tab3:
            if not absences_df.empty:
                # Afficher seulement les colonnes disponibles
                colonnes_absences = ['nom', 'prenom', 'service', 'poste', 'date_absence', 'motif', 'justifie']
                colonnes_disponibles = [col for col in colonnes_absences if col in absences_df.columns]
                st.dataframe(absences_df[colonnes_disponibles], use_container_width=True)
            else:
                st.info("Aucune absence dans la période sélectionnée")

def show_statistiques():
    st.title("📈 Statistiques")
    
    stats_df = get_stats_mensuelles()
    
    if not stats_df.empty:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            total_retard = stats_df['total_retard_minutes'].sum()
            st.metric("Total retard (min)", total_retard)
        
        with col2:
            total_depart_avance = stats_df['total_depart_avance_minutes'].sum()
            st.metric("Total départ anticipé (min)", total_depart_avance)
        
        with col3:
            moy_retard = stats_df['jours_retard'].mean()
            st.metric("Moyenne retards/jour", f"{moy_retard:.1f}")
        
        # Graphique des retards par service
        fig = px.bar(
            stats_df.groupby('service')['jours_retard'].sum().reset_index(),
            x='service',
            y='jours_retard',
            title="Nombre de retards par service"
        )
        st.plotly_chart(fig)
        
        # Tableau détaillé
        st.subheader("📋 Statistiques détaillées par employé")
        st.dataframe(stats_df, use_container_width=True)
    else:
        st.info("Aucune statistique disponible pour le mois en cours")

def show_gestion_conges():
    st.title("📅 Gestion des Congés")
    
    tab1, tab2, tab3, tab4 = st.tabs(["Mes Congés", "Demander un Congé", "Tous les Congés", "Approbation"])
    
    with tab1:
        st.subheader("📋 Mes demandes de congé")
        mes_conges = get_conges_employe(st.session_state.user_id)
        if not mes_conges.empty:
            st.dataframe(mes_conges, use_container_width=True)
        else:
            st.info("Vous n'avez aucune demande de congé")
    
    with tab2:
        st.subheader("➕ Nouvelle demande de congé")
        with st.form("demande_conge"):
            col1, col2 = st.columns(2)
            with col1:
                date_debut = st.date_input("Date de début", min_value=date.today())
                type_conge = st.selectbox("Type de congé", ["Congé annuel", "Maladie", "Familial", "Exceptionnel"])
            with col2:
                date_fin = st.date_input("Date de fin", min_value=date.today())
                motif = st.text_area("Motif")
            
            if st.form_submit_button("📤 Soumettre la demande"):
                if date_debut <= date_fin:
                    if verifier_disponibilite_conge(st.session_state.user_id, date_debut, date_fin):
                        if demander_conge(st.session_state.user_id, date_debut, date_fin, type_conge, motif):
                            st.success("✅ Demande de congé soumise avec succès")
                        else:
                            st.error("❌ Erreur lors de la soumission")
                    else:
                        st.error("❌ Vous avez déjà des congés qui se chevauchent avec cette période")
                else:
                    st.error("❌ La date de fin doit être après la date de début")
    
    with tab3:
        st.subheader("👥 Tous les congés")
        filtre_statut = st.selectbox("Filtrer par statut", ["Tous", "En attente", "Approuvé", "Rejeté"])
        tous_les_conges = get_tous_les_conges(filtre_statut if filtre_statut != "Tous" else "Tous")
        
        if not tous_les_conges.empty:
            st.dataframe(tous_les_conges, use_container_width=True)
        else:
            st.info("Aucun congé trouvé")
    
    with tab4:
        if st.session_state.user_role == "admin":
            st.subheader("✅ Approbation des congés")
            congés_en_attente = get_tous_les_conges("En attente")
            
            if not congés_en_attente.empty:
                for _, conge in congés_en_attente.iterrows():
                    with st.expander(f"{conge['prenom']} {conge['nom']} - {conge['date_debut']} au {conge['date_fin']}"):
                        st.write(f"**Type:** {conge['type_conge']}")
                        st.write(f"**Motif:** {conge['motif']}")
                        
                        col_btn1, col_btn2 = st.columns(2)
                        with col_btn1:
                            if st.button(f"✅ Approuver {conge['id']}"):
                                if modifier_statut_conge(conge['id'], "Approuvé"):
                                    st.success("✅ Congé approuvé")
                                    st.rerun()
                                else:
                                    st.error("❌ Erreur lors de l'approbation")
                        with col_btn2:
                            if st.button(f"❌ Rejeter {conge['id']}"):
                                if modifier_statut_conge(conge['id'], "Rejeté"):
                                    st.success("✅ Congé rejeté")
                                    st.rerun()
                                else:
                                    st.error("❌ Erreur lors du rejet")
            else:
                st.info("Aucun congé en attente d'approbation")
        else:
            st.warning("⛔ Accès réservé aux administrateurs")

def show_gestion_utilisateurs():
    st.title("👥 Gestion des Utilisateurs")
    
    if st.session_state.user_role != "admin":
        st.warning("⛔ Accès réservé aux administrateurs")
        return
    
    tab1, tab2 = st.tabs(["Liste des Utilisateurs", "Ajouter un Utilisateur"])
    
    with tab1:
        st.subheader("📋 Liste des utilisateurs")
        users_df = get_all_users()
        if not users_df.empty:
            st.dataframe(users_df, use_container_width=True)
        else:
            st.info("Aucun utilisateur enregistré")
    
    with tab2:
        st.subheader("➕ Ajouter un nouvel utilisateur")
        with st.form("ajouter_utilisateur"):
            col1, col2 = st.columns(2)
            with col1:
                username = st.text_input("Nom d'utilisateur*")
                email = st.text_input("Email")
            with col2:
                password = st.text_input("Mot de passe*", type="password")
                role = st.selectbox("Rôle", ["user", "admin"])
            
            if st.form_submit_button("➕ Ajouter l'utilisateur"):
                if username and password:
                    if create_user(username, password, role, email):
                        st.success("✅ Utilisateur ajouté avec succès")
                    else:
                        st.error("❌ Erreur lors de l'ajout de l'utilisateur")
                else:
                    st.warning("⚠️ Veuillez remplir tous les champs obligatoires")

# =========================
# Point d'entrée principal
# =========================

if __name__ == "__main__":
    # Initialisation des états de session
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
    if "user" not in st.session_state:
        st.session_state.user = None
    if "user_role" not in st.session_state:
        st.session_state.user_role = None
    if "user_id" not in st.session_state:
        st.session_state.user_id = None
    if "show_stats" not in st.session_state:
        st.session_state.show_stats = False
    
    # Lancement de l'application
    main()# Connexion PostgreSQL
# =========================

def get_connection():
    try:
        return psycopg2.connect(
            host=PG_HOST,
            database=PG_DB,
            user=PG_USER,
            password=PG_PASS,
            port=PG_PORT,
        )
    except psycopg2.OperationalError as e:
        st.error(f"Erreur de connexion à PostgreSQL: {e}")
        return None

def test_connection_background():
    try:
        conn = get_connection()
        if conn:
            conn.close()
            return True
        return False
    except Exception:
        return False

# =========================
# Authentification & Utilisateurs
# =========================

def sha256(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()

def create_users_table():
    conn = get_connection()
    if conn is None:
        return False

    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(50) UNIQUE NOT NULL,
                    password_hash VARCHAR(255) NOT NULL,
                    role VARCHAR(20) DEFAULT 'user',
                    email VARCHAR(100),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            # Créer un admin par défaut si absent
            cur.execute("SELECT COUNT(*) FROM users WHERE username = %s", (DEFAULT_ADMIN_USER,))
            exists = cur.fetchone()[0]
            if exists == 0:
                cur.execute(
                    "INSERT INTO users (username, password_hash, role, email) VALUES (%s, %s, %s, %s)",
                    (
                        DEFAULT_ADMIN_USER,
                        sha256(DEFAULT_ADMIN_PASS),
                        "admin",
                        f"{DEFAULT_ADMIN_USER}@example.com",
                    ),
                )
        return True
    except Exception as e:
        st.error(f"Erreur création table users: {e}")
        return False
    finally:
        if conn:
            conn.close()

def authenticate_user(username, password):
    conn = get_connection()
    if conn is None:
        return False

    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, username, role FROM users WHERE username = %s AND password_hash = %s",
                (username, sha256(password)),
            )
            user = cur.fetchone()
            return user if user else False
    except Exception as e:
        st.error(f"Erreur authentification: {e}")
        return False
    finally:
        if conn:
            conn.close()

def get_all_users():
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        return pd.read_sql_query(
            "SELECT id, username, role, email, created_at FROM users ORDER BY username",
            conn,
        )
    except Exception as e:
        st.error(f"Erreur récupération utilisateurs: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

def create_user(username, password, role, email):
    conn = get_connection()
    if conn is None:
        return False
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO users (username, password_hash, role, email) VALUES (%s, %s, %s, %s)",
                    (username, sha256(password), role, email),
                )
        return True
    except Exception as e:
        st.error(f"Erreur création utilisateur: {e}")
        return False
    finally:
        if conn:
            conn.close()

# =========================
# Modèle de données
# =========================

def create_tables():
    conn = get_connection()
    if conn is None:
        return False

    try:
        with conn:
            with conn.cursor() as cur:
                # Table personnels
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS personnels (
                        id SERIAL PRIMARY KEY,
                        nom VARCHAR(100) NOT NULL,
                        prenom VARCHAR(100) NOT NULL,
                        service VARCHAR(100) NOT NULL,
                        poste VARCHAR(50) NOT NULL CHECK (poste IN ('Jour', 'Nuit')),
                        heure_entree_prevue TIME NOT NULL,
                        heure_sortie_prevue TIME NOT NULL,
                        actif BOOLEAN DEFAULT TRUE,
                        date_creation TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )

                # Table congés
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS conges (
                        id SERIAL PRIMARY KEY,
                        personnel_id INTEGER REFERENCES personnels(id) ON DELETE CASCADE,
                        date_debut DATE NOT NULL,
                        date_fin DATE NOT NULL,
                        type_conge VARCHAR(50) NOT NULL,
                        motif TEXT,
                        statut VARCHAR(20) DEFAULT 'En attente' CHECK (statut IN ('En attente', 'Approuvé', 'Rejeté')),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )

                # Table pointages
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS pointages (
                        id SERIAL PRIMARY KEY,
                        personnel_id INTEGER REFERENCES personnels(id) ON DELETE CASCADE,
                        date_pointage DATE NOT NULL,
                        heure_arrivee TIME,
                        heure_depart TIME,
                        statut_arrivee VARCHAR(50) DEFAULT 'Present',
                        statut_depart VARCHAR(50) DEFAULT 'Present',
                        retard_minutes INTEGER DEFAULT 0,
                        depart_avance_minutes INTEGER DEFAULT 0,
                        motif_retard TEXT,
                        motif_depart_avance TEXT,
                        notes TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(personnel_id, date_pointage)
                    )
                    """
                )

                # Table retards
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS retards (
                        id SERIAL PRIMARY KEY,
                        personnel_id INTEGER REFERENCES personnels(id) ON DELETE CASCADE,
                        date_retard DATE NOT NULL,
                        retard_minutes INTEGER NOT NULL,
                        motif TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )

                # Table absences
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS absences (
                        id SERIAL PRIMARY KEY,
                        personnel_id INTEGER REFERENCES personnels(id) ON DELETE CASCADE,
                        date_absence DATE NOT NULL,
                        motif TEXT,
                        justifie BOOLEAN DEFAULT FALSE,
                        certificat_justificatif BYTEA,
                        type_certificat VARCHAR(10),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(personnel_id, date_absence)
                    )
                    """
                )

                # Données d'exemple s'il n'y a personne
                cur.execute("SELECT COUNT(*) FROM personnels")
                if cur.fetchone()[0] == 0:
                    cur.execute(
                        """
                        INSERT INTO personnels (nom, prenom, service, poste, heure_entree_prevue, heure_sortie_prevue) VALUES
                        ('Dupont', 'Jean', 'Reception', 'Jour', '08:00:00', '16:00:00'),
                        ('Martin', 'Marie', 'Radiologie', 'Nuit', '20:00:00', '04:00:00'),
                        ('Bernard', 'Pierre', 'Urgence', 'Jour', '07:30:00', '15:30:00'),
                        ('Dubois', 'Sophie', 'Maternité', 'Nuit', '21:00:00', '05:00:00'),
                        ('Moreau', 'Luc', 'Administration', 'Jour', '09:00:00', '17:00:00')
                        """
                    )
        # Crée la table users et l'admin par défaut
        ok = create_users_table()
        return True
    except Exception as e:
        st.error(f"Erreur création tables: {e}")
        return False
    finally:
        if conn:
            conn.close()

# =========================
# Fonctions utilitaires
# =========================

def _as_time(value) -> tm:
    if isinstance(value, tm):
        return value
    s = str(value)
    for fmt in ("%H:%M:%S", "%H:%M:%S.%f", "%H:%M"):
        try:
            return datetime.strptime(s, fmt).time()
        except ValueError:
            continue
    return tm(8, 0)

def get_services_disponibles():
    conn = get_connection()
    if conn is None:
        return []
    try:
        df = pd.read_sql_query("SELECT DISTINCT service FROM personnels WHERE actif = TRUE ORDER BY service", conn)
        return df['service'].tolist()
    except Exception as e:
        st.error(f"Erreur récupération services: {e}")
        return []
    finally:
        if conn:
            conn.close()

def filtrer_personnel(recherche, filtre_service):
    personnel_par_service = get_personnel_par_service()
    result = {}
    
    for service, employes in personnel_par_service.items():
        if filtre_service != "Tous les services" and service != filtre_service:
            continue
            
        employes_filtres = []
        for emp in employes:
            nom_complet = f"{emp['prenom']} {emp['nom']}".lower()
            if not recherche or recherche.lower() in nom_complet or recherche.lower() in emp['service'].lower() or recherche.lower() in emp['poste'].lower():
                employes_filtres.append(emp)
        
        if employes_filtres:
            result[service] = employes_filtres
    
    return result

def get_pointage_employe_jour(personnel_id, date_pointage):
    conn = get_connection()
    if conn is None:
        return None
    try:
        df = pd.read_sql_query(
            """
            SELECT * FROM pointages 
            WHERE personnel_id = %s AND date_pointage = %s
            """,
            conn,
            params=(personnel_id, date_pointage)
        )
        if not df.empty:
            return df.iloc[0]
        return None
    except Exception as e:
        st.error(f"Erreur récupération pointage: {e}")
        return None
    finally:
        if conn:
            conn.close()

# =========================
# Requêtes métier
# =========================

def get_personnel():
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        return pd.read_sql_query(
            "SELECT id, nom, prenom, service, poste, heure_entree_prevue, heure_sortie_prevue, actif FROM personnels ORDER BY nom, prenom",
            conn,
        )
    except Exception as e:
        st.error(f"Erreur récupération personnel: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

def ajouter_personnel(nom, prenom, service, poste, heure_entree_prevue, heure_sortie_prevue):
    conn = get_connection()
    if conn is None:
        return False
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO personnels (nom, prenom, service, poste, heure_entree_prevue, heure_sortie_prevue)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (nom, prenom, service, poste, heure_entree_prevue, heure_sortie_prevue),
                )
        return True
    except Exception as e:
        st.error(f"Erreur ajout personnel: {e}")
        return False
    finally:
        if conn:
            conn.close()

def modifier_personnel(personnel_id, nom, prenom, service, poste, heure_entree_prevue, heure_sortie_prevue, actif):
    conn = get_connection()
    if conn is None:
        return False
    try:
        personnel_id = int(personnel_id)
        
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE personnels 
                    SET nom = %s, prenom = %s, service = %s, poste = %s, 
                        heure_entree_prevue = %s, heure_sortie_prevue = %s, actif = %s
                    WHERE id = %s
                    """,
                    (nom, prenom, service, poste, heure_entree_prevue, heure_sortie_prevue, actif, personnel_id),
                )
        return True
    except Exception as e:
        st.error(f"Erreur modification personnel: {e}")
        return False
    finally:
        if conn:
            conn.close()

def calculer_statut_arrivee(heure_pointage, heure_prevue):
    """
    Calcule le statut de pointage selon les règles spécifiques:
    - Plage normale: 15min avant à 5min avant l'heure prévue (07:45 à 07:55 pour 08:00)
    - En retard: après 5min avant l'heure prévue jusqu'à 29 minutes de retard
    - Absent: 30 minutes ou plus de retard (après 08:30 pour 08:00)
    """
    if not heure_pointage or not heure_prevue:
        return "Non pointé", 0, False
    
    heure_prevue = _as_time(heure_prevue)
    heure_pointage = _as_time(heure_pointage)
    
    # Convertir en datetime pour les calculs
    dt_prevue = datetime.combine(date.today(), heure_prevue)
    dt_pointage = datetime.combine(date.today(), heure_pointage)
    
    # Calcul de la différence en minutes
    difference_minutes = (dt_pointage - dt_prevue).total_seconds() / 60
    
    # Définition des plages horaires spécifiques
    debut_plage = dt_prevue - timedelta(minutes=15)  # 07:45 pour 08:00
    fin_plage = dt_prevue - timedelta(minutes=5)     # 07:55 pour 08:00
    limite_retard = dt_prevue + timedelta(minutes=30) # 08:30 pour 08:00
    
    if debut_plage <= dt_pointage <= fin_plage:
        return "Présent à l'heure", 0, False
    elif fin_plage < dt_pointage < limite_retard:
        retard = (dt_pointage - fin_plage).total_seconds() / 60
        return "En retard", int(retard), False
    elif dt_pointage >= limite_retard:
        return "Absent", 30, True  # Retourne 30 minutes de retard et marque comme absent
    elif dt_pointage < debut_plage:
        avance = (debut_plage - dt_pointage).total_seconds() / 60
        return "En avance", int(-avance), False
    
    return "Non pointé", 0, False

def enregistrer_pointage_arrivee(personnel_id, date_pointage, heure_arrivee, motif_retard=None, notes=None, est_absent=False):
    # Vérifier si l'employé est en congé
    if est_en_conge(personnel_id, date_pointage):
        st.error("❌ Cet employé est en congé aujourd'hui. Pointage impossible.")
        return False, 0
    
    conn = get_connection()
    if conn is None:
        return False, 0
    try:
        personnel_id = int(personnel_id)
        
        with conn:
            with conn.cursor() as cur:
                # Heure prévue
                cur.execute("SELECT heure_entree_prevue FROM personnels WHERE id = %s", (personnel_id,))
                res = cur.fetchone()
                if not res:
                    return False, 0
                heure_prevue = _as_time(res[0])

                # Calcul statut selon les nouvelles règles
                statut_arrivee, retard_minutes, est_absent_calc = calculer_statut_arrivee(heure_arrivee, heure_prevue)
                
                # Si le système détecte une absence (retard >= 30min), enregistrer dans la table absences
                if est_absent or est_absent_calc:
                    cur.execute(
                        """
                        INSERT INTO absences (personnel_id, date_absence, motif, justifie)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (personnel_id, date_absence) DO NOTHING
                        """,
                        (personnel_id, date_pointage, motif_retard or f"Absence automatique (retard de {retard_minutes} minutes)", False)
                    )
                    # Ne pas enregistrer le pointage d'arrivée si absent
                    return True, retard_minutes
                
                # Enregistrer le retard si applicable (seulement si < 30 minutes)
                if retard_minutes > 0 and retard_minutes < 30:
                    cur.execute(
                        """
                        INSERT INTO retards (personnel_id, date_retard, retard_minutes, motif)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                        """,
                        (personnel_id, date_pointage, retard_minutes, motif_retard),
                    )

                # Vérifier si un pointage existe déjà pour cette journée
                cur.execute(
                    "SELECT id FROM pointages WHERE personnel_id = %s AND date_pointage = %s",
                    (personnel_id, date_pointage)
                )
                existing = cur.fetchone()

                arr = _as_time(heure_arrivee)

                if existing:
                    # Mettre à jour l'arrivée
                    cur.execute(
                        """
                        UPDATE pointages 
                        SET heure_arrivee = %s, statut_arrivee = %s, retard_minutes = %s, 
                            motif_retard = %s, notes = COALESCE(%s, notes)
                        WHERE id = %s
                        """,
                        (arr, statut_arrivee, retard_minutes, motif_retard, notes, existing[0])
                    )
                else:
                    # Nouveau pointage
                    cur.execute(
                        """
                        INSERT INTO pointages (personnel_id, date_pointage, heure_arrivee, statut_arrivee, retard_minutes, motif_retard, notes)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (personnel_id, date_pointage, arr, statut_arrivee, retard_minutes, motif_retard, notes),
                    )
        return True, retard_minutes
    except Exception as e:
        st.error(f"Erreur enregistrement pointage arrivée: {e}")
        return False, 0
    finally:
        if conn:
            conn.close()

def est_en_conge(personnel_id, date_check):
    """Vérifie si l'employé est en congé à une date donnée"""
    conn = get_connection()
    if conn is None:
        return False
    
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) FROM conges 
                WHERE personnel_id = %s 
                AND statut = 'Approuvé'
                AND date_debut <= %s 
                AND date_fin >= %s
                """,
                (personnel_id, date_check, date_check)
            )
            count = cur.fetchone()[0]
            return count > 0
    except Exception as e:
        st.error(f"Erreur vérification congé: {e}")
        return False
    finally:
        if conn:
            conn.close()



def enregistrer_pointage_depart(personnel_id, date_pointage, heure_depart, motif_depart_avance=None, notes=None):
    # Vérifier si l'employé est en congé
    if est_en_conge(personnel_id, date_pointage):
        st.error("❌ Cet employé est en congé aujourd'hui. Pointage impossible.")
        return False, 0
    
    conn = get_connection()
    if conn is None:
        return False, 0
    try:
        personnel_id = int(personnel_id)
        
        with conn:
            with conn.cursor() as cur:
                # Heure de sortie prévue
                cur.execute("SELECT heure_sortie_prevue FROM personnels WHERE id = %s", (personnel_id,))
                res = cur.fetchone()
                if not res:
                    return False, 0
                heure_sortie_prevue = _as_time(res[0])

                # Calcul départ en avance
                depart_avance_minutes = 0
                statut_depart = "Present"
                dep = _as_time(heure_depart)
                
                # Calculer la différence en minutes
                delta_minutes = (datetime.combine(date.today(), heure_sortie_prevue) - datetime.combine(date.today(), dep)).total_seconds() / 60
                
                # Départ en avance seulement si plus de 5 minutes
                if delta_minutes > 5:
                    depart_avance_minutes = int(delta_minutes)
                    statut_depart = "Départ anticipé"

                # Vérifier si un pointage existe déjà pour cette journée
                cur.execute(
                    "SELECT id FROM pointages WHERE personnel_id = %s AND date_pointage = %s",
                    (personnel_id, date_pointage)
                )
                existing = cur.fetchone()

                if existing:
                    # Mettre à jour le départ
                    cur.execute(
                        """
                        UPDATE pointages 
                        SET heure_depart = %s, statut_depart = %s, depart_avance_minutes = %s, 
                            motif_depart_avance = %s, notes = COALESCE(%s, notes)
                        WHERE id = %s
                        """,
                        (dep, statut_depart, depart_avance_minutes, motif_depart_avance, notes, existing[0])
                    )
                else:
                    # Nouveau pointage (cas rare où on pointerait le départ sans l'arrivée)
                    cur.execute(
                        """
                        INSERT INTO pointages (personnel_id, date_pointage, heure_depart, statut_depart, depart_avance_minutes, motif_depart_avance, notes)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (personnel_id, date_pointage, dep, statut_depart, depart_avance_minutes, motif_depart_avance, notes),
                    )
        return True, depart_avance_minutes
    except Exception as e:
        st.error(f"Erreur enregistrement pointage départ: {e}")
        return False, 0
    finally:
        if conn:
            conn.close()

def get_pointages_periode(date_debut, date_fin):
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        return pd.read_sql_query(
            """
            SELECT p.nom, p.prenom, p.service, p.poste, p.heure_entree_prevue, p.heure_sortie_prevue,
                   pt.date_pointage, pt.heure_arrivee, pt.heure_depart, pt.statut_arrivee, pt.statut_depart, 
                   pt.retard_minutes, pt.depart_avance_minutes, pt.motif_retard, pt.motif_depart_avance, pt.notes
            FROM pointages pt
            JOIN personnels p ON pt.personnel_id = p.id
            WHERE pt.date_pointage BETWEEN %s AND %s
            ORDER BY pt.date_pointage DESC, p.nom, p.prenom
            """,
            conn,
            params=(date_debut, date_fin),
        )
    except Exception as e:
        st.error(f"Erreur récupération pointages: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

def get_retards_periode(date_debut, date_fin):
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        return pd.read_sql_query(
            """
            SELECT p.nom, p.prenom, p.service, p.poste, p.heure_entree_prevue,
                   r.date_retard, r.retard_minutes, r.motif, r.created_at
            FROM retards r
            JOIN personnels p ON r.personnel_id = p.id
            WHERE r.date_retard BETWEEN %s AND %s
            ORDER BY r.date_retard DESC, r.retard_minutes DESC
            """,
            conn,
            params=(date_debut, date_fin),
        )
    except Exception as e:
        st.error(f"Erreur récupération retards: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

def get_absences_du_jour():
    """Récupère les absences du jour actuel"""
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        return pd.read_sql_query(
            """
            SELECT p.id, p.nom, p.prenom, p.service, p.poste, p.heure_entree_prevue,
                   a.motif, a.justifie, a.created_at
            FROM personnels p
            LEFT JOIN absences a ON p.id = a.personnel_id AND a.date_absence = %s
            WHERE p.actif = TRUE 
            AND p.id NOT IN (
                SELECT personnel_id FROM pointages WHERE date_pointage = %s AND heure_arrivee IS NOT NULL
            )
            AND p.id NOT IN (
                SELECT personnel_id FROM conges 
                WHERE statut = 'Approuvé' 
                AND date_debut <= %s 
                AND date_fin >= %s
            )
            ORDER BY p.nom, p.prenom
            """,
            conn,
            params=(date.today(), date.today(), date.today(), date.today()),
        )
    except Exception as e:
        st.error(f"Erreur récupération absences du jour: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

def get_absences_periode(date_debut, date_fin):
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        return pd.read_sql_query(
            """
            SELECT a.date_absence, p.nom, p.prenom, p.service, p.poste, 
                   p.heure_entree_prevue, a.motif, a.justifie, a.certificat_justificatif IS NOT NULL as has_certificat,
                   a.created_at
            FROM absences a
            JOIN personnels p ON a.personnel_id = p.id
            WHERE a.date_absence BETWEEN %s AND %s
            ORDER BY a.date_absence DESC, p.nom, p.prenom
            """,
            conn,
            params=(date_debut, date_fin),
        )
    except Exception as e:
        st.error(f"Erreur récupération absences: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

def get_stats_mensuelles():
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        return pd.read_sql_query(
            """
            SELECT 
                p.nom, p.prenom, p.service,
                COUNT(pt.id) as jours_presents,
                SUM(CASE WHEN pt.statut_arrivee = 'Retard' THEN 1 ELSE 0 END) as jours_retard,
                SUM(CASE WHEN pt.statut_depart = 'Départ anticipé' THEN 1 ELSE 0 END) as jours_depart_anticipé,
                COALESCE(SUM(pt.retard_minutes),0) as total_retard_minutes,
                COALESCE(SUM(pt.depart_avance_minutes),0) as total_depart_avance_minutes
            FROM personnels p
            LEFT JOIN pointages pt ON p.id = pt.personnel_id 
                AND pt.date_pointage >= DATE_TRUNC('month', CURRENT_DATE)
            WHERE p.actif = TRUE
            GROUP BY p.id, p.nom, p.prenom, p.service
            ORDER BY p.nom, p.prenom
            """,
            conn,
        )
    except Exception as e:
        st.error(f"Erreur stats mensuelles: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

def marquer_absence_automatique():
    conn = get_connection()
    if conn is None:
        return False
    
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT p.id, p.nom, p.prenom, p.heure_entree_prevue
                    FROM personnels p
                    WHERE p.actif = TRUE 
                    AND p.id NOT IN (
                        SELECT personnel_id FROM pointages WHERE date_pointage = %s AND heure_arrivee IS NOT NULL
                    )
                    AND p.id NOT IN (
                        SELECT personnel_id FROM conges 
                        WHERE statut = 'Approuvé' 
                        AND date_debu <= %s 
                        AND date_fin >= %s
                    )
                    """,
                    (date.today(), date.today(), date.today())
                )
                employes_absents = cur.fetchall()
                
                maintenant = datetime.now().time()
                
                for emp in employes_absents:
                    emp_id, nom, prenom, heure_prevue = emp
                    heure_prevue = _as_time(heure_prevue)
                    
                    heure_limite = (datetime.combine(date.today(), heure_prevue) + timedelta(minutes=30)).time()
                    
                    if maintenant > heure_limite:
                        cur.execute(
                            """
                            INSERT INTO absences (personnel_id, date_absence, motif, justifie)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (personnel_id, date_absence) DO NOTHING
                            """,
                            (emp_id, date.today(), "Absence non justifiée (automatique)", False)
                        )
        return True
    except Exception as e:
        st.error(f"Erreur marquage automatique des absences: {e}")
        return False
    finally:
        if conn:
            conn.close()

def get_personnel_par_service():
    conn = get_connection()
    if conn is None:
        return {}
    try:
        df = pd.read_sql_query(
            "SELECT id, nom, prenom, service, poste, heure_entree_prevue, heure_sortie_prevue, actif FROM personnels WHERE actif = TRUE ORDER BY service, nom, prenom",
            conn,
        )
        
        personnel_par_service = {}
        for _, row in df.iterrows():
            service = row['service']
            if service not in personnel_par_service:
                personnel_par_service[service] = []
            personnel_par_service[service].append(row.to_dict())
            
        return personnel_par_service
    except Exception as e:
        st.error(f"Erreur récupération personnel par service: {e}")
        return {}
    finally:
        if conn:
            conn.close()

def get_pointages_du_jour():
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        return pd.read_sql_query(
            """
            SELECT p.id, p.nom, p.prenom, p.service, p.poste, p.heure_entree_prevue, p.heure_sortie_prevue,
                   pt.heure_arrivee, pt.heure_depart, pt.statut_arrivee, pt.statut_depart, 
                   pt.retard_minutes, pt.depart_avance_minutes, pt.motif_retard, pt.motif_depart_avance, pt.notes
            FROM pointages pt
            JOIN personnels p ON pt.personnel_id = p.id
            WHERE pt.date_pointage = %s
            ORDER BY p.service, p.nom, p.prenom
            """,
            conn,
            params=(date.today(),),
        )
    except Exception as e:
        st.error(f"Erreur récupération pointages du jour: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

def enregistrer_absence(personnel_id, date_absence, motif, justifie=False, certificat_file=None):
    conn = get_connection()
    if conn is None:
        return False
    try:
        # Conversion de numpy.int64 en int Python standard
        personnel_id = int(personnel_id) if hasattr(personnel_id, 'item') else int(personnel_id)
        
        with conn:
            with conn.cursor() as cur:
                if certificat_file:
                    # Lire le fichier et déterminer le type
                    file_data = certificat_file.read()
                    file_type = certificat_file.type.split('/')[-1]
                    
                    cur.execute(
                        """
                        INSERT INTO absences (personnel_id, date_absence, motif, justifie, certificat_justificatif, type_certificat)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (personnel_id, date_absence)
                        DO UPDATE SET 
                            motif = EXCLUDED.motif,
                            justifie = EXCLUDED.justifie,
                            certificat_justificatif = EXCLUDED.certificat_justificatif,
                            type_certificat = EXCLUDED.type_certificat
                        """,
                        (personnel_id, date_absence, motif, justifie, file_data, file_type),
                    )
                else:
                    cur.execute(
                        """
                        INSERT INTO absences (personnel_id, date_absence, motif, justifie)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (personnel_id, date_absence)
                        DO UPDATE SET 
                            motif = EXCLUDED.motif,
                            justifie = EXCLUDED.justifie
                        """,
                        (personnel_id, date_absence, motif, justifie),
                    )
        return True
    except Exception as e:
        st.error(f"Erreur enregistrement absence: {e}")
        return False
    finally:
        if conn:
            conn.close()

def get_certificat_absence(absence_id):
    conn = get_connection()
    if conn is None:
        return None, None
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT certificat_justificatif, type_certificat FROM absences WHERE id = %s",
                (absence_id,)
            )
            result = cur.fetchone()
            if result:
                return result[0], result[1]
            return None, None
    except Exception as e:
        st.error(f"Erreur récupération certificat: {e}")
        return None, None
    finally:
        if conn:
            conn.close()

# =========================
# FONCTIONS CONGES
# =========================

def demander_conge(personnel_id, date_debut, date_fin, type_conge, motif):
    """Enregistre une nouvelle demande de congé"""
    conn = get_connection()
    if conn is None:
        return False
    
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO conges (personnel_id, date_debut, date_fin, type_conge, motif)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (personnel_id, date_debut, date_fin, type_conge, motif)
                )
        return True
    except Exception as e:
        st.error(f"Erreur lors de la demande de congé: {e}")
        return False
    finally:
        if conn:
            conn.close()

def get_conges_employe(personnel_id):
    """Récupère tous les congés d'un employé"""
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    
    try:
        return pd.read_sql_query(
            """
            SELECT c.id, c.date_debut, c.date_fin, c.type_conge, c.motif, c.statut, c.created_at
            FROM conges c
            WHERE c.personnel_id = %s
            ORDER BY c.date_debut DESC
            """,
            conn,
            params=(personnel_id,)
        )
    except Exception as e:
        st.error(f"Erreur récupération congés employé: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

def get_tous_les_conges(filtre_statut="Tous"):
    """Récupère tous les congés avec option de filtre par statut"""
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    
    try:
        query = """
            SELECT c.id, p.nom, p.prenom, p.service, c.date_debut, c.date_fin, 
                   c.type_conge, c.motif, c.statut, c.created_at
            FROM conges c
            JOIN personnels p ON c.personnel_id = p.id
        """
        
        params = []
        if filtre_statut != "Tous":
            query += " WHERE c.statut = %s"
            params.append(filtre_statut)
        
        query += " ORDER BY c.created_at DESC"
        
        return pd.read_sql_query(query, conn, params=params)
    except Exception as e:
        st.error(f"Erreur récupération tous les congés: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

def modifier_statut_conge(conge_id, nouveau_statut):
    """Modifie le statut d'une demande de congé"""
    conn = get_connection()
    if conn is None:
        return False
    
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE conges 
                    SET statut = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                    """,
                    (nouveau_statut, conge_id)
                )
        return True
    except Exception as e:
        st.error(f"Erreur modification statut congé: {e}")
        return False
    finally:
        if conn:
            conn.close()

def get_conges_en_cours():
    """Récupère les congés en cours (aujourd'hui dans la période)"""
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    
    try:
        return pd.read_sql_query(
            """
            SELECT p.nom, p.prenom, p.service, c.date_debut, c.date_fin, c.type_conge
            FROM conges c
            JOIN personnels p ON c.personnel_id = p.id
            WHERE c.statut = 'Approuvé'
            AND c.date_debut <= CURRENT_DATE
            AND c.date_fin >= CURRENT_DATE
            ORDER BY p.service, p.nom
            """,
            conn
        )
    except Exception as e:
        st.error(f"Erreur récupération congés en cours: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

def verifier_disponibilite_conge(personnel_id, date_debut, date_fin):
    """Vérifie si l'employé n'a pas déjà des congés qui se chevauchent"""
    conn = get_connection()
    if conn is None:
        return False
    
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) FROM conges 
                WHERE personnel_id = %s 
                AND statut IN ('En attente', 'Approuvé')
                AND (
                    (date_debu BETWEEN %s AND %s) OR
                    (date_fin BETWEEN %s AND %s) OR
                    (date_debu <= %s AND date_fin >= %s)
                )
                """,
                (personnel_id, date_debut, date_fin, date_debut, date_fin, date_debut, date_fin)
            )
            count = cur.fetchone()[0]
            return count == 0
    except Exception as e:
        st.error(f"Erreur vérification disponibilité congé: {e}")
        return False
    finally:
        if conn:
            conn.close()

# =========================
# NOUVELLES FONCTIONS POUR ABSENCES ET RETARDS
# =========================

def show_absences_page():
    st.title("❌ Gestion des Absences")
    
    tab1, tab2 = st.tabs(["Liste des Absences", "Ajouter une Absence"])
    
    with tab1:
        st.subheader("📋 Liste des absences")
        
        col1, col2 = st.columns(2)
        with col1:
            date_debut = st.date_input("Date de début", value=date.today() - timedelta(days=30), key="abs_debut")
        with col2:
            date_fin = st.date_input("Date de fin", value=date.today(), key="abs_fin")
        
        if st.button("🔍 Charger les absences", key="btn_absences"):
            absences_df = get_absences_periode(date_debut, date_fin)
            
            if not absences_df.empty:
                # Filtrer les colonnes pour une meilleure lisibilité
                colonnes_affichees = ['date_absence', 'nom', 'prenom', 'service', 'poste', 'motif', 'justifie', 'has_certificat']
                colonnes_disponibles = [col for col in colonnes_affichees if col in absences_df.columns]
                
                st.dataframe(absences_df[colonnes_disponibles], use_container_width=True)
                
                # Statistiques des absences
                st.subheader("📊 Statistiques des absences")
                col_stat1, col_stat2, col_stat3 = st.columns(3)
                
                with col_stat1:
                    total_absences = len(absences_df)
                    st.metric("Total des absences", total_absences)
                
                with col_stat2:
                    absences_justifiees = len(absences_df[absences_df['justifie'] == True])
                    st.metric("Absences justifiées", absences_justifiees)
                
                with col_stat3:
                    absences_non_justifiees = len(absences_df[absences_df['justifie'] == False])
                    st.metric("Absences non justifiées", absences_non_justifiees)
                
                # Graphique des absences par service
                absences_par_service = absences_df.groupby('service').size().reset_index(name='count')
                if not absences_par_service.empty:
                    fig = px.bar(
                        absences_par_service,
                        x='service',
                        y='count',
                        title="Nombre d'absences par service"
                    )
                    st.plotly_chart(fig)
            else:
                st.info("Aucune absence trouvée pour la période sélectionnée")
    
    with tab2:
        st.subheader("➕ Ajouter une absence")
        
        personnel_df = get_personnel()
        if not personnel_df.empty:
            employe_selection = st.selectbox(
                "Sélectionner un employé",
                personnel_df.apply(lambda x: f"{x['prenom']} {x['nom']} - {x['service']}", axis=1)
            )
            
            if employe_selection:
                selected_index = personnel_df[
                    personnel_df.apply(lambda x: f"{x['prenom']} {x['nom']} - {x['service']}" == employe_selection, axis=1)
                ].index[0]
                
                emp_data = personnel_df.loc[selected_index]
                
                with st.form("ajouter_absence"):
                    col1, col2 = st.columns(2)
                    with col1:
                        date_absence = st.date_input("Date d'absence", value=date.today())
                        justifie = st.checkbox("Absence justifiée")
                    with col2:
                        motif = st.text_area("Motif de l'absence")
                        certificat = st.file_uploader("Certificat médical (si justifiée)", type=['pdf', 'jpg', 'jpeg', 'png'])
                    
                    if st.form_submit_button("✅ Enregistrer l'absence"):
                        if enregistrer_absence(emp_data['id'], date_absence, motif, justifie, certificat):
                            st.success("✅ Absence enregistrée avec succès")
                        else:
                            st.error("❌ Erreur lors de l'enregistrement de l'absence")
        else:
            st.info("Aucun employé disponible")

def show_retards_page():
    st.title("⏰ Gestion des Retards")
    
    tab1, tab2 = st.tabs(["Liste des Retards", "Statistiques des Retards"])
    
    with tab1:
        st.subheader("📋 Liste des retards")
        
        col1, col2 = st.columns(2)
        with col1:
            date_debut = st.date_input("Date de début", value=date.today() - timedelta(days=30), key="retard_debut")
        with col2:
            date_fin = st.date_input("Date de fin", value=date.today(), key="retard_fin")
        
        if st.button("🔍 Charger les retards", key="btn_retards"):
            retards_df = get_retards_periode(date_debut, date_fin)
            
            if not retards_df.empty:
                st.dataframe(retards_df, use_container_width=True)
                
                # Statistiques des retards
                st.subheader("📊 Statistiques des retards")
                col_stat1, col_stat2, col_stat3 = st.columns(3)
                
                with col_stat1:
                    total_retards = len(retards_df)
                    st.metric("Total des retards", total_retards)
                
                with col_stat2:
                    retard_moyen = retards_df['retard_minutes'].mean()
                    st.metric("Retard moyen (min)", f"{retard_moyen:.1f}")
                
                with col_stat3:
                    retard_max = retards_df['retard_minutes'].max()
                    st.metric("Retard maximum (min)", retard_max)
                
                # Top 5 des employés avec le plus de retards
                retards_par_employe = retards_df.groupby(['nom', 'prenom']).agg({
                    'retard_minutes': ['count', 'sum', 'mean']
                }).round(1)
                retards_par_employe.columns = ['nb_retards', 'total_minutes', 'moyenne_minutes']
                retards_par_employe = retards_par_employe.sort_values('nb_retards', ascending=False)
                
                st.subheader("🏆 Top 5 des employés avec le plus de retards")
                st.dataframe(retards_par_employe.head(), use_container_width=True)
                
                # Graphique des retards par service
                retards_par_service = retards_df.groupby('service').agg({
                    'retard_minutes': ['count', 'sum']
                }).round(1)
                retards_par_service.columns = ['nb_retards', 'total_minutes']
                retards_par_service = retards_par_service.sort_values('nb_retards', ascending=False)
                
                if not retards_par_service.empty:
                    fig = px.bar(
                        retards_par_service.reset_index(),
                        x='service',
                        y='nb_retards',
                        title="Nombre de retards par service"
                    )
                    st.plotly_chart(fig)
            else:
                st.info("Aucun retard trouvé pour la période sélectionnée")
    
    with tab2:
        st.subheader("📈 Analyse des retards")
        
        # Charger les données pour l'analyse
        retards_df = get_retards_periode(date.today() - timedelta(days=60), date.today())
        
        if not retards_df.empty:
            # Évolution des retards dans le temps
            retards_par_jour = retards_df.groupby('date_retard').agg({
                'retard_minutes': ['count', 'mean']
            }).round(1)
            retards_par_jour.columns = ['nb_retards', 'moyenne_minutes']
            
            fig = px.line(
                retards_par_jour.reset_index(),
                x='date_retard',
                y='nb_retards',
                title="Évolution du nombre de retards par jour"
            )
            st.plotly_chart(fig)
            
            # Répartition des retards par durée
            fig2 = px.histogram(
                retards_df,
                x='retard_minutes',
                nbins=20,
                title="Répartition des retards par durée (minutes)"
            )
            st.plotly_chart(fig2)
            
            # Retards par jour de la semaine
            retards_df['jour_semaine'] = pd.to_datetime(retards_df['date_retard']).dt.day_name()
            retards_par_jour_semaine = retards_df.groupby('jour_semaine').agg({
                'retard_minutes': ['count', 'mean']
            }).round(1)
            retards_par_jour_semaine.columns = ['nb_retards', 'moyenne_minutes']
            
            # Ordonner les jours de la semaine
            jours_ordre = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            retards_par_jour_semaine = retards_par_jour_semaine.reindex(jours_ordre)
            
            fig3 = px.bar(
                retards_par_jour_semaine.reset_index(),
                x='jour_semaine',
                y='nb_retards',
                title="Nombre de retards par jour de la semaine"
            )
            st.plotly_chart(fig3)
        else:
            st.info("Aucune donnée de retard disponible pour l'analyse")

# =========================
# Interface Streamlit
# =========================

def main():
    # Initialisation
    if not test_connection_background():
        st.error("❌ Impossible de se connecter à la base de données. Vérifiez la configuration.")
        return
    
    if not create_tables():
        st.error("❌ Erreur lors de l'initialisation des tables.")
        return
    
    # Authentification
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
        st.session_state.user = None
        st.session_state.user_role = None
        st.session_state.user_id = None
    
    if not st.session_state.authenticated:
        show_login()
        return
    
    # Menu principal
    st.sidebar.title(f"👤 {st.session_state.user} ({st.session_state.user_role})")
    
    menu_options = [
        "🏠 Tableau de Bord",
        "⏰ Pointage du Jour", 
        "👥 Gestion du Personnel",
        "📊 Historique des Pointages",
        "📈 Statistiques",
        "📅 Gestion des Congés",
        "❌ Absences",
        "⏰ Retards",
        "👥 Gestion des Utilisateurs"
    ]
    
    if st.session_state.user_role != "admin":
        menu_options.remove("👥 Gestion des Utilisateurs")
        menu_options.remove("👥 Gestion du Personnel")
        menu_options.remove("❌ Absences")
        menu_options.remove("⏰ Retards")
    
    choice = st.sidebar.selectbox("Navigation", menu_options)
    
    if choice == "🏠 Tableau de Bord":
        show_dashboard()
    elif choice == "⏰ Pointage du Jour":
        show_pointage_du_jour()
    elif choice == "👥 Gestion du Personnel":
        show_gestion_personnel()
    elif choice == "📊 Historique des Pointages":
        show_historique_pointages()
    elif choice == "📈 Statistiques":
        show_statistiques()
    elif choice == "📅 Gestion des Congés":
        show_gestion_conges()
    elif choice == "❌ Absences":
        show_absences_page()
    elif choice == "⏰ Retards":
        show_retards_page()
    elif choice == "👥 Gestion des Utilisateurs" and st.session_state.user_role == "admin":
        show_gestion_utilisateurs()
    
    # Bouton de déconnexion
    if st.sidebar.button("🚪 Déconnexion"):
        st.session_state.authenticated = False
        st.session_state.user = None
        st.session_state.user_role = None
        st.session_state.user_id = None
        st.rerun()

def show_login():
    st.title("🔐 Connexion")
    with st.form("login_form"):
        username = st.text_input("Nom d'utilisateur")
        password = st.text_input("Mot de passe", type="password")
        submit = st.form_submit_button("Se connecter")
        
        if submit:
            user = authenticate_user(username, password)
            if user:
                st.session_state.authenticated = True
                st.session_state.user = user[1]  # username
                st.session_state.user_role = user[2]  # role
                st.session_state.user_id = user[0]  # id
                st.rerun()
            else:
                st.error("❌ Identifiants incorrects")

def show_dashboard():
    st.title("🏠 Tableau de Bord")
    
    # Marquage automatique des absences
    if st.button("🔄 Vérifier les absences automatiques"):
        if marquer_absence_automatique():
            st.success("✅ Absences automatiques vérifiées")
        else:
            st.error("❌ Erreur lors de la vérification des absences")
    
    col1, col2, col3, col4 = st.columns(4)
    
    # Statistiques rapides
    personnel_df = get_personnel()
    pointages_du_jour = get_pointages_du_jour()
    absences_du_jour = get_absences_du_jour()
    conges_en_cours = get_conges_en_cours()
    
    with col1:
        st.metric("Total Personnel", len(personnel_df[personnel_df['actif']]))
    with col2:
        st.metric("Pointages Aujourd'hui", len(pointages_du_jour))
    with col3:
        st.metric("Absences Aujourd'hui", len(absences_du_jour))
    with col4:
        st.metric("Congés en Cours", len(conges_en_cours))
    
    # Congés en cours
    st.subheader("🎯 Congés en cours aujourd'hui")
    if not conges_en_cours.empty:
        st.dataframe(conges_en_cours, use_container_width=True)
    else:
        st.info("Aucun congé en cours aujourd'hui")
    
    # Derniers pointages
    st.subheader("📋 Derniers pointages aujourd'hui")
    if not pointages_du_jour.empty:
        st.dataframe(pointages_du_jour[['nom', 'prenom', 'service', 'heure_arrivee', 'statut_arrivee']], 
                    use_container_width=True)
    else:
        st.info("Aucun pointage enregistré aujourd'hui")

def show_pointage_du_jour():
    st.title("⏰ Pointage du Jour")
    
    # Recherche et filtres
    col1, col2 = st.columns(2)
    with col1:
        recherche = st.text_input("🔍 Rechercher un employé")
    with col2:
        services = ["Tous les services"] + get_services_disponibles()
        filtre_service = st.selectbox("Filtrer par service", services)
    
    # Liste du personnel filtrée
    personnel_filtre = filtrer_personnel(recherche, filtre_service)
    
    for service, employes in personnel_filtre.items():
        st.subheader(f"🏥 {service}")
        
        for emp in employes:
            with st.expander(f"{emp['prenom']} {emp['nom']} - {emp['poste']}"):
                pointage = get_pointage_employe_jour(emp['id'], date.today())
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write(f"**Heure prévue:** {emp['heure_entree_prevue']} - {emp['heure_sortie_prevue']}")
                    
                    if pointage is not None and pointage.get('heure_arrivee'):
                        st.success(f"✅ Arrivée: {pointage['heure_arrivee']} ({pointage['statut_arrivee']})")
                        if pointage.get('retard_minutes', 0) > 0:
                            st.warning(f"⏰ Retard: {pointage['retard_minutes']} minutes")
                    else:
                        st.error("❌ Non pointé")
                
                with col2:
                    if pointage is not None and pointage.get('heure_depart'):
                        st.success(f"✅ Départ: {pointage['heure_depart']} ({pointage['statut_depart']})")
                        if pointage.get('depart_avance_minutes', 0) > 0:
                            st.warning(f"⏰ Départ anticipé: {pointage['depart_avance_minutes']} minutes")
                    else:
                        st.info("ℹ️ Départ non enregistré")
                
                # Formulaire de pointage
                with st.form(f"pointage_{emp['id']}"):
                    col_a, col_b = st.columns(2)
                    
                    with col_a:
                        heure_arrivee = st.time_input("Heure d'arrivée", value=datetime.now().time(), key=f"arrivee_{emp['id']}")
                        motif_retard = st.text_area("Motif retard/absence", key=f"motif_arr_{emp['id']}")
                    
                    with col_b:
                        heure_depart = st.time_input("Heure de départ", value=datetime.now().time(), key=f"depart_{emp['id']}")
                        motif_depart = st.text_area("Motif départ anticipé", key=f"motif_dep_{emp['id']}")
                    
                    notes = st.text_area("Notes", key=f"notes_{emp['id']}")
                    
                    col_btn1, col_btn2, col_btn3 = st.columns(3)
                    
                    with col_btn1:
                        if st.form_submit_button("✅ Pointer l'arrivée"):
                            success, retard = enregistrer_pointage_arrivee(
                                emp['id'], date.today(), heure_arrivee, motif_retard, notes
                            )
                            if success:
                                st.success("✅ Pointage d'arrivée enregistré")
                                if retard > 0:
                                    st.warning(f"⏰ Retard enregistré: {retard} minutes")
                    
                    with col_btn2:
                        if st.form_submit_button("🚪 Pointer le départ"):
                            success, avance = enregistrer_pointage_depart(
                                emp['id'], date.today(), heure_depart, motif_depart, notes
                            )
                            if success:
                                st.success("✅ Pointage de départ enregistré")
                                if avance > 0:
                                    st.warning(f"⏰ Départ anticipé: {avance} minutes")
                    
                    with col_btn3:
                        if st.form_submit_button("❌ Marquer absent"):
                            success = enregistrer_absence(
                                emp['id'], date.today(), motif_retard or "Absence non justifiée", False
                            )
                            if success:
                                st.success("✅ Absence enregistrée")

def show_gestion_personnel():
    st.title("👥 Gestion du Personnel")
    
    tab1, tab2, tab3 = st.tabs(["Liste du Personnel", "Ajouter un Employé", "Modifier un Employé"])
    
    with tab1:
        personnel_df = get_personnel()
        if not personnel_df.empty:
            st.dataframe(personnel_df, use_container_width=True)
        else:
            st.info("Aucun personnel enregistré")
    
    with tab2:
        with st.form("ajouter_personnel"):
            col1, col2 = st.columns(2)
            with col1:
                nom = st.text_input("Nom")
                prenom = st.text_input("Prénom")
                service = st.text_input("Service")
            with col2:
                poste = st.selectbox("Poste", ["Jour", "Nuit"])
                heure_entree = st.time_input("Heure d'entrée prévue", value=tm(8, 0))
                heure_sortie = st.time_input("Heure de sortie prévue", value=tm(16, 0))
            
            if st.form_submit_button("➕ Ajouter"):
                if nom and prenom and service:
                    if ajouter_personnel(nom, prenom, service, poste, heure_entree, heure_sortie):
                        st.success("✅ Employé ajouté avec succès")
                    else:
                        st.error("❌ Erreur lors de l'ajout")
                else:
                    st.warning("⚠️ Veuillez remplir tous les champs obligatoires")
    
    with tab3:
        personnel_actif = get_personnel()
        if not personnel_actif.empty:
            employe_selection = st.selectbox(
                "Sélectionner un employé",
                personnel_actif.apply(lambda x: f"{x['prenom']} {x['nom']} - {x['service']}", axis=1)
            )
            
            if employe_selection:
                selected_index = personnel_actif[
                    personnel_actif.apply(lambda x: f"{x['prenom']} {x['nom']} - {x['service']}" == employe_selection, axis=1)
                ].index[0]
                
                emp_data = personnel_actif.loc[selected_index]
                
                with st.form("modifier_personnel"):
                    col1, col2 = st.columns(2)
                    with col1:
                        nom = st.text_input("Nom", value=emp_data['nom'])
                        prenom = st.text_input("Prénom", value=emp_data['prenom'])
                        service = st.text_input("Service", value=emp_data['service'])
                    with col2:
                        poste = st.selectbox("Poste", ["Jour", 'Nuit'], index=0 if emp_data['poste'] == "Jour" else 1)
                        heure_entree = st.time_input("Heure d'entrée prévue", value=_as_time(emp_data['heure_entree_prevue']))
                        heure_sortie = st.time_input("Heure de sortie prévue", value=_as_time(emp_data['heure_sortie_prevue']))
                        actif = st.checkbox("Actif", value=emp_data['actif'])
                    
                    if st.form_submit_button("💾 Enregistrer les modifications"):
                        if modifier_personnel(emp_data['id'], nom, prenom, service, poste, heure_entree, heure_sortie, actif):
                            st.success("✅ Employé modifié avec succès")
                        else:
                            st.error("❌ Erreur lors de la modification")
        else:
            st.info("Aucun employé à modifier")

def show_historique_pointages():
    st.title("📊 Historique des Pointages")
    
    col1, col2 = st.columns(2)
    with col1:
        date_debut = st.date_input("Date de début", value=date.today() - timedelta(days=7))
    with col2:
        date_fin = st.date_input("Date de fin", value=date.today())
    
    if st.button("🔍 Charger l'historique"):
        pointages_df = get_pointages_periode(date_debut, date_fin)
        retards_df = get_retards_periode(date_debut, date_fin)
        absences_df = get_absences_periode(date_debut, date_fin)
        
        tab1, tab2, tab3 = st.tabs(["Pointages", "Retards", "Absences"])
        
        with tab1:
            if not pointages_df.empty:
                st.dataframe(pointages_df, use_container_width=True)
            else:
                st.info("Aucun pointage dans la période sélectionnée")
        
        with tab2:
            if not retards_df.empty:
                # Afficher seulement les colonnes disponibles
                colonnes_retards = ['nom', 'prenom', 'service', 'poste', 'date_retard', 'retard_minutes', 'motif']
                colonnes_disponibles = [col for col in colonnes_retards if col in retards_df.columns]
                st.dataframe(retards_df[colonnes_disponibles], use_container_width=True)
            else:
                st.info("Aucun retard dans la période sélectionnée")
        
        with tab3:
            if not absences_df.empty:
                # Afficher seulement les colonnes disponibles
                colonnes_absences = ['nom', 'prenom', 'service', 'poste', 'date_absence', 'motif', 'justifie']
                colonnes_disponibles = [col for col in colonnes_absences if col in absences_df.columns]
                st.dataframe(absences_df[colonnes_disponibles], use_container_width=True)
            else:
                st.info("Aucune absence dans la période sélectionnée")

def show_statistiques():
    st.title("📈 Statistiques")
    
    stats_df = get_stats_mensuelles()
    
    if not stats_df.empty:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            total_retard = stats_df['total_retard_minutes'].sum()
            st.metric("Total retard (min)", total_retard)
        
        with col2:
            total_depart_avance = stats_df['total_depart_avance_minutes'].sum()
            st.metric("Total départ anticipé (min)", total_depart_avance)
        
        with col3:
            moy_retard = stats_df['jours_retard'].mean()
            st.metric("Moyenne retards/jour", f"{moy_retard:.1f}")
        
        # Graphique des retards par service
        fig = px.bar(
            stats_df.groupby('service')['jours_retard'].sum().reset_index(),
            x='service',
            y='jours_retard',
            title="Nombre de retards par service"
        )
        st.plotly_chart(fig)
        
        # Tableau détaillé
        st.subheader("📋 Statistiques détaillées par employé")
        st.dataframe(stats_df, use_container_width=True)
    else:
        st.info("Aucune statistique disponible pour le mois en cours")

def show_gestion_conges():
    st.title("📅 Gestion des Congés")
    
    tab1, tab2, tab3, tab4 = st.tabs(["Mes Congés", "Demander un Congé", "Tous les Congés", "Approbation"])
    
    with tab1:
        st.subheader("📋 Mes demandes de congé")
        mes_conges = get_conges_employe(st.session_state.user_id)
        if not mes_conges.empty:
            st.dataframe(mes_conges, use_container_width=True)
        else:
            st.info("Vous n'avez aucune demande de congé")
    
    with tab2:
        st.subheader("➕ Nouvelle demande de congé")
        with st.form("demande_conge"):
            col1, col2 = st.columns(2)
            with col1:
                date_debut = st.date_input("Date de début", min_value=date.today())
                type_conge = st.selectbox("Type de congé", ["Congé annuel", "Maladie", "Familial", "Exceptionnel"])
            with col2:
                date_fin = st.date_input("Date de fin", min_value=date.today())
                motif = st.text_area("Motif")
            
            if st.form_submit_button("📤 Soumettre la demande"):
                if date_debut <= date_fin:
                    if verifier_disponibilite_conge(st.session_state.user_id, date_debut, date_fin):
                        if demander_conge(st.session_state.user_id, date_debut, date_fin, type_conge, motif):
                            st.success("✅ Demande de congé soumise avec succès")
                        else:
                            st.error("❌ Erreur lors de la soumission")
                    else:
                        st.error("❌ Vous avez déjà des congés qui se chevauchent avec cette période")
                else:
                    st.error("❌ La date de fin doit être après la date de début")
    
    with tab3:
        st.subheader("👥 Tous les congés")
        filtre_statut = st.selectbox("Filtrer par statut", ["Tous", "En attente", "Approuvé", "Rejeté"])
        tous_les_conges = get_tous_les_conges(filtre_statut if filtre_statut != "Tous" else "Tous")
        
        if not tous_les_conges.empty:
            st.dataframe(tous_les_conges, use_container_width=True)
        else:
            st.info("Aucun congé trouvé")
    
    with tab4:
        if st.session_state.user_role == "admin":
            st.subheader("✅ Approbation des congés")
            congés_en_attente = get_tous_les_conges("En attente")
            
            if not congés_en_attente.empty:
                for _, conge in congés_en_attente.iterrows():
                    with st.expander(f"{conge['prenom']} {conge['nom']} - {conge['date_debut']} au {conge['date_fin']}"):
                        st.write(f"**Type:** {conge['type_conge']}")
                        st.write(f"**Motif:** {conge['motif']}")
                        
                        col_btn1, col_btn2 = st.columns(2)
                        with col_btn1:
                            if st.button(f"✅ Approuver {conge['id']}"):
                                if modifier_statut_conge(conge['id'], "Approuvé"):
                                    st.success("✅ Congé approuvé")
                                    st.rerun()
                                else:
                                    st.error("❌ Erreur lors de l'approbation")
                        with col_btn2:
                            if st.button(f"❌ Rejeter {conge['id']}"):
                                if modifier_statut_conge(conge['id'], "Rejeté"):
                                    st.success("✅ Congé rejeté")
                                    st.rerun()
                                else:
                                    st.error("❌ Erreur lors du rejet")
            else:
                st.info("Aucun congé en attente d'approbation")
        else:
            st.warning("⛔ Accès réservé aux administrateurs")

def show_gestion_utilisateurs():
    st.title("👥 Gestion des Utilisateurs")
    
    if st.session_state.user_role != "admin":
        st.warning("⛔ Accès réservé aux administrateurs")
        return
    
    tab1, tab2 = st.tabs(["Liste des Utilisateurs", "Ajouter un Utilisateur"])
    
    with tab1:
        st.subheader("📋 Liste des utilisateurs")
        users_df = get_all_users()
        if not users_df.empty:
            st.dataframe(users_df, use_container_width=True)
        else:
            st.info("Aucun utilisateur enregistré")
    
    with tab2:
        st.subheader("➕ Ajouter un nouvel utilisateur")
        with st.form("ajouter_utilisateur"):
            col1, col2 = st.columns(2)
            with col1:
                username = st.text_input("Nom d'utilisateur*")
                email = st.text_input("Email")
            with col2:
                password = st.text_input("Mot de passe*", type="password")
                role = st.selectbox("Rôle", ["user", "admin"])
            
            if st.form_submit_button("➕ Ajouter l'utilisateur"):
                if username and password:
                    if create_user(username, password, role, email):
                        st.success("✅ Utilisateur ajouté avec succès")
                    else:
                        st.error("❌ Erreur lors de l'ajout de l'utilisateur")
                else:
                    st.warning("⚠️ Veuillez remplir tous les champs obligatoires")

# =========================
# Point d'entrée principal
# =========================

if __name__ == "__main__":
    # Initialisation des états de session
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
    if "user" not in st.session_state:
        st.session_state.user = None
    if "user_role" not in st.session_state:
        st.session_state.user_role = None
    if "user_id" not in st.session_state:
        st.session_state.user_id = None
    if "show_stats" not in st.session_state:
        st.session_state.show_stats = False
    
    # Vérification de la connexion à la base de données
    if not test_connection_background():
        st.error("❌ Impossible de se connecter à la base de données. Vérifiez la configuration.")
        st.stop()
    
    # Initialisation des tables
    if not create_tables():
        st.error("❌ Erreur lors de l'initialisation des tables de la base de données.")
        st.stop()
    
    # Lancement de l'application

    main()
