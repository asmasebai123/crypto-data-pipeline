#!/usr/bin/env python3
"""
🚀 SCRIPT MAÎTRE : Lance le pipeline COMPLET avec une seule commande

Cela lance :
  ✅ Kafka (Docker)
  ✅ Ingestion Batch (main.py) - chaque 10 min
  ✅ Kafka Producer - chaque 5 sec
  ✅ Kafka Consumer - continu
  ✅ Dashboard Streamlit
  ✅ Transformations

Utilisation :
  python run_pipeline.py [options]

Options :
  --mode batch          : Batch seulement (pas Kafka)
  --mode stream         : Streaming seulement (Kafka)
  --mode full           : Complet (batch + streaming + Kafka) [DÉFAUT]
  --docker              : Lancer Kafka avec Docker
  --local-kafka         : Kafka doit déjà tourner
  --no-dashboard        : Sans dashboard Streamlit
  --help                : Afficher cette aide
"""

import sys
import os
import subprocess
import time
import threading
import logging
from datetime import datetime
from pathlib import Path

# Configuration logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/pipeline.log", encoding="utf-8")
    ]
)
logger = logging.getLogger("pipeline_master")

# ─── CONFIGURATION ────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent
INGESTION_DIR = PROJECT_ROOT / "ingestion"
DASHBOARD_DIR = PROJECT_ROOT / "dashboard"

# Mode d'exécution
MODE = "full"  # batch | stream | full
USE_DOCKER = True
USE_DASHBOARD = True

# ─── COULEURS POUR LES LOGS ──────────────────────────────
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

# ─── GESTION DES PROCESSUS ───────────────────────────────
processes = []
threads = []

def print_header(title):
    """Affiche un titre"""
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'='*70}{Colors.ENDC}")
    print(f"{Colors.BOLD}{Colors.BLUE}{title:^70}{Colors.ENDC}")
    print(f"{Colors.BOLD}{Colors.BLUE}{'='*70}{Colors.ENDC}\n")

def print_section(title):
    """Affiche une section"""
    print(f"\n{Colors.BOLD}{Colors.CYAN}>>> {title}{Colors.ENDC}")

def run_command(name, cmd, cwd=None, env=None):
    """
    Lance une commande dans un processus séparé

    Paramètres :
        name : nom du processus (pour logs)
        cmd : commande à exécuter (liste)
        cwd : répertoire de travail
        env : variables d'environnement
    """
    logger.info(f"🚀 Démarrage : {name}")
    print(f"{Colors.GREEN}✓ Lancement {name}{Colors.ENDC}")

    try:
        env_copy = os.environ.copy() if env is None else env
        process = subprocess.Popen(
            cmd,
            cwd=cwd,
            env=env_copy,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )
        processes.append((name, process))

        # Lire les logs du processus
        while True:
            output = process.stdout.readline()
            if not output:
                break
            print(f"[{name}] {output.rstrip()}")

    except Exception as e:
        logger.error(f"❌ Erreur {name}: {e}")
        print(f"{Colors.RED}✗ Erreur {name}: {e}{Colors.ENDC}")

def run_command_background(name, cmd, cwd=None, env=None):
    """Lance une commande en arrière-plan (dans un thread)"""
    thread = threading.Thread(
        target=run_command,
        args=(name, cmd, cwd, env),
        daemon=True
    )
    thread.start()
    threads.append(thread)
    time.sleep(2)  # Laisser le temps au processus de démarrer

# ─── KAFKA ────────────────────────────────────────────────
def check_kafka_running():
    """Vérifie si Kafka tourne sur localhost:9092"""
    try:
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex(('localhost', 9092))
        sock.close()
        return result == 0
    except:
        return False

def start_kafka_docker():
    """Lance Kafka avec Docker"""
    print_section("Démarrage Kafka avec Docker")

    # Vérifier que Docker est installé
    try:
        subprocess.run(["docker", "--version"], capture_output=True, check=True)
    except:
        logger.error("Docker n'est pas installé")
        print(f"{Colors.RED}❌ Docker n'est pas installé. Installe Docker Desktop.{Colors.ENDC}")
        return False

    # Lancer docker-compose
    try:
        logger.info("Lancement docker-compose up -d kafka")
        result = subprocess.run(
            ["docker-compose", "up", "-d", "kafka", "zookeeper"],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True
        )

        if result.returncode == 0:
            logger.info("Kafka démarré avec Docker")
            print(f"{Colors.GREEN}✓ Kafka démarré (Docker){Colors.ENDC}")
            time.sleep(5)  # Attendre que Kafka soit prêt
            return True
        else:
            logger.error(f"Erreur Docker: {result.stderr}")
            print(f"{Colors.RED}✗ Erreur Docker: {result.stderr}{Colors.ENDC}")
            return False

    except Exception as e:
        logger.error(f"Erreur lancement Docker: {e}")
        print(f"{Colors.RED}✗ Erreur Docker: {e}{Colors.ENDC}")
        return False

def stop_kafka_docker():
    """Arrête Kafka"""
    print_section("Arrêt Kafka")
    try:
        subprocess.run(
            ["docker-compose", "down"],
            cwd=PROJECT_ROOT,
            capture_output=True
        )
        logger.info("Kafka arrêté")
        print(f"{Colors.GREEN}✓ Kafka arrêté{Colors.ENDC}")
    except:
        pass

# ─── INGESTION BATCH ──────────────────────────────────────
def start_batch_ingestion():
    """Lance l'ingestion batch (main.py)"""
    print_section("Démarrage Ingestion Batch (main.py)")

    cmd = [sys.executable, "-u", "main.py"]
    run_command_background("Ingestion Batch", cmd, cwd=INGESTION_DIR)

# ─── KAFKA PRODUCER ──────────────────────────────────────
def start_kafka_producer():
    """Lance le producteur Kafka"""
    print_section("Démarrage Kafka Producer")

    cmd = [sys.executable, "-u", "kafka_producer.py"]
    run_command_background("Kafka Producer", cmd, cwd=INGESTION_DIR)

# ─── KAFKA CONSUMER ──────────────────────────────────────
def start_kafka_consumer():
    """Lance le consommateur Kafka"""
    print_section("Démarrage Kafka Consumer")

    cmd = [sys.executable, "-u", "kafka_consumer.py"]
    run_command_background("Kafka Consumer", cmd, cwd=INGESTION_DIR)

# ─── DASHBOARD STREAMLIT ──────────────────────────────────
def start_dashboard():
    """Lance le dashboard Streamlit"""
    print_section("Démarrage Dashboard Streamlit")

    cmd = [sys.executable, "-m", "streamlit", "run", "app.py", "--logger.level=warning"]
    run_command_background("Dashboard Streamlit", cmd, cwd=DASHBOARD_DIR)

    print(f"\n{Colors.BOLD}{Colors.GREEN}🌐 Dashboard disponible sur :{Colors.ENDC}")
    print(f"   http://localhost:8501\n")

# ─── TRANSFORMATIONS SPARK ───────────────────────────────
def start_spark_transformations():
    """Lance les transformations Spark (optionnel)"""
    print_section("Transformations Spark (optionnel)")

    transform_file = PROJECT_ROOT / "transformations" / "run_transforms.py"

    if transform_file.exists():
        print(f"{Colors.YELLOW}ℹ️  Les transformations Spark peuvent être lancées manuellement :{Colors.ENDC}")
        print(f"   python transformations/run_transforms.py --engine auto\n")
    else:
        logger.warning("Fichier transformations/run_transforms.py non trouvé")

# ─── AFFICHAGE STATUT ─────────────────────────────────────
def print_status():
    """Affiche le statut de tous les processus"""
    print_section("Statut des Composants")

    status = {
        "Kafka": "🟢 EN COURS" if check_kafka_running() else "🔴 ARRÊTÉ",
        "Batch Ingestion": "🟢 EN COURS",
        "Kafka Producer": "🟢 EN COURS",
        "Kafka Consumer": "🟢 EN COURS",
        "Dashboard": "🟢 EN COURS (http://localhost:8501)"
    }

    for component, state in status.items():
        print(f"  {component:<25} {state}")

# ─── GRACEFUL SHUTDOWN ────────────────────────────────────
def cleanup():
    """Arrête tous les processus proprement"""
    print_section("Arrêt du Pipeline")

    logger.info("Fermeture des processus...")
    print(f"{Colors.YELLOW}⏹️  Arrêt des composants...{Colors.ENDC}")

    # Arrêter les processus
    for name, process in processes:
        try:
            process.terminate()
            process.wait(timeout=5)
            logger.info(f"✓ {name} arrêté")
        except subprocess.TimeoutExpired:
            process.kill()
            logger.warning(f"⚠️  {name} forcé d'arrêter")

    # Arrêter Kafka
    if USE_DOCKER:
        stop_kafka_docker()

    logger.info("Pipeline arrêté")
    print(f"{Colors.GREEN}✓ Pipeline arrêté{Colors.ENDC}\n")

# ─── AFFICHAGE D'AIDE ─────────────────────────────────────
def show_help():
    """Affiche l'aide"""
    print(__doc__)

# ─── PARSING DES ARGUMENTS ────────────────────────────────
def parse_args():
    """Parse les arguments de la ligne de commande"""
    global MODE, USE_DOCKER, USE_DASHBOARD

    for i, arg in enumerate(sys.argv[1:]):
        if arg == "--mode" and i+1 < len(sys.argv)-1:
            MODE = sys.argv[i+2]
        elif arg == "--docker":
            USE_DOCKER = True
        elif arg == "--local-kafka":
            USE_DOCKER = False
        elif arg == "--no-dashboard":
            USE_DASHBOARD = False
        elif arg == "--help" or arg == "-h":
            show_help()
            sys.exit(0)

# ─── MAIN ─────────────────────────────────────────────────
def main():
    """Lance le pipeline complet"""
    global MODE

    print_header("🚀 CRYPTO DATA PIPELINE - LANCEMENT COMPLET")

    print(f"{Colors.BOLD}Mode sélectionné :{Colors.ENDC} {MODE.upper()}")
    print(f"{Colors.BOLD}Kafka :{Colors.ENDC} {'Docker' if USE_DOCKER else 'Local'}")
    print(f"{Colors.BOLD}Dashboard :{Colors.ENDC} {'Oui' if USE_DASHBOARD else 'Non'}\n")

    # Créer le dossier logs
    os.makedirs("logs", exist_ok=True)

    logger.info("="*70)
    logger.info(f"Démarrage pipeline - Mode: {MODE}")
    logger.info("="*70)

    try:
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # KAFKA (si mode stream ou full)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

        if MODE in ["stream", "full"]:
            if USE_DOCKER:
                if not start_kafka_docker():
                    print(f"{Colors.RED}❌ Impossible de démarrer Kafka. Continue sans Kafka...{Colors.ENDC}")
                    MODE = "batch"
            else:
                if not check_kafka_running():
                    print(f"{Colors.RED}❌ Kafka n'est pas en cours d'exécution sur localhost:9092{Colors.ENDC}")
                    print(f"{Colors.YELLOW}   Démarre Kafka localement d'abord :{Colors.ENDC}")
                    print(f"   - bin/kafka-server-start.sh config/server.properties (Linux/Mac)")
                    print(f"   - bin\\windows\\kafka-server-start.bat config\\server.properties (Windows)")
                    MODE = "batch"
                else:
                    print(f"{Colors.GREEN}✓ Kafka détecté sur localhost:9092{Colors.ENDC}")

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # INGESTION BATCH
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

        if MODE in ["batch", "full"]:
            start_batch_ingestion()

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # KAFKA PRODUCER & CONSUMER
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

        if MODE in ["stream", "full"]:
            start_kafka_producer()
            time.sleep(1)
            start_kafka_consumer()

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # TRANSFORMATIONS SPARK
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

        start_spark_transformations()

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # DASHBOARD STREAMLIT
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

        if USE_DASHBOARD:
            start_dashboard()

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # RÉSUMÉ
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

        print_header("✅ PIPELINE LANCÉ AVEC SUCCÈS")

        print(f"\n{Colors.BOLD}Composants en cours d'exécution :{Colors.ENDC}")
        print(f"  {Colors.GREEN}✓{Colors.ENDC} Kafka (Docker)" if MODE in ["stream", "full"] and USE_DOCKER else "")
        print(f"  {Colors.GREEN}✓{Colors.ENDC} Ingestion Batch" if MODE in ["batch", "full"] else "")
        print(f"  {Colors.GREEN}✓{Colors.ENDC} Kafka Producer" if MODE in ["stream", "full"] else "")
        print(f"  {Colors.GREEN}✓{Colors.ENDC} Kafka Consumer" if MODE in ["stream", "full"] else "")
        print(f"  {Colors.GREEN}✓{Colors.ENDC} Dashboard Streamlit" if USE_DASHBOARD else "")

        print(f"\n{Colors.BOLD}Où accéder :{Colors.ENDC}")
        print(f"  🌐 Dashboard : http://localhost:8501")
        print(f"  📁 Logs      : logs/pipeline.log")

        print(f"\n{Colors.BOLD}Pour arrêter :{Colors.ENDC}")
        print(f"  Appuie sur {Colors.BOLD}CTRL + C{Colors.ENDC}")

        print(f"\n{Colors.YELLOW}⏳ Pipeline en cours d'exécution...{Colors.ENDC}\n")

        logger.info(f"Pipeline lancé en mode {MODE}")

        # Garder le processus principal en vie
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print(f"\n{Colors.YELLOW}⏹️  Arrêt du pipeline...{Colors.ENDC}")
        logger.info("Arrêt du pipeline (KeyboardInterrupt)")
        cleanup()
        sys.exit(0)

    except Exception as e:
        logger.error(f"Erreur : {e}", exc_info=True)
        print(f"{Colors.RED}❌ Erreur : {e}{Colors.ENDC}")
        cleanup()
        sys.exit(1)

# ─── POINT D'ENTRÉE ───────────────────────────────────────
if __name__ == "__main__":
    parse_args()
    main()
