import datetime
import os
import signal
import time
import traceback
from copy import deepcopy
from sys import exit
import toml
import electric_meter
import setup_logging
import db_model as db
import db_postgrest_model as db_postgrest
import threading

FEHLERDATEI = "fehler_smartmeter.log"
SKRIPTPFAD = os.path.abspath(os.path.dirname(__file__))


def load_config(file):
    """Laedt die Konfiguration aus dem smartmeter86_cfg.toml File"""
    file = os.path.join(SKRIPTPFAD, file)
    with open(file) as conffile:
        config = toml.loads(conffile.read())
    return config


LOGGER = setup_logging.create_logger("smartmeter", 20)


class MessHandler:
    """
    MessHandler ist zustaendig für das organiesieren, dass Messwerte ausgelesen, gespeichert und in die Datenbank
    geschrieben werden.
    """

    def __init__(self, messregister, CONFIG):
        self.CONFIG = CONFIG
        self.schnelles_messen = False
        self.startzeit_schnelles_messen = datetime.datetime(1970, 1, 1)
        self.messregister = messregister
        self.messregister_save = deepcopy(messregister)
        self.messwerte_liste = []
        self.intervall_daten_senden = CONFIG["mess_cfg"]["intervall_daten_senden"]
        self.pausenzeit = CONFIG["mess_cfg"]["messintervall"]

    def set_schnelles_messintervall(self, *_):
        """Kommt von außerhalb das Signal USR2 wird das Mess und Sendeintervall verkuerzt"""
        self.schnelles_messen = True
        self.startzeit_schnelles_messen = datetime.datetime.utcnow()
        self.intervall_daten_senden = self.CONFIG["mess_cfg"]["schnelles_messintervall"]
        self.pausenzeit = self.CONFIG["mess_cfg"]["schnelles_messintervall"]

    def off_schnelles_messintervall(self):
        """Mess und Sendeintervall wird wieder auf Standardwerte zurueckgesetzt"""
        self.schnelles_messen = False
        self.startzeit_schnelles_messen = datetime.datetime(1970, 1, 1)
        self.intervall_daten_senden = self.CONFIG["mess_cfg"]["intervall_daten_senden"]
        self.pausenzeit = self.CONFIG["mess_cfg"]["messintervall"]

    def add_messwerte(self, messwerte):
        """Speichern der Messwerte zwischen bis zu Ihrer Uebertragung"""
        self.messwerte_liste.append(messwerte)

    def schreibe_messwerte(self, datenbankschnittstelle):
        """Gespeicherte Messwerte in die Datenbank schreiben"""
        LOGGER.debug("Sende Daten")
        datenbankschnittstelle.insert_many(self.messwerte_liste)
        self.messwerte_liste = []

    def erstelle_auszulesende_messregister(self):
        """Prueft welche Messwerte nach Ihren Intervalleinstellungen im aktuellen Durchlauf ausgelesen werden muessen"""
        if self.schnelles_messen:
            return [key for key in self.messregister]
        else:
            return [key for key in self.messregister if self.messregister[key]["verbleibender_durchlauf"] <= 1]

    def reduziere_durchlauf_anzahl(self):
        for key in self.messregister:
            self.messregister[key]["verbleibender_durchlauf"] -= 1

    def durchlauf_zuruecksetzen(self, messauftrag):
        for key in messauftrag:
            self.messregister[key]["verbleibender_durchlauf"] = deepcopy(self.messregister[key]["intervall"])


class Datenbankschnittstelle:
    def __init__(self, db_adapter, device, CONFIG):
        self.db_tables = [db.get_smartmeter_table(device)]
        self.db_table = self.db_tables[0]

        self.db_adapter = db_adapter

        if db_adapter == "postgrest":
            self.headers = {f"Authorization": "{user} {token}".format(user=CONFIG["db"]["postgrest"]["user"],
                                                                      token=CONFIG["db"]["postgrest"]["token"])}
            url = CONFIG["db"]["postgrest"]["url"]
            if not url.endswith("/"):
                url = f"{url}/"
            self.url = "{url}{table}".format(url=url,
                                             table=CONFIG["db"]["postgrest"]["table"])
            self.none_messdaten = self.__none_messdaten_dictionary_erstellen(CONFIG)
        else:
            try:
                self.headers = None
                self.url = None
                db_adapter = CONFIG["db"]["db"]
                db_ = db.init_db(CONFIG["db"][db_adapter]["database"], db_adapter, CONFIG["db"].get(db_adapter))
                db.DB_PROXY.initialize(db_)
                db.create_tables(self.db_tables)
            except:
                print("Kleiner Fehler passiert (nicht schlimm)")

    def insert_many(self, daten):
        if self.db_adapter == "postgrest":
            db_postgrest.sende_daten(self.url, self.headers, daten, self.none_messdaten, LOGGER)
        else:
            db.insert_many(daten, self.db_table)

    @staticmethod
    def __none_messdaten_dictionary_erstellen(CONFIG):
        none_daten = {"ts": None}
        for key in CONFIG["durchlaufintervall"]:
            none_daten[key.lower()] = None
        return none_daten


def schreibe_config(config, configfile):
    with open(configfile, "a", encoding="UTF-8") as file:
        file.write(f"# Nach dem wievielten Durchlauf der jeweilige Wert ausgelesen werden soll \n"
                   f"# Ausschalten mit false\n"
                   f"# Eintraege werden automatisch bei dem ersten Start erstellt, Config anschließend nochmal prüfen\n"
                   f"{toml.dumps(config)}")
    LOGGER.info("Durchlaufintervall in Config aktualisiert \n Programm wird beendet. Bitte neu starten")
    global nofailure
    nofailure = True
    exit(0)


def erzeuge_durchlaufintervall(smartmeter, file):
    register = smartmeter.get_input_keys()
    durchlaufintervall = {}
    for key in register:
        durchlaufintervall[key] = 1
    config = {"durchlaufintervall": durchlaufintervall}
    schreibe_config(config, file)


def erzeuge_messregister(smartmeter, CONFIG):
    """Erzeugt das messregister nach dem Start des Skriptes"""
    if "durchlaufintervall" in CONFIG:
        messregister = {}
        for key, value in CONFIG["durchlaufintervall"].items():
            if value:
                messregister[key] = {}
                messregister[key]["intervall"] = value
                messregister[key]["verbleibender_durchlauf"] = 0
        return messregister
    else:
        erzeuge_durchlaufintervall(smartmeter, file)


def fehlermeldung_schreiben(fehlermeldung):
    """
    Schreibt nicht abgefangene Fehlermeldungen in eine sperate Datei, um so leichter Fehlermeldungen ausfindig machen zu
    koennen welche noch Abgefangen werden muessen.
    """
    with open(os.path.join(SKRIPTPFAD, FEHLERDATEI), "a") as file:
        file.write(fehlermeldung)


def loadtest(CONFIG):
    datenbankschnittstelle = Datenbankschnittstelle(CONFIG["db"]["db"], CONFIG["mess_cfg"]["device"], CONFIG)
    return datenbankschnittstelle


class thread(threading.Thread):
    def __init__(self, configDatei: str, durchleaufe: int):
        threading.Thread.__init__(self)
        breake = 0

        CONFIG = load_config(configDatei)
        datenbankschnittstelle = loadtest(CONFIG)

        print("config file : " + str(configDatei))
        print("loaded slave : " + str(CONFIG["modbus"]["slave_addr"]))
        device = electric_meter.get_device_list().get(CONFIG["mess_cfg"]["device"])
        smartmeter = device(serial_if=CONFIG["modbus"]["serial_if"],
                            serial_if_baud=CONFIG["modbus"]["serial_if_baud"],
                            serial_if_byte=CONFIG["modbus"]["serial_if_byte"],
                            serial_if_par=CONFIG["modbus"]["serial_if_par"],
                            serial_if_stop=CONFIG["modbus"]["serial_if_stop"],
                            slave_addr=CONFIG["modbus"]["slave_addr"],
                            timeout=CONFIG["modbus"]["timeout"],
                            logger=LOGGER)

        messregister = erzeuge_messregister(smartmeter, CONFIG)
        messhandler = MessHandler(messregister, CONFIG)

        # SIGUSR2 setzt das schnelle Messintervall
        threadLock.acquire()

        signal.signal(signal.SIGUSR2, messhandler.set_schnelles_messintervall)
        threadLock.release()
        zeitpunkt_daten_gesendet = datetime.datetime(1970, 1, 1)
        start_messzeitpunkt = datetime.datetime(1970, 1, 1)

        LOGGER.info(
            "Initialisierung abgeschlossen - Start Messungen slave num : " + str(CONFIG["modbus"]["slave_addr"]))

        while True:

            now = datetime.datetime.utcnow()
            now = now.replace(microsecond=0)

            # Prüfen ob schnelles Messen aktiv ist und ob dies wieder auf Standard zurück gesetzt werden muss
            if messhandler.schnelles_messen:
                if (now - messhandler.startzeit_schnelles_messen).total_seconds() > \
                        CONFIG["mess_cfg"]["dauer_schnelles_messintervall"]:
                    messhandler.off_schnelles_messintervall()

            if (now - start_messzeitpunkt).total_seconds() > messhandler.pausenzeit:

                # Prüfe welche Messwerte auszulesen sind
                messauftrag = messhandler.erstelle_auszulesende_messregister()

                # Messauftrag abarbeiten und Zeitpunk ergänzen
                if messauftrag:
                    start_messzeitpunkt = datetime.datetime.utcnow()
                    messwerte = smartmeter.read_input_values(messauftrag)
                    LOGGER.debug("Messdauer: {}".format(datetime.datetime.utcnow() - start_messzeitpunkt))
                    messwerte["ts"] = now
                    messhandler.add_messwerte(messwerte)

                if not messhandler.schnelles_messen:
                    messhandler.reduziere_durchlauf_anzahl()
                    messhandler.durchlauf_zuruecksetzen(messauftrag)

                # Schreibe die Messdaten in die Datenbank nach eingestellten Intervall
                if (now - zeitpunkt_daten_gesendet).total_seconds() > messhandler.intervall_daten_senden:
                    start_schreiben = datetime.datetime.utcnow()
                    messhandler.schreibe_messwerte(datenbankschnittstelle)
                    LOGGER.debug("DB Dauer schreiben: {}".format(datetime.datetime.utcnow() - start_schreiben))
                    zeitpunkt_daten_gesendet = now

                    breake += 1
                    print(breake)
                LOGGER.debug("Durchlaufdauer: {}".format(datetime.datetime.utcnow() - now))

                try:
                    if durchleaufe == int(durchleaufe):
                        print("next...")
                        break
                except:
                    print("Kleiner Fehler (Kein Problem) 😊")
            time.sleep(CONFIG["mess_cfg"]["messintervall"])


if __name__ == "__main__":
    nofailure = False
    pathtoconfigdir = str(SKRIPTPFAD) + "/configs/"

    try:
        while True:
            # set path
            while os.path.isdir(pathtoconfigdir):
                for file in os.listdir(pathtoconfigdir):
                    # loop through the folder
                    if file.endswith('.toml'):
                        print(file)  # print text to keep track the process
                        threadLock = threading.Lock()
                        threads = []
                        run = thread(configDatei="configs/" + str(file), durchleaufe=10)
                        run.start()
                        run.join()
                        time.sleep(2)
                    elif os.path.isdir(os.path.join(pathtoconfigdir, file)):  # if it is a subfolder
                        print(os.path.join(pathtoconfigdir, file))
                        pathtoconfigdir = os.path.join(pathtoconfigdir, file)
                        print('is dir')
                        break
                    else:
                        pathtoconfigdir = os.path.join(pathtoconfigdir, file)

    finally:
        if not nofailure:
            fehlermeldung_schreiben(traceback.format_exc())
            LOGGER.exception("Schwerwiegender Fehler aufgetreten")
