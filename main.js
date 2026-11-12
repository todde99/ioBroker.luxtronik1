/**
 * luxtronik1 adapter - hardened & refactored version
 */

/* jshint -W097 */
/* jslint node: true */
'use strict';

const utils = require('@iobroker/adapter-core');
const net = require('net');

let adapter;
let deviceIpAddress;
let port;
let polling = null;
let unloaded = false;

// --- Status- & Sperrvariablen ---
let activeSocket = null;
let isProcessingQueue = false;
let schaltzbwblock = false;
let zeitblock = false;
let lastData3400 = null;
let lastData2100 = null;
let lastsetbws = null;
let bwretry = 0;

// Fehlerzähler pro Befehl
const errorCounters = {};
const timers = new Set();

// Warteschlangen: Steuerbefehle (commandQueue) haben Vorrang vor Abfragen (pollQueue)
let pollQueue = [];
let commandQueue = [];
let currentPollCycleActive = false;

const MODUS = ['AUTO', 'ZWE', 'Party', 'Ferien', 'Aus', 'Aus'];
const NUMERIC_CONTROLS = [
  'control.BWs',
  'control.NachtAbs',
  'control.ParaVHK',
  'control.EndpunktHK',
  'control.AbwRLs',
  'control.HystBWs',
  'control.HystHKs'
];

const ERROR_CODES = {
  '11': 'kein Fehler',
  '701': 'Niederdruckstörung',
  '702': 'Niederdrucksperre',
  '703': 'Frostschutz',
  '704': 'Heissgasstörung',
  '705': 'Motorschutz VEN',
  '706': '',
  '707': 'Codierung WP',
  '708': 'Fühler Rücklauf',
  '709': 'Fühler Vorlauf',
  '710': 'Fühler Heissgas',
  '711': 'Fühler Aussentemp.',
  '712': 'Fühler Brauchwasser',
  '713': 'Fühler WQ-Ein',
  '714': 'Heissgas BW',
  '715': 'Hochdruck-Abschalt.',
  '716': 'Hochdruckstörung',
  '717': 'Durchfluss-WQ',
  '718': 'Max. Aussentemp.',
  '719': 'Min. Aussentemp.',
  '720': 'WQ-Temperatur',
  '721': 'Niederdruckabschaltung',
  '722': 'Tempdiff Heizwasser',
  '723': 'Tempdiff Brauchw.',
  '724': 'Tempdiff Abtauen',
  '725': 'Anlagefehler BW',
  '726': 'Fühler Mischkreis 1',
  '727': 'Soledruck',
  '728': 'Fühler WQ-Aus',
  '729': 'Drehfeldfehler',
  '730': 'Leistung Ausheizen',
  '731': '',
  '732': 'Störung Kühlung',
  '733': 'Störung Anode',
  '734': 'Störung Anode',
  '735': 'Fühler Ext. En',
  '736': 'Fühler Solarkollektor',
  '737': 'Fühler Solarspeicher',
  '738': 'Fühler Mischkreis2',
  '739': 'CAN-Fehler: WP fehlt',
  '740': 'CAN-Fehler: Timeout',
  '741': 'CAN-Fehler: Bus off',
  '742': 'CAN-Fehler: Daten',
  '743': 'CAN-Fehler: Adresse',
  '744': '',
  '745': 'Modem-Fehler'
};

// --- Timer- & Shutdown-Management ---

function later(fn, ms) {
  const t = setTimeout(() => {
    timers.delete(t);
    if (unloaded) return;
    try {
      fn();
    } catch (e) {
      adapter && adapter.log.warn('Timer-Fehler: ' + e);
    }
  }, ms);
  timers.add(t);
  return t;
}

function clearTimer(t) {
  if (t) {
    clearTimeout(t);
    timers.delete(t);
  }
}

function shutdown() {
  unloaded = true;
  if (polling) {
    clearInterval(polling);
    polling = null;
  }
  timers.forEach((t) => clearTimeout(t));
  timers.clear();
  pollQueue = [];
  commandQueue = [];
  isProcessingQueue = false;
  if (activeSocket) {
    try {
      activeSocket.removeAllListeners();
      activeSocket.on('error', () => {});
      activeSocket.destroy();
    } catch (e) { /* ignore */ }
    activeSocket = null;
  }
}

function resetConnectionState(reason) {
  adapter.log.warn('Verbindung zur Luxtronik wird zurückgesetzt (' + reason + ')');
  pollQueue = [];
  currentPollCycleActive = false;
  Object.keys(errorCounters).forEach((k) => { errorCounters[k] = 0; });
  adapter.setState('info.connection', false, true);
}

// --- Zentrale Queue-Steuerung ---

function enqueueCommand(name, taskFn) {
  commandQueue.push({ name, taskFn });
  processQueues();
}

function enqueuePoll(name, taskFn) {
  pollQueue.push({ name, taskFn });
  processQueues();
}

function processQueues() {
  if (unloaded || isProcessingQueue || activeSocket) return;

  // Steuerbefehle haben immer Vorrang vor normalen Poll-Abfragen
  const nextItem = commandQueue.shift() || pollQueue.shift();
  if (!nextItem) {
    currentPollCycleActive = false;
    return;
  }

  isProcessingQueue = true;
  try {
    nextItem.taskFn(() => {
      isProcessingQueue = false;
      // Kurze Pause (300 ms) zur Entlastung des RS232-Konverters vor dem nächsten Request
      later(processQueues, 300);
    });
  } catch (e) {
    adapter.log.warn('Fehler bei Ausführung von ' + nextItem.name + ': ' + e);
    isProcessingQueue = false;
    later(processQueues, 500);
  }
}

// --- Generische Socket-Kommunikation (Lesen & Schreiben) ---

/**
 * Führt eine Lese-Abfrage durch (z. B. 1100, 1800, 2100, 3200, 3400, 3405, 3505)
 */
function executeRead(cmd, isCompleteFn, parseFn, doneCallback) {
  if (unloaded) return doneCallback();

  let rxBuffer = '';
  let timedOut = false;
  let hadError = false;
  let connected = false;

  const idleTimeout = parseInt(adapter.config.socketTimeout, 10) || 15000;
  const socket = new net.Socket();
  activeSocket = socket;

  socket.setTimeout(idleTimeout);

  const hardTimer = later(() => {
    if (!socket.destroyed) {
      adapter.log.warn(cmd + ': Anfrage dauert zu lange (Hard-Timeout), Verbindung wird geschlossen');
      timedOut = true;
      socket.destroy();
    }
  }, idleTimeout * 3);

  socket.on('connect', () => {
    connected = true;
    adapter.log.debug(cmd + ': Verbunden mit Luxtronik');
    socket.write(cmd + '\r\n');
  });

  socket.on('data', (chunk) => {
    rxBuffer += chunk.toString('utf8');
    try {
      if (isCompleteFn(rxBuffer)) {
        adapter.log.debug(cmd + ': Daten vollständig empfangen');
        socket.destroy();
      }
    } catch (e) {
      adapter.log.warn(cmd + ': Fehler bei Vollständigkeitsprüfung: ' + e);
    }
  });

  socket.on('timeout', () => {
    timedOut = true;
    if (connected) {
      adapter.log.warn(cmd + ': Timeout nach ' + idleTimeout + ' ms (' + rxBuffer.length + ' Zeichen empfangen)');
    } else {
      adapter.log.warn(cmd + ': Keine Verbindung zum RS232-Konverter möglich (Timeout ' + idleTimeout + ' ms)');
    }
    socket.destroy();
  });

  socket.on('error', (err) => {
    hadError = true;
    adapter.log.warn(cmd + ' Socket-Fehler: ' + err.message);
  });

  socket.on('close', () => {
    clearTimer(hardTimer);
    if (activeSocket === socket) activeSocket = null;

    if (hadError || timedOut) {
      adapter.setState('info.connection', false, true);
      if (timedOut && pollQueue.length > 0) {
        adapter.log.debug('Abfragezyklus wegen Timeout abgebrochen (' + pollQueue.length + ' Abfragen übersprungen)');
        pollQueue = [];
        currentPollCycleActive = false;
      }
      return doneCallback(false);
    }

    let success = false;
    try {
      success = parseFn(rxBuffer.trim());
    } catch (e) {
      adapter.log.warn('callluxtronik' + cmd + ' - Parse-Fehler: ' + e);
      success = false;
    }

    if (success) {
      errorCounters[cmd] = 0;
    } else {
      errorCounters[cmd] = (errorCounters[cmd] || 0) + 1;
      adapter.log.debug('Datensatz ' + cmd + ' unvollständig/ungültig (Fehler #' + errorCounters[cmd] + ')');
      if (errorCounters[cmd] > 4) {
        resetConnectionState('Mehrfach unvollständiger Datensatz ' + cmd);
      }
    }

    doneCallback(success);
  });

  socket.connect(port, deviceIpAddress);
}

/**
 * Führt ein Schreib-Kommando durch (z. B. 3401, 2101, 3406, 3506, 3501, 3201, 2701)
 * Unterstützt automatische 779-Fehlerbehebung (Abbruch & Reset des Editiermodus)
 */
function executeWrite(cmd, payloadLine, onCloseParseFn, doneCallback) {
  if (unloaded) return doneCallback();

  let rxBuffer = '';
  let recoveryTriggered = false;
  let stepTimer1 = null;
  let stepTimer2 = null;

  const idleTimeout = parseInt(adapter.config.socketTimeout, 10) || 15000;
  const socket = new net.Socket();
  activeSocket = socket;

  socket.setTimeout(idleTimeout);

  const hardTimer = later(() => {
    if (!socket.destroyed) {
      adapter.log.warn(cmd + ' (Write): Hard-Timeout erreicht, Verbindung wird geschlossen');
      socket.destroy();
    }
  }, idleTimeout * 3);

  function safeWrite(str) {
    if (!socket.destroyed && socket.writable) {
      socket.write(str);
    }
  }

  socket.on('connect', () => {
    adapter.log.debug(cmd + ': Verbunden mit Luxtronik (Write)');
    safeWrite(cmd + '\r\n');
    stepTimer1 = later(() => {
      if (!recoveryTriggered) safeWrite(payloadLine + '\r\n');
    }, 1500);
    stepTimer2 = later(() => {
      if (!recoveryTriggered) safeWrite('999\r\n');
    }, 3000);
  });

  socket.on('data', (chunk) => {
    rxBuffer += chunk.toString('utf8');
    try {
      // Fehler 779 = Luxtronik ist noch in einem alten Programmiermodus blockiert
      if (rxBuffer.includes('779') && !recoveryTriggered) {
        recoveryTriggered = true;
        clearTimer(stepTimer1);
        clearTimer(stepTimer2);
        adapter.log.warn(cmd + ': Kommunikationsstörung (779) erkannt, führe Reset-Sequenz aus...');

        later(() => safeWrite(cmd + '\r\n'), 300);
        later(() => safeWrite(cmd + ';0\r\n'), 1000);
        later(() => safeWrite('999\r\n'), 1800);
        later(() => {
          if (!socket.destroyed) socket.destroy();
        }, 2500);
        return;
      }

      if (rxBuffer.includes('993\r\n999') || rxBuffer.includes('993\n999')) {
        socket.destroy();
      }
    } catch (e) {
      adapter.log.debug(cmd + ': Fehler bei Störungsbehebung: ' + e);
    }
  });

  socket.on('timeout', () => {
    adapter.log.warn(cmd + ' (Write): Timeout nach ' + idleTimeout + ' ms');
    socket.destroy();
  });

  socket.on('error', (err) => {
    adapter.log.warn(cmd + ' (Write) Socket-Fehler: ' + err.message);
  });

  socket.on('close', () => {
    clearTimer(hardTimer);
    clearTimer(stepTimer1);
    clearTimer(stepTimer2);
    if (activeSocket === socket) activeSocket = null;

    try {
      if (rxBuffer !== '' && !recoveryTriggered && onCloseParseFn) {
        onCloseParseFn(rxBuffer.trim());
      } else if (recoveryTriggered) {
        adapter.log.warn(cmd + ': Editiermodus wurde nach Fehler 779 zurückgesetzt. Befehl wird erneut eingereiht.');
        // Nach 779-Recovery den eigentlichen Schreibbefehl einmal automatisch wiederholen
        later(() => {
          enqueueCommand(cmd + '_retry', (cb) => executeWrite(cmd, payloadLine, onCloseParseFn, cb));
        }, 1000);
      }
    } catch (e) {
      adapter.log.warn('callluxtronik' + cmd + ' - Fehler: ' + e);
    }

    adapter.log.debug('Daten ' + cmd + ' fertig verarbeitet.');
    doneCallback(!recoveryTriggered);
  });

  socket.connect(port, deviceIpAddress);
}

// --- Hilfsfunktionen für Protokoll-Prüfungen ---

function isSingleLineComplete(expectedCmd, buffer) {
  if (!buffer.includes(expectedCmd)) return false;
  if (!buffer.endsWith('\n') && !buffer.endsWith('\r')) return false;
  const clean = buffer.replace(/(\r\n|\n|\r)/gm, '');
  const parts = clean.split(';');
  const expectedCount = parseInt(parts[1], 10);
  return !Number.isNaN(expectedCount) && parts.length === expectedCount + 2;
}

// --- Spezifische Lese-Funktionen ---

function callluxtronik1100(done) {
  executeRead(
    '1100',
    (buf) => isSingleLineComplete('1100', buf),
    (raw) => {
      if (raw.length <= 40) return false;
      const arr = raw.replace(/(\r\n|\n|\r)/gm, '').split(';');
      if (arr.length !== parseInt(arr[1], 10) + 2) return false;

      if (lastsetbws !== null && Number(arr[8]) !== Number(lastsetbws)) {
        adapter.log.info('BW-Solltemperatur nicht korrekt gesetzt (' + arr[8] + ' statt ' + lastsetbws + '), Vorgang wird wiederholt');
        if ((errorCounters['1100_bws'] || 0) < 5) {
          errorCounters['1100_bws'] = (errorCounters['1100_bws'] || 0) + 1;
          controlbws(lastsetbws);
        } else {
          errorCounters['1100_bws'] = 0;
          resetConnectionState('Setzen der BW-Solltemperatur mehrmals fehlgeschlagen');
        }
        return true;
      }

      errorCounters['1100_bws'] = 0;
      adapter.setState('temperaturen.VL', arr[2] / 10, true);
      adapter.setState('temperaturen.RL', arr[3] / 10, true);
      adapter.setState('temperaturen.RLs', arr[4] / 10, true);
      adapter.setState('temperaturen.HG', arr[5] / 10, true);
      adapter.setState('temperaturen.AUT', arr[6] / 10, true);
      adapter.setState('temperaturen.BWi', arr[7] / 10, true);
      adapter.setState('temperaturen.BWs', arr[8] / 10, true);
      adapter.setState('temperaturen.WQe', arr[9] / 10, true);
      adapter.setState('temperaturen.WQa', arr[10] / 10, true);
      adapter.setState('temperaturen.MK1VLi', arr[11] / 10, true);
      adapter.setState('temperaturen.MK1VLs', arr[12] / 10, true);
      adapter.setState('temperaturen.RS', arr[13] / 10, true);
      return true;
    },
    done
  );
}

function callluxtronik1800(done) {
  executeRead(
    '1800',
    (buf) => buf.includes('1800;8', 10) && (buf.endsWith('\n') || buf.endsWith('\r')),
    (raw) => {
      if (raw.length <= 500) return false;
      const lines = raw.split(/\r?\n/);
      if (lines.length <= 21) return false;

      const temperaturen = lines[2].split(';'); // 1100
      adapter.setState('temperaturen.VL', temperaturen[2] / 10, true);
      adapter.setState('temperaturen.RL', temperaturen[3] / 10, true);
      adapter.setState('temperaturen.RLs', temperaturen[4] / 10, true);
      adapter.setState('temperaturen.HG', temperaturen[5] / 10, true);
      adapter.setState('temperaturen.AUT', temperaturen[6] / 10, true);
      adapter.setState('temperaturen.BWi', temperaturen[7] / 10, true);
      adapter.setState('temperaturen.BWs', temperaturen[8] / 10, true);
      if (currentPollCycleActive) {
        adapter.setState('control.BWs', temperaturen[8] / 10, true);
      }
      adapter.setState('temperaturen.WQe', temperaturen[9] / 10, true);
      adapter.setState('temperaturen.WQa', temperaturen[10] / 10, true);
      adapter.setState('temperaturen.MK1VLi', temperaturen[11] / 10, true);
      adapter.setState('temperaturen.MK1VLs', temperaturen[12] / 10, true);
      adapter.setState('temperaturen.RS', temperaturen[13] / 10, true);

      const eingaenge = lines[3].split(';'); // 1200
      adapter.setState('eingaenge.ASD', !!+eingaenge[2], true);
      adapter.setState('eingaenge.EVU', !!+eingaenge[3], true);
      adapter.setState('eingaenge.HD', !!+eingaenge[4], true);
      adapter.setState('eingaenge.MOT', !!+eingaenge[5], true);
      adapter.setState('eingaenge.ND', !!+eingaenge[6], true);
      adapter.setState('eingaenge.PEX', !!+eingaenge[7], true);

      const ausgaenge = lines[4].split(';'); // 1300
      adapter.setState('ausgaenge.Abtauventil', !!+ausgaenge[2], true);
      adapter.setState('ausgaenge.Brauchwarmwasserumwaelzpumpe', !!+ausgaenge[3], true);
      adapter.setState('ausgaenge.Fussbodenheizungsumwaelzpumpe', !!+ausgaenge[4], true);
      adapter.setState('ausgaenge.Heizungsumwaelzpumpe', !!+ausgaenge[5], true);
      adapter.setState('ausgaenge.Mischer_1_Auf', !!+ausgaenge[6], true);
      adapter.setState('ausgaenge.Mischer_1_Zu', !!+ausgaenge[7], true);
      adapter.setState('ausgaenge.Ventilation_des_Waermepumpengehaeuses', !!+ausgaenge[8], true);
      adapter.setState('ausgaenge.Ventilator_Brunnen_oder_Soleumwaelzpumpe', !!+ausgaenge[9], true);
      adapter.setState('ausgaenge.Verdichter_1_in_Waermepumpe', !!+ausgaenge[10], true);
      adapter.setState('ausgaenge.Verdichter_2_in_Waermepumpe', !!+ausgaenge[11], true);
      adapter.setState('ausgaenge.Zusatzumwaelzpumpe_Zirkulationspumpe', !!+ausgaenge[12], true);
      adapter.setState('ausgaenge.Zweiter_Waermeerzeuger_1', !!+ausgaenge[13], true);
      adapter.setState('ausgaenge.Zweiter_Waermeerzeuger_2_Sammelstoerung', !!+ausgaenge[14], true);

      const ablaufzeiten = lines[5].split(';'); // 1400
      const currentWpSeit = parseInt(ablaufzeiten[2], 10) * 3600 + parseInt(ablaufzeiten[3], 10) * 60 + parseInt(ablaufzeiten[4], 10);

      adapter.getState('ablaufzeiten.WPseit', (err, state) => {
        if (state && currentWpSeit === 0 && state.val > 0) {
          adapter.setState('ablaufzeiten.WPseitlast', state.val, true);
        }
        adapter.setState('ablaufzeiten.WPseit', currentWpSeit, true);
      });

      adapter.setState('ablaufzeiten.ZWE1seit', parseInt(ablaufzeiten[5], 10) * 3600 + parseInt(ablaufzeiten[6], 10) * 60 + parseInt(ablaufzeiten[7], 10), true);
      adapter.setState('ablaufzeiten.ZWE2seit', parseInt(ablaufzeiten[8], 10) * 3600 + parseInt(ablaufzeiten[9], 10) * 60 + parseInt(ablaufzeiten[10], 10), true);
      adapter.setState('ablaufzeiten.Netzeinv', parseInt(ablaufzeiten[11], 10), true);
      adapter.setState('ablaufzeiten.SSPstand', parseInt(ablaufzeiten[12], 10) * 60 + parseInt(ablaufzeiten[13], 10), true);
      adapter.setState('ablaufzeiten.SSPverz', parseInt(ablaufzeiten[14], 10) * 60 + parseInt(ablaufzeiten[15], 10), true);
      adapter.setState('ablaufzeiten.VDstand', parseInt(ablaufzeiten[16], 10) * 3600 + parseInt(ablaufzeiten[17], 10) * 60 + parseInt(ablaufzeiten[18], 10), true);
      adapter.setState('ablaufzeiten.HRM', parseInt(ablaufzeiten[19], 10) * 3600 + parseInt(ablaufzeiten[20], 10) * 60 + parseInt(ablaufzeiten[21], 10), true);
      adapter.setState('ablaufzeiten.HRW', parseInt(ablaufzeiten[22], 10) * 3600 + parseInt(ablaufzeiten[23], 10) * 60 + parseInt(ablaufzeiten[24], 10), true);
      adapter.setState('ablaufzeiten.TDIseit', parseInt(ablaufzeiten[25], 10) * 3600 + parseInt(ablaufzeiten[26], 10) * 60 + parseInt(ablaufzeiten[27], 10), true);
      adapter.setState('ablaufzeiten.BWsperre', parseInt(ablaufzeiten[28], 10) * 3600 + parseInt(ablaufzeiten[29], 10) * 60 + parseInt(ablaufzeiten[30], 10), true);

      const betriebsstunden = lines[6].split(';');
      adapter.setState('betriebsstunden.VD1', toTimeString(betriebsstunden[2]), true);
      adapter.setState('betriebsstunden.Imp1', betriebsstunden[3], true);
      adapter.setState('betriebsstunden.AvVD1', toTimeString(betriebsstunden[4]), true);
      adapter.setState('betriebsstunden.VD2', toTimeString(betriebsstunden[5]), true);
      adapter.setState('betriebsstunden.Imp2', betriebsstunden[6], true);
      adapter.setState('betriebsstunden.AvVD2', toTimeString(betriebsstunden[7]), true);
      adapter.setState('betriebsstunden.ZWE1', toTimeString(betriebsstunden[8]), true);
      adapter.setState('betriebsstunden.ZWE2', toTimeString(betriebsstunden[9]), true);
      adapter.setState('betriebsstunden.WP', toTimeString(betriebsstunden[10]), true);

      for (let i = 1; i < 6; i++) {
        adapter.setState('fehler.' + (6 - i), setfehlertext(lines[7 + i]), true);
        adapter.setState('abschaltungen.' + (6 - i), setabschalttext(lines[14 + i]), true);
      }

      const statusParts = lines[21].split(';');
      adapter.setState('status.WPtyp', String(statusParts[2]), true);
      adapter.setState('status.SW', String(statusParts[3]), true);
      adapter.setState('status.BivStufe', String(statusParts[4]), true);
      adapter.setState('status.ANL', setstatustext(String(lines[21])), true);

      adapter.setState('info.connection', true, true);
      return true;
    },
    done
  );
}

function callluxtronik2100(done) {
  executeRead(
    '2100',
    (buf) => isSingleLineComplete('2100', buf),
    (raw) => {
      const arr = raw.replace(/(\r\n|\n|\r)/gm, '').split(';');
      if (arr.length !== parseInt(arr[1], 10) + 2) return false;

      lastData2100 = arr.slice();
      adapter.setState('temperaturen.einstellungen.RLBegr', arr[2] / 10, true);
      adapter.setState('temperaturen.einstellungen.HystHR', arr[3] / 10, true);
      if (currentPollCycleActive) {
        adapter.setState('control.HystHKs', arr[3] / 10, true);
      }
      adapter.setState('temperaturen.einstellungen.TRErhMax', arr[4] / 10, true);
      adapter.setState('temperaturen.einstellungen.Freig2VD', arr[5] / 10, true);
      adapter.setState('temperaturen.einstellungen.FreigZWE', arr[6] / 10, true);
      adapter.setState('temperaturen.einstellungen.T-Luftabt', arr[7] / 10, true);
      adapter.setState('temperaturen.einstellungen.TDIsoll', arr[8] / 10, true);
      adapter.setState('temperaturen.einstellungen.HystBW', arr[9] / 10, true);
      if (currentPollCycleActive) {
        adapter.setState('control.HystBWs', arr[9] / 10, true);
      }
      adapter.setState('temperaturen.einstellungen.VL2VDBW', arr[10] / 10, true);
      adapter.setState('temperaturen.einstellungen.TAussenMax', arr[11] / 10, true);
      adapter.setState('temperaturen.einstellungen.TAussenMin', arr[12] / 10, true);
      adapter.setState('temperaturen.einstellungen.TWQMin', arr[13] / 10, true);
      adapter.setState('temperaturen.einstellungen.THGMax', arr[14] / 10, true);
      adapter.setState('temperaturen.einstellungen.TLAbtEnde', arr[15] / 10, true);
      adapter.setState('temperaturen.einstellungen.AbsenkBis', arr[16] / 10, true);
      adapter.setState('temperaturen.einstellungen.VLmax', arr[17] / 10, true);
      return true;
    },
    done
  );
}

function callluxtronik3405(done) {
  executeRead(
    '3405',
    (buf) => isSingleLineComplete('3405', buf),
    (raw) => {
      const arr = raw.replace(/(\r\n|\n|\r)/gm, '').split(';');
      if (arr.length !== parseInt(arr[1], 10) + 2) return false;
      const val = parseInt(arr[2], 10);
      adapter.setState('status.ModusHeizung', val, true);
      if (currentPollCycleActive) {
        adapter.setState('control.ModusHeizung', val, true);
      }
      return true;
    },
    done
  );
}

function callluxtronik3505(done) {
  executeRead(
    '3505',
    (buf) => isSingleLineComplete('3505', buf),
    (raw) => {
      const arr = raw.replace(/(\r\n|\n|\r)/gm, '').split(';');
      if (arr.length !== parseInt(arr[1], 10) + 2) return false;
      const val = parseInt(arr[2], 10);
      adapter.setState('status.ModusWW', val, true);
      if (currentPollCycleActive) {
        adapter.setState('control.ModusWW', val, true);
      }
      return true;
    },
    done
  );
}

function callluxtronik3400(done) {
  executeRead(
    '3400',
    (buf) => isSingleLineComplete('3400', buf),
    (raw) => {
      const arr = raw.replace(/(\r\n|\n|\r)/gm, '').split(';');
      if (arr.length !== 11) return false;

      lastData3400 = arr.slice();
      adapter.setState('heizkurve.AbwRLs', arr[2] / 10, true);
      adapter.setState('heizkurve.Endpunkt', arr[3] / 10, true);
      adapter.setState('heizkurve.ParaV', arr[4] / 10, true);
      adapter.setState('heizkurve.NachtAbs', arr[5] / 10, true);
      if (currentPollCycleActive) {
        adapter.setState('control.AbwRLs', arr[2] / 10, true);
        adapter.setState('control.EndpunktHK', arr[3] / 10, true);
        adapter.setState('control.ParaVHK', arr[4] / 10, true);
        adapter.setState('control.NachtAbs', arr[5] / 10, true);
      }
      return true;
    },
    done
  );
}

function callluxtronik3200(done) {
  executeRead(
    '3200',
    (buf) => isSingleLineComplete('3200', buf),
    (raw) => {
      const arr = raw.replace(/(\r\n|\n|\r)/gm, '').split(';');
      if (arr.length !== parseInt(arr[1], 10) + 2) return false;

      const s1 = arr[2].padStart(2, '0') + ':' + arr[3].padStart(2, '0');
      const e1 = arr[4].padStart(2, '0') + ':' + arr[5].padStart(2, '0');
      const s2 = arr[6].padStart(2, '0') + ':' + arr[7].padStart(2, '0');
      const e2 = arr[8].padStart(2, '0') + ':' + arr[9].padStart(2, '0');

      if (!schaltzbwblock) {
        adapter.setState('control.SchaltzWoBW.Start1', s1, true);
        adapter.setState('control.SchaltzWoBW.Ende1', e1, true);
        adapter.setState('control.SchaltzWoBW.Start2', s2, true);
        adapter.setState('control.SchaltzWoBW.Ende2', e2, true);
      }
      adapter.setState('status.SchaltzWoBW.Start1', s1, true);
      adapter.setState('status.SchaltzWoBW.Ende1', e1, true);
      adapter.setState('status.SchaltzWoBW.Start2', s2, true);
      adapter.setState('status.SchaltzWoBW.Ende2', e2, true);
      return true;
    },
    done
  );
}

// --- Transaktions-Helfer für Read-Modify-Write (Heizkurve & Hysterese) ---

function updateHeizkurveField(index, value) {
  enqueueCommand('read_3400_for_write', (cb) => {
    lastData3400 = null;
    callluxtronik3400(cb);
  });
  enqueueCommand('write_3401', (cb) => {
    if (!lastData3400 || lastData3400.length < 11) {
      adapter.log.warn('Heizkurve nicht geändert: aktuelle Werte (3400) konnten nicht gelesen werden');
      return cb();
    }
    const dataHK = lastData3400.map((v) => String(v).trim());
    dataHK[0] = '3401';
    dataHK[index] = String(value);
    executeWrite('3401', dataHK.join(';'), null, cb);
  });
  enqueueCommand('verify_3400', callluxtronik3400);
}

function updateHystereseField(index, value) {
  enqueueCommand('read_2100_for_write', (cb) => {
    lastData2100 = null;
    callluxtronik2100(cb);
  });
  enqueueCommand('write_2101', (cb) => {
    if (!lastData2100 || lastData2100.length < 10) {
      adapter.log.warn('Hysterese nicht geändert: aktuelle Werte (2100) konnten nicht gelesen werden');
      return cb();
    }
    const dataHyst = lastData2100.map((v) => String(v).trim());
    dataHyst[0] = '2101';
    dataHyst[index] = String(value);
    executeWrite('2101', dataHyst.join(';'), null, cb);
  });
  enqueueCommand('verify_2100', callluxtronik2100);
}

// --- Steuerfunktionen ---

function controlbws(statebws) {
  enqueueCommand('write_3501', (cb) => {
    executeWrite(
      '3501',
      '3501;1;' + statebws,
      (raw) => {
        const lines = raw.split(/\r?\n/);
        const confirmedVal = lines[3] ? lines[3].split(';')[2] : null;
        if (confirmedVal !== undefined && Number(confirmedVal) === Number(lastsetbws)) {
          bwretry = 0;
          adapter.setState('temperaturen.BWs', Number(confirmedVal) / 10, true);
          enqueueCommand('verify_1100', callluxtronik1100);
        } else {
          bwretry++;
          if (bwretry <= 3) {
            adapter.log.debug('BW-Solltemperatur setzen nicht erfolgreich, Wiederholung (' + bwretry + '/3)');
            controlbws(lastsetbws);
          } else {
            adapter.log.warn('Warmwasser-Soll konnte nicht gesetzt werden, Vorgang abgebrochen');
          }
        }
      },
      cb
    );
  });
}

function controlmodusww(statemodusww) {
  enqueueCommand('write_3506', (cb) => {
    executeWrite(
      '3506',
      '3506;1;' + statemodusww,
      () => adapter.setState('status.ModusWW', statemodusww, true),
      cb
    );
  });
}

function controlmodusheizung(statemodusheizung) {
  enqueueCommand('write_3406', (cb) => {
    executeWrite(
      '3406',
      '3406;1;' + statemodusheizung,
      () => adapter.setState('status.ModusHeizung', statemodusheizung, true),
      cb
    );
  });
}

function controlSchaltzWoBW(arr) {
  schaltzbwblock = false;
  enqueueCommand('write_3201', (cb) => {
    executeWrite('3201', arr.join(';'), () => {
      enqueueCommand('verify_3200', callluxtronik3200);
    }, cb);
  });
}

function controlZeit(arr) {
  zeitblock = false;
  enqueueCommand('write_2701', (cb) => {
    executeWrite('2701', arr.join(';'), null, cb);
  });
}

// --- Polling & State-Handling ---

function pollluxtronik() {
  if (pollQueue.length > 0 || isProcessingQueue) {
    adapter.log.debug('Vorheriger Poll oder Befehl noch aktiv, überspringe diesen Zyklus');
    return;
  }
  currentPollCycleActive = true;
  enqueuePoll('1800', callluxtronik1800);
  enqueuePoll('2100', callluxtronik2100);
  enqueuePoll('3405', callluxtronik3405);
  enqueuePoll('3505', callluxtronik3505);
  enqueuePoll('3400', callluxtronik3400);
  enqueuePoll('3200', callluxtronik3200);
}

function toTenths(value, id) {
  const n = Number(value);
  if (value === null || value === '' || typeof value === 'boolean' || !Number.isFinite(n)) {
    adapter.log.warn('Ungültiger Wert für ' + id + ': ' + value + ' - Befehl wird ignoriert');
    return null;
  }
  return Math.round(n * 10);
}

function controlluxtronik(id, stateVal) {
  try {
    let tenths = null;
    if (NUMERIC_CONTROLS.includes(id)) {
      tenths = toTenths(stateVal, id);
      if (tenths === null) return;
    }

    switch (id) {
      case 'control.BWs':
        bwretry = 0;
        lastsetbws = tenths;
        controlbws(tenths);
        break;
      case 'control.ModusWW':
        controlmodusww(stateVal);
        break;
      case 'control.ModusHeizung':
        controlmodusheizung(stateVal);
        break;
      case 'control.AbwRLs':
        updateHeizkurveField(2, tenths);
        break;
      case 'control.EndpunktHK':
        updateHeizkurveField(3, tenths);
        break;
      case 'control.ParaVHK':
        updateHeizkurveField(4, tenths);
        break;
      case 'control.NachtAbs':
        updateHeizkurveField(5, tenths);
        break;
      case 'control.HystHKs':
        updateHystereseField(3, tenths);
        break;
      case 'control.HystBWs':
        updateHystereseField(9, tenths);
        break;
      case 'control.SchaltzWoBW.senden':
        readStatesAndSendSchaltzeiten();
        break;
      case 'control.Zeit.senden':
        readStatesAndSendZeit();
        break;
      default:
        if (id.includes('control.SchaltzWoBW.Start') || id.includes('control.SchaltzWoBW.Ende')) {
          schaltzbwblock = true;
          later(() => { schaltzbwblock = false; }, 60000);
        } else if (id.includes('control.Zeit.uhrzeit') || id.includes('control.Zeit.dow') || id.includes('control.Zeit.datum')) {
          zeitblock = true;
          later(() => { zeitblock = false; }, 60000);
        }
    }
  } catch (e) {
    adapter.log.warn('controlluxtronik-Fehler: ' + e);
  }
}

function readStatesAndSendSchaltzeiten() {
  const keys = [
    'control.SchaltzWoBW.Start1',
    'control.SchaltzWoBW.Ende1',
    'control.SchaltzWoBW.Start2',
    'control.SchaltzWoBW.Ende2'
  ];
  const results = [];

  function next(idx) {
    if (idx >= keys.length) {
      const payload = ['3201', '8'];
      for (const timeStr of results) {
        const str = String(timeStr || '00:00');
        payload.push(Number(str.slice(0, 2)), Number(str.slice(-2)));
      }
      controlSchaltzWoBW(payload);
      return;
    }
    adapter.getState(keys[idx], (err, state) => {
      if (err || !state || typeof state.val !== 'string') {
        adapter.log.warn('Fehler beim Auslesen von ' + keys[idx]);
        return;
      }
      results.push(state.val);
      next(idx + 1);
    });
  }
  next(0);
}

function readStatesAndSendZeit() {
  adapter.getState('control.Zeit.datum', (err1, sDatum) => {
    adapter.getState('control.Zeit.dow', (err2, sDow) => {
      adapter.getState('control.Zeit.uhrzeit', (err3, sZeit) => {
        if (!sDatum || !sDow || !sZeit || !sDatum.val || !sZeit.val) {
          adapter.log.warn('Fehler beim Auslesen der Zeitfelder');
          return;
        }
        const dStr = String(sDatum.val);
        const tStr = String(sZeit.val);
        const payload = [
          '2701',
          '7',
          Number(dStr.slice(-2)),
          Number(dStr.slice(3, 5)),
          Number(dStr.slice(0, 2)),
          sDow.val,
          Number(tStr.slice(0, 2)),
          Number(tStr.slice(3, 5)),
          Number(tStr.slice(-2))
        ];
        controlZeit(payload);
      });
    });
  });
}

// --- Formatierungs-Helfer ---

function setfehlertext(fehlerinfo) {
  try {
    if (!fehlerinfo) return '';
    const arr = fehlerinfo.split(';');
    const code = arr[3];
    const zeit =
      ('00' + arr[4]).slice(-2) + '.' +
      ('00' + arr[5]).slice(-2) + '.' +
      ('00' + arr[6]).slice(-2) + ', ' +
      ('00' + arr[7]).slice(-2) + ':' +
      ('00' + arr[8]).slice(-2);
    const text = ERROR_CODES[code] !== undefined ? ERROR_CODES[code] : 'Unbekannter Fehler (' + code + ')';
    return text + ' ' + zeit;
  } catch (e) {
    adapter.log.warn('setfehlertext - Fehler: ' + e);
    return '';
  }
}

function setabschalttext(abschaltinfo) {
  try {
    if (!abschaltinfo) return '';
    const arr = abschaltinfo.split(';');
    const code = arr[3];
    const zeit =
      ('00' + arr[4]).slice(-2) + '.' +
      ('00' + arr[5]).slice(-2) + '.' +
      ('00' + arr[6]).slice(-2) + ', ' +
      ('00' + arr[7]).slice(-2) + ':' +
      ('00' + arr[8]).slice(-2);
    const map = {
      '010': 'weniger Waerme, ',
      '001': 'Waermepumpenstoerung, ',
      '002': 'Anlagenstoerung, '
    };
    return (map[code] || '') + ' ' + zeit;
  } catch (e) {
    adapter.log.warn('setabschalttext - Fehler: ' + e);
    return '';
  }
}

function setstatustext(statuscode) {
  try {
    const code = statuscode.split(';')[5];
    const map = {
      '0': 'Heizung',
      '1': 'Warmwasser',
      '2': 'Schwimmbad',
      '3': 'EVU-Sperre',
      '4': 'Abtauen',
      '5': 'Bereitschaft'
    };
    return map[code] || 'Status unklar';
  } catch (e) {
    adapter.log.warn('setstatustext - Fehler: ' + e);
    return 'Status unklar';
  }
}

function toTimeString(totalseconds) {
  try {
    const total = Number(totalseconds) || 0;
    const hours = Math.floor(total / 3600);
    const minutes = Math.floor((total - hours * 3600) / 60);
    const seconds = Math.floor(total - hours * 3600 - minutes * 60);
    return (
      (hours < 10 ? '0' + hours : hours) + 'h ' +
      (minutes < 10 ? '0' + minutes : minutes) + 'min ' +
      (seconds < 10 ? '0' + seconds : seconds) + 's'
    );
  } catch (e) {
    adapter.log.warn('toTimeString - Fehler: ' + e);
    return '00h 00min 00s';
  }
}

// --- Adapter-Lifecycle ---

function startAdapter(options) {
  options = Object.assign({}, options, { name: 'luxtronik1' });
  adapter = new utils.Adapter(options);

  adapter.on('unload', (callback) => {
    try {
      shutdown();
      adapter.log.info('[END] Stopping luxtronik adapter...');
      adapter.setState('info.connection', false, true);
      callback();
    } catch (e) {
      callback();
    }
  });

  adapter.on('stateChange', (id, state) => {
    if (!id || !state || state.ack) return;
    const shortId = id.substring(adapter.namespace.length + 1);
    adapter.log.debug('Steuerbefehl empfangen: ' + shortId + ' = ' + state.val);
    controlluxtronik(shortId, state.val);
  });

  adapter.on('ready', () => {
    if (!adapter.config.host) {
      adapter.log.warn('[START] Keine IP-Adresse konfiguriert');
      return;
    }
    unloaded = false;
    adapter.setState('info.connection', false, true);
    main();
  });

  return adapter;
}

function main() {
  deviceIpAddress = adapter.config.host;
  port = parseInt(adapter.config.port, 10);
  if (!(port > 0 && port < 65536)) {
    adapter.log.error('[START] Ungültiger Port: ' + adapter.config.port);
    return;
  }

  const pollingTime = Math.max(parseInt(adapter.config.pollInterval, 10) || 300000, 10000);
  adapter.log.info('[START] Luxtronik-Adapter gestartet (' + deviceIpAddress + ':' + port + ', Intervall: ' + pollingTime + ' ms)');

  adapter.subscribeStates('control.*');
  pollluxtronik();

  polling = setInterval(() => {
    if (!unloaded) pollluxtronik();
  }, pollingTime);
}

if (module && module.parent) {
  module.exports = startAdapter;
} else {
  startAdapter();
}
