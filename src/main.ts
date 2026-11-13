import * as utils from '@iobroker/adapter-core';
import * as net from 'net';

export interface Luxtronik1Config extends ioBroker.AdapterConfig {
    host: string;
    port: number;
    pollInterval: number;
    socketTimeout: number;
}

type QueueTaskFn = (done: (success?: boolean) => void) => void | Promise<void>;

interface QueueItem {
    name: string;
    taskFn: QueueTaskFn;
}

const NUMERIC_CONTROLS: readonly string[] = [
    'control.BWs',
    'control.NachtAbs',
    'control.ParaVHK',
    'control.EndpunktHK',
    'control.AbwRLs',
    'control.HystBWs',
    'control.HystHKs',
];

const ERROR_CODES: Record<string, string> = {
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
    '745': 'Modem-Fehler',
};

export class Luxtronik1 extends utils.Adapter {
    declare public config: Luxtronik1Config;

    private deviceIpAddress = '';
    private port = 0;
    private pollingTimer: ioBroker.Interval | null | undefined = null;
    private unloaded = false;

    private sharedSocket: net.Socket | null = null;
    private isProcessingQueue = false;
    private schaltzbwblock = false;
    private zeitblock = false;

    private lastData3400: string[] | null = null;
    private lastData2100: string[] | null = null;
    private lastsetbws: number | null = null;
    private bwretry = 0;

    private readonly errorCounters: Record<string, number> = {};
    private readonly timers = new Set<ioBroker.Timeout>();

    private pollQueue: QueueItem[] = [];
    private commandQueue: QueueItem[] = [];

    public constructor(options: Partial<utils.AdapterOptions> = {}) {
        super({
            ...options,
            name: 'luxtronik1',
        });

        this.on('ready', this.onReady.bind(this));
        this.on('stateChange', this.onStateChange.bind(this));
        this.on('unload', this.onUnload.bind(this));
    }

    private async onReady(): Promise<void> {
        if (!this.config.host) {
            this.log.warn('[START] Keine IP-Adresse konfiguriert');
            return;
        }

        this.unloaded = false;
        await this.setStateAsync('info.connection', false, true);

        this.deviceIpAddress = String(this.config.host).trim();
        this.port = Number(this.config.port);

        if (!(this.port > 0 && this.port < 65536)) {
            this.log.error(`[START] Ungültiger Port: ${this.config.port}`);
            return;
        }

        const pollingTime = Math.max(Number(this.config.pollInterval) || 300000, 10000);
        this.log.info(
            `[START] Luxtronik-Adapter v${this.version} gestartet (${this.deviceIpAddress}:${this.port}, Intervall: ${pollingTime} ms)`
        );

        this.subscribeStates('control.*');
        this.pollLuxtronik();

        this.pollingTimer = this.setInterval(() => {
            if (!this.unloaded) {
                this.pollLuxtronik();
            }
        }, pollingTime);
    }

    private onUnload(callback: () => void): void {
        try {
            this.shutdown();
            this.log.info('[END] Stopping luxtronik adapter...');
            void this.setState('info.connection', false, true);
            callback();
        } catch {
            callback();
        }
    }

    private onStateChange(id: string, state: ioBroker.State | null | undefined): void {
        if (!id || !state || state.ack) {
            return;
        }
        const shortId = id.substring(this.namespace.length + 1);
        this.log.debug(`Steuerbefehl empfangen: ${shortId} = ${state.val}`);
        void this.controlLuxtronik(shortId, state.val);
    }

    private later(fn: () => void, ms: number): ioBroker.Timeout | undefined {
        const t = this.setTimeout(() => {
            if (t) {
                this.timers.delete(t);
            }
            if (this.unloaded) {
                return;
            }
            try {
                fn();
            } catch (e) {
                this.log.warn(`Timer-Fehler: ${String(e)}`);
            }
        }, ms);

        if (t) {
            this.timers.add(t);
        }
        return t;
    }

    private clearTrackedTimer(t: ioBroker.Timeout | null | undefined): void {
        if (t) {
            this.clearTimeout(t);
            this.timers.delete(t);
        }
    }

    private getOrConnectSocket(callback: (err: Error | null, socket: net.Socket | null) => void): void {
        if (this.unloaded) {
            callback(new Error('Adapter unloaded'), null);
            return;
        }

        if (this.sharedSocket && !this.sharedSocket.destroyed && this.sharedSocket.writable) {
            this.sharedSocket.removeAllListeners('data');
            this.sharedSocket.removeAllListeners('timeout');
            this.sharedSocket.removeAllListeners('error');
            this.sharedSocket.removeAllListeners('close');
            this.sharedSocket.on('error', () => {
                /* prevent unhandled error while idle */
            });
            callback(null, this.sharedSocket);
            return;
        }

        this.closeSharedSocket();

        const connectTimeout = Number(this.config.socketTimeout) || 15000;
        const socket = new net.Socket();
        socket.setNoDelay(true);
        socket.setKeepAlive(true, 5000);
        this.sharedSocket = socket;

        let settled = false;
        const connTimer = this.later(() => {
            if (!settled) {
                settled = true;
                socket.destroy();
                this.sharedSocket = null;
                callback(new Error(`Connect-Timeout (${connectTimeout} ms)`), null);
            }
        }, connectTimeout);

        socket.once('connect', () => {
            if (settled) {
                return;
            }
            settled = true;
            this.clearTrackedTimer(connTimer);
            this.log.debug('TCP-Verbindung zum RS232-Konverter hergestellt');
            this.later(() => {
                callback(null, socket);
            }, 150);
        });

        socket.once('error', (err: Error) => {
            if (settled) {
                return;
            }
            settled = true;
            this.clearTrackedTimer(connTimer);
            socket.destroy();
            if (this.sharedSocket === socket) {
                this.sharedSocket = null;
            }
            callback(err, null);
        });

        socket.connect(this.port, this.deviceIpAddress);
    }

    private closeSharedSocket(): void {
        if (!this.sharedSocket) {
            return;
        }
        const s = this.sharedSocket;
        this.sharedSocket = null;
        try {
            s.removeAllListeners();
            s.on('error', () => {
                /* ignore */
            });
            s.end();
            this.later(() => {
                if (!s.destroyed) {
                    s.destroy();
                }
            }, 150);
        } catch {
            /* ignore */
        }
    }

    private shutdown(): void {
        this.unloaded = true;
        if (this.pollingTimer) {
            this.clearInterval(this.pollingTimer);
            this.pollingTimer = undefined;
        }
        this.timers.forEach((t) => this.clearTimeout(t));
        this.timers.clear();
        this.pollQueue = [];
        this.commandQueue = [];
        this.isProcessingQueue = false;
        this.closeSharedSocket();
    }

    private resetConnectionState(reason: string): void {
        this.log.warn(`Verbindung zur Luxtronik wird zurückgesetzt (${reason})`);
        this.pollQueue = [];
        for (const key of Object.keys(this.errorCounters)) {
            this.errorCounters[key] = 0;
        }
        this.closeSharedSocket();
        void this.setState('info.connection', false, true);
    }

    private enqueueCommand(name: string, taskFn: QueueTaskFn): void {
        this.commandQueue.push({ name, taskFn });
        this.processQueues();
    }

    private enqueuePoll(name: string, taskFn: QueueTaskFn): void {
        this.pollQueue.push({ name, taskFn });
        this.processQueues();
    }

    private processQueues(): void {
        if (this.unloaded || this.isProcessingQueue) {
            return;
        }

        const nextItem = this.commandQueue.shift() ?? this.pollQueue.shift();
        if (!nextItem) {
            this.closeSharedSocket();
            return;
        }

        this.isProcessingQueue = true;
        // Nach einem Schreibbefehl 2500 ms Pause für das EEPROM, zwischen Lese-Befehlen 250 ms
        const delayAfterMs = nextItem.name.startsWith('write_') ? 2500 : 250;

        try {
            void nextItem.taskFn(() => {
                this.isProcessingQueue = false;
                this.later(() => this.processQueues(), delayAfterMs);
            });
        } catch (e) {
            this.log.warn(`Fehler bei Ausführung von ${nextItem.name}: ${String(e)}`);
            this.isProcessingQueue = false;
            this.closeSharedSocket();
            this.later(() => this.processQueues(), 500);
        }
    }

    private extractDataParts(expectedCmd: string, buffer: string): string[] | null {
        const lines = buffer.split(/\r?\n/);
        for (const rawLine of lines) {
            const line = rawLine.trim();
            const idx = line.indexOf(`${expectedCmd};`);
            if (idx >= 0) {
                const cleanLine = line.substring(idx);
                const parts = cleanLine.split(';');
                const expectedCount = parseInt(parts[1], 10);
                if (
                    !Number.isNaN(expectedCount) &&
                    parts.length === expectedCount + 2 &&
                    parts[parts.length - 1].length > 0
                ) {
                    return parts;
                }
            }
        }
        return null;
    }

    private isSingleLineComplete(expectedCmd: string, buffer: string): boolean {
        return this.extractDataParts(expectedCmd, buffer) !== null;
    }

    private executeRead(
        cmd: string,
        isCompleteFn: (buf: string) => boolean,
        parseFn: (raw: string) => Promise<boolean> | boolean,
        doneCallback: (success?: boolean) => void
    ): void {
        if (this.unloaded) {
            doneCallback(false);
            return;
        }

        this.getOrConnectSocket((connErr, socket) => {
            if (connErr || !socket) {
                this.log.warn(`${cmd}: Verbindungsfehler: ${connErr?.message ?? 'Kein Socket'}`);
                void this.setState('info.connection', false, true);
                this.pollQueue = [];
                doneCallback(false);
                return;
            }

            let rxBuffer = '';
            let finished = false;
            let retransmitDone = false;
            let wakeRetryTimer: ioBroker.Timeout | undefined;

            const idleTimeout = Number(this.config.socketTimeout) || 15000;

            const finishStep = (ok: boolean, destroySocket: boolean): void => {
                if (finished) {
                    return;
                }
                finished = true;
                this.clearTrackedTimer(timeoutTimer);
                this.clearTrackedTimer(wakeRetryTimer);

                socket.removeAllListeners('data');
                socket.removeAllListeners('error');
                socket.removeAllListeners('close');
                socket.on('error', () => {
                    /* ignore while idle */
                });

                if (destroySocket) {
                    this.closeSharedSocket();
                }

                if (!ok) {
                    doneCallback(false);
                    return;
                }

                void (async () => {
                    let success = false;
                    try {
                        success = await parseFn(rxBuffer.trim());
                    } catch (e) {
                        this.log.warn(`${cmd} Parse-Fehler: ${String(e)}`);
                        success = false;
                    }

                    if (success) {
                        this.errorCounters[cmd] = 0;
                    } else {
                        const preview = rxBuffer.replace(/\r/g, '\\r').replace(/\n/g, '\\n');
                        this.errorCounters[cmd] = (this.errorCounters[cmd] ?? 0) + 1;
                        this.log.warn(
                            `Datensatz ${cmd} unvollständig (Fehler #${this.errorCounters[cmd]}, Rohdaten: "${preview}")`
                        );
                        if (this.errorCounters[cmd] > 4) {
                            this.resetConnectionState(`Mehrfach unvollständiger Datensatz ${cmd}`);
                        }
                    }
                    doneCallback(success);
                })();
            };

            const timeoutTimer = this.later(() => {
                const preview = rxBuffer.replace(/\r/g, '\\r').replace(/\n/g, '\\n');
                this.log.warn(
                    `${cmd}: Timeout nach ${idleTimeout} ms (${rxBuffer.length} Bytes, Rohdaten: "${preview}")`
                );
                void this.setState('info.connection', false, true);
                finishStep(false, true);
            }, idleTimeout);

            const scheduleWakeRetransmit = (): void => {
                this.clearTrackedTimer(wakeRetryTimer);
                if (retransmitDone || finished) {
                    return;
                }
                wakeRetryTimer = this.later(() => {
                    if (!finished && !socket.destroyed && socket.writable && !isCompleteFn(rxBuffer)) {
                        retransmitDone = true;
                        const preview = rxBuffer.replace(/\r/g, '\\r').replace(/\n/g, '\\n');
                        this.log.debug(
                            `${cmd}: Nach 2000 ms erst ${rxBuffer.length} Bytes ("${preview}"), sende 999 & Retransmit...`
                        );
                        rxBuffer = '';
                        socket.write('999\r\n');
                        this.later(() => {
                            if (!finished && !socket.destroyed && socket.writable) {
                                rxBuffer = '';
                                socket.write(`${cmd}\r\n`);
                            }
                        }, 350);
                    }
                }, 2000);
            };

            socket.on('data', (chunk: Buffer) => {
                rxBuffer += chunk.toString('utf8');
                try {
                    if (isCompleteFn(rxBuffer)) {
                        this.log.debug(`${cmd}: OK (${rxBuffer.length} Bytes)`);
                        finishStep(true, false);
                        return;
                    }
                    scheduleWakeRetransmit();
                } catch (e) {
                    this.log.warn(`${cmd}: Fehler bei Prüfung: ${String(e)}`);
                }
            });

            socket.once('error', (err: Error) => {
                this.log.warn(`${cmd} Socket-Fehler: ${err.message}`);
                finishStep(false, true);
            });

            socket.once('close', () => {
                if (!finished) {
                    this.sharedSocket = null;
                    finishStep(isCompleteFn(rxBuffer), true);
                }
            });

            socket.write(`${cmd}\r\n`);
            scheduleWakeRetransmit();
        });
    }

    private executeWrite(
        cmd: string,
        payloadLine: string,
        onCloseParseFn: ((raw: string) => Promise<void> | void) | null,
        doneCallback: (success?: boolean) => void
    ): void {
        if (this.unloaded) {
            doneCallback(false);
            return;
        }

        // Vor einem Schreibbefehl immer eine frische Verbindung öffnen
        this.closeSharedSocket();

        this.getOrConnectSocket((connErr, socket) => {
            if (connErr || !socket) {
                this.log.warn(`${cmd} (Write): Verbindungsfehler: ${connErr?.message ?? 'Kein Socket'}`);
                doneCallback(false);
                return;
            }

            let rxBuffer = '';
            let finished = false;
            let recoveryTriggered = false;
            let payloadSent = false;
            let saveSent = false;
            let stepTimer1: ioBroker.Timeout | undefined;
            let stepTimer2: ioBroker.Timeout | undefined;
            let closeAfterWriteTimer: ioBroker.Timeout | undefined;

            const idleTimeout = Number(this.config.socketTimeout) || 15000;

            const safeWrite = (str: string): void => {
                if (!finished && !socket.destroyed && socket.writable) {
                    socket.write(str);
                }
            };

            const finishWrite = (): void => {
                if (finished) {
                    return;
                }
                finished = true;
                this.clearTrackedTimer(timeoutTimer);
                this.clearTrackedTimer(stepTimer1);
                this.clearTrackedTimer(stepTimer2);
                this.clearTrackedTimer(closeAfterWriteTimer);

                const preview = rxBuffer.replace(/\r/g, '\\r').replace(/\n/g, '\\n');
                this.log.debug(`${cmd} (Write) Antwort der Luxtronik: "${preview}"`);

                // Nach dem Schreiben Socket schließen, damit das EEPROM sauber speichert
                this.closeSharedSocket();

                void (async () => {
                    try {
                        if (rxBuffer !== '' && !recoveryTriggered && onCloseParseFn) {
                            await onCloseParseFn(rxBuffer.trim());
                        } else if (recoveryTriggered) {
                            this.log.warn(
                                `${cmd}: Editiermodus nach 779 zurückgesetzt, wiederhole Schreibbefehl...`
                            );
                            this.later(() => {
                                this.enqueueCommand(`write_${cmd}_retry`, (cb) =>
                                    this.executeWrite(cmd, payloadLine, onCloseParseFn, cb)
                                );
                            }, 1500);
                        }
                    } catch (e) {
                        this.log.warn(`${cmd} Write-Parse-Fehler: ${String(e)}`);
                    }
                    doneCallback(!recoveryTriggered);
                })();
            };

            const sendPayloadStep = (): void => {
                if (payloadSent || recoveryTriggered || finished) {
                    return;
                }
                payloadSent = true;
                this.clearTrackedTimer(stepTimer1);
                safeWrite(`${payloadLine}\r\n`);

                // Fallback-Timer für 999 (2000 ms nach Payload)
                stepTimer2 = this.later(() => {
                    sendSaveStep();
                }, 2000);
            };

            const sendSaveStep = (): void => {
                if (saveSent || recoveryTriggered || finished) {
                    return;
                }
                saveSent = true;
                this.clearTrackedTimer(stepTimer2);
                safeWrite('999\r\n');
                closeAfterWriteTimer = this.later(() => {
                    finishWrite();
                }, 1200);
            };

            const timeoutTimer = this.later(() => {
                const preview = rxBuffer.replace(/\r/g, '\\r').replace(/\n/g, '\\n');
                this.log.warn(`${cmd} (Write): Timeout (${rxBuffer.length} Bytes, Rohdaten: "${preview}")`);
                finishWrite();
            }, idleTimeout);

            socket.on('data', (chunk: Buffer) => {
                rxBuffer += chunk.toString('utf8');
                try {
                    if (rxBuffer.includes('779') && !recoveryTriggered) {
                        recoveryTriggered = true;
                        this.clearTrackedTimer(stepTimer1);
                        this.clearTrackedTimer(stepTimer2);
                        this.clearTrackedTimer(closeAfterWriteTimer);
                        this.log.warn(`${cmd}: Fehler 779 erkannt, führe Reset-Sequenz aus...`);

                        this.later(() => safeWrite(`${cmd}\r\n`), 300);
                        this.later(() => safeWrite(`${cmd};0\r\n`), 1000);
                        this.later(() => safeWrite('999\r\n'), 1800);
                        this.later(() => finishWrite(), 2500);
                        return;
                    }

                    // 1. Sobald die Luxtronik den Programmierbefehl mit "<cmd>;0" bestätigt -> Payload senden
                    if (!payloadSent && rxBuffer.includes(`${cmd};0`)) {
                        this.later(() => sendPayloadStep(), 250);
                        return;
                    }

                    // 2. Sobald die Luxtronik das Echo der Payload-Zeile zurückgeschickt hat -> 999 senden
                    if (payloadSent && !saveSent && rxBuffer.includes(payloadLine.substring(0, 10))) {
                        this.later(() => sendSaveStep(), 350);
                        return;
                    }

                    // 3. Sobald "993" und "999" bestätigt wurden -> fertig
                    if (
                        saveSent &&
                        (rxBuffer.includes('993\r\n999') ||
                            rxBuffer.includes('993\n999') ||
                            rxBuffer.endsWith('999\r\n') ||
                            rxBuffer.endsWith('999\n'))
                    ) {
                        finishWrite();
                    }
                } catch (e) {
                    this.log.debug(`${cmd}: Fehler bei Störungsbehebung: ${String(e)}`);
                }
            });

            socket.once('error', (err: Error) => {
                this.log.warn(`${cmd} (Write) Socket-Fehler: ${err.message}`);
                finishWrite();
            });

            socket.once('close', () => {
                this.sharedSocket = null;
                finishWrite();
            });

            this.log.info(`${cmd}: Sende Schreibbefehl: ${payloadLine}`);
            safeWrite(`${cmd}\r\n`);

            // Fallback nach 2000 ms
            stepTimer1 = this.later(() => {
                sendPayloadStep();
            }, 2000);
        });
    }

    private callLuxtronik1100(done: (success?: boolean) => void): void {
        this.executeRead(
            '1100',
            (buf) => this.isSingleLineComplete('1100', buf),
            async (raw) => {
                const arr = this.extractDataParts('1100', raw);
                if (!arr) {
                    return false;
                }

                if (this.lastsetbws !== null && Number(arr[8]) !== Number(this.lastsetbws)) {
                    this.log.info(
                        `BW-Solltemperatur nicht korrekt gesetzt (${arr[8]} statt ${this.lastsetbws}), Vorgang wird wiederholt`
                    );
                    if ((this.errorCounters['1100_bws'] ?? 0) < 5) {
                        this.errorCounters['1100_bws'] = (this.errorCounters['1100_bws'] ?? 0) + 1;
                        this.controlBws(this.lastsetbws);
                    } else {
                        this.errorCounters['1100_bws'] = 0;
                        this.resetConnectionState('Setzen der BW-Solltemperatur mehrmals fehlgeschlagen');
                    }
                    return true;
                }

                this.errorCounters['1100_bws'] = 0;
                await this.setStateAsync('temperaturen.VL', Number(arr[2]) / 10, true);
                await this.setStateAsync('temperaturen.RL', Number(arr[3]) / 10, true);
                await this.setStateAsync('temperaturen.RLs', Number(arr[4]) / 10, true);
                await this.setStateAsync('temperaturen.HG', Number(arr[5]) / 10, true);
                await this.setStateAsync('temperaturen.AUT', Number(arr[6]) / 10, true);
                await this.setStateAsync('temperaturen.BWi', Number(arr[7]) / 10, true);
                await this.setStateAsync('temperaturen.BWs', Number(arr[8]) / 10, true);
                await this.setStateAsync('control.BWs', Number(arr[8]) / 10, true);
                await this.setStateAsync('temperaturen.WQe', Number(arr[9]) / 10, true);
                await this.setStateAsync('temperaturen.WQa', Number(arr[10]) / 10, true);
                await this.setStateAsync('temperaturen.MK1VLi', Number(arr[11]) / 10, true);
                await this.setStateAsync('temperaturen.MK1VLs', Number(arr[12]) / 10, true);
                await this.setStateAsync('temperaturen.RS', Number(arr[13]) / 10, true);
                return true;
            },
            done
        );
    }

    private callLuxtronik1800(done: (success?: boolean) => void): void {
        this.executeRead(
            '1800',
            (buf) => buf.includes('1800;8', 10) && (buf.endsWith('\n') || buf.endsWith('\r')),
            async (raw) => {
                if (raw.length <= 500) {
                    return false;
                }
                const lines = raw.split(/\r?\n/);
                if (lines.length <= 21) {
                    return false;
                }

                const temperaturen = lines[2].split(';'); // 1100
                await this.setStateAsync('temperaturen.VL', Number(temperaturen[2]) / 10, true);
                await this.setStateAsync('temperaturen.RL', Number(temperaturen[3]) / 10, true);
                await this.setStateAsync('temperaturen.RLs', Number(temperaturen[4]) / 10, true);
                await this.setStateAsync('temperaturen.HG', Number(temperaturen[5]) / 10, true);
                await this.setStateAsync('temperaturen.AUT', Number(temperaturen[6]) / 10, true);
                await this.setStateAsync('temperaturen.BWi', Number(temperaturen[7]) / 10, true);
                await this.setStateAsync('temperaturen.BWs', Number(temperaturen[8]) / 10, true);
                await this.setStateAsync('control.BWs', Number(temperaturen[8]) / 10, true);
                await this.setStateAsync('temperaturen.WQe', Number(temperaturen[9]) / 10, true);
                await this.setStateAsync('temperaturen.WQa', Number(temperaturen[10]) / 10, true);
                await this.setStateAsync('temperaturen.MK1VLi', Number(temperaturen[11]) / 10, true);
                await this.setStateAsync('temperaturen.MK1VLs', Number(temperaturen[12]) / 10, true);
                await this.setStateAsync('temperaturen.RS', Number(temperaturen[13]) / 10, true);

                const eingaenge = lines[3].split(';'); // 1200
                await this.setStateAsync('eingaenge.ASD', Boolean(Number(eingaenge[2])), true);
                await this.setStateAsync('eingaenge.EVU', Boolean(Number(eingaenge[3])), true);
                await this.setStateAsync('eingaenge.HD', Boolean(Number(eingaenge[4])), true);
                await this.setStateAsync('eingaenge.MOT', Boolean(Number(eingaenge[5])), true);
                await this.setStateAsync('eingaenge.ND', Boolean(Number(eingaenge[6])), true);
                await this.setStateAsync('eingaenge.PEX', Boolean(Number(eingaenge[7])), true);

                const ausgaenge = lines[4].split(';'); // 1300
                await this.setStateAsync('ausgaenge.Abtauventil', Boolean(Number(ausgaenge[2])), true);
                await this.setStateAsync(
                    'ausgaenge.Brauchwarmwasserumwaelzpumpe',
                    Boolean(Number(ausgaenge[3])),
                    true
                );
                await this.setStateAsync(
                    'ausgaenge.Fussbodenheizungsumwaelzpumpe',
                    Boolean(Number(ausgaenge[4])),
                    true
                );
                await this.setStateAsync('ausgaenge.Heizungsumwaelzpumpe', Boolean(Number(ausgaenge[5])), true);
                await this.setStateAsync('ausgaenge.Mischer_1_Auf', Boolean(Number(ausgaenge[6])), true);
                await this.setStateAsync('ausgaenge.Mischer_1_Zu', Boolean(Number(ausgaenge[7])), true);
                await this.setStateAsync(
                    'ausgaenge.Ventilation_des_Waermepumpengehaeuses',
                    Boolean(Number(ausgaenge[8])),
                    true
                );
                await this.setStateAsync(
                    'ausgaenge.Ventilator_Brunnen_oder_Soleumwaelzpumpe',
                    Boolean(Number(ausgaenge[9])),
                    true
                );
                await this.setStateAsync(
                    'ausgaenge.Verdichter_1_in_Waermepumpe',
                    Boolean(Number(ausgaenge[10])),
                    true
                );
                await this.setStateAsync(
                    'ausgaenge.Verdichter_2_in_Waermepumpe',
                    Boolean(Number(ausgaenge[11])),
                    true
                );
                await this.setStateAsync(
                    'ausgaenge.Zusatzumwaelzpumpe_Zirkulationspumpe',
                    Boolean(Number(ausgaenge[12])),
                    true
                );
                await this.setStateAsync('ausgaenge.Zweiter_Waermeerzeuger_1', Boolean(Number(ausgaenge[13])), true);
                await this.setStateAsync(
                    'ausgaenge.Zweiter_Waermeerzeuger_2_Sammelstoerung',
                    Boolean(Number(ausgaenge[14])),
                    true
                );

                const ablaufzeiten = lines[5].split(';'); // 1400
                const currentWpSeit =
                    parseInt(ablaufzeiten[2], 10) * 3600 +
                    parseInt(ablaufzeiten[3], 10) * 60 +
                    parseInt(ablaufzeiten[4], 10);

                const prevWpSeitState = await this.getStateAsync('ablaufzeiten.WPseit');
                if (prevWpSeitState && currentWpSeit === 0 && Number(prevWpSeitState.val) > 0) {
                    await this.setStateAsync('ablaufzeiten.WPseitlast', Number(prevWpSeitState.val), true);
                }
                await this.setStateAsync('ablaufzeiten.WPseit', currentWpSeit, true);

                await this.setStateAsync(
                    'ablaufzeiten.ZWE1seit',
                    parseInt(ablaufzeiten[5], 10) * 3600 +
                        parseInt(ablaufzeiten[6], 10) * 60 +
                        parseInt(ablaufzeiten[7], 10),
                    true
                );
                await this.setStateAsync(
                    'ablaufzeiten.ZWE2seit',
                    parseInt(ablaufzeiten[8], 10) * 3600 +
                        parseInt(ablaufzeiten[9], 10) * 60 +
                        parseInt(ablaufzeiten[10], 10),
                    true
                );
                await this.setStateAsync('ablaufzeiten.Netzeinv', parseInt(ablaufzeiten[11], 10), true);
                await this.setStateAsync(
                    'ablaufzeiten.SSPstand',
                    parseInt(ablaufzeiten[12], 10) * 60 + parseInt(ablaufzeiten[13], 10),
                    true
                );
                await this.setStateAsync(
                    'ablaufzeiten.SSPverz',
                    parseInt(ablaufzeiten[14], 10) * 60 + parseInt(ablaufzeiten[15], 10),
                    true
                );
                await this.setStateAsync(
                    'ablaufzeiten.VDstand',
                    parseInt(ablaufzeiten[16], 10) * 3600 +
                        parseInt(ablaufzeiten[17], 10) * 60 +
                        parseInt(ablaufzeiten[18], 10),
                    true
                );
                await this.setStateAsync(
                    'ablaufzeiten.HRM',
                    parseInt(ablaufzeiten[19], 10) * 3600 +
                        parseInt(ablaufzeiten[20], 10) * 60 +
                        parseInt(ablaufzeiten[21], 10),
                    true
                );
                await this.setStateAsync(
                    'ablaufzeiten.HRW',
                    parseInt(ablaufzeiten[22], 10) * 3600 +
                        parseInt(ablaufzeiten[23], 10) * 60 +
                        parseInt(ablaufzeiten[24], 10),
                    true
                );
                await this.setStateAsync(
                    'ablaufzeiten.TDIseit',
                    parseInt(ablaufzeiten[25], 10) * 3600 +
                        parseInt(ablaufzeiten[26], 10) * 60 +
                        parseInt(ablaufzeiten[27], 10),
                    true
                );
                await this.setStateAsync(
                    'ablaufzeiten.BWsperre',
                    parseInt(ablaufzeiten[28], 10) * 3600 +
                        parseInt(ablaufzeiten[29], 10) * 60 +
                        parseInt(ablaufzeiten[30], 10),
                    true
                );

                const betriebsstunden = lines[6].split(';');
                await this.setStateAsync('betriebsstunden.VD1', this.toTimeString(betriebsstunden[2]), true);
                await this.setStateAsync('betriebsstunden.Imp1', String(betriebsstunden[3]), true);
                await this.setStateAsync('betriebsstunden.AvVD1', this.toTimeString(betriebsstunden[4]), true);
                await this.setStateAsync('betriebsstunden.VD2', this.toTimeString(betriebsstunden[5]), true);
                await this.setStateAsync('betriebsstunden.Imp2', String(betriebsstunden[6]), true);
                await this.setStateAsync('betriebsstunden.AvVD2', this.toTimeString(betriebsstunden[7]), true);
                await this.setStateAsync('betriebsstunden.ZWE1', this.toTimeString(betriebsstunden[8]), true);
                await this.setStateAsync('betriebsstunden.ZWE2', this.toTimeString(betriebsstunden[9]), true);
                await this.setStateAsync('betriebsstunden.WP', this.toTimeString(betriebsstunden[10]), true);

                for (let i = 1; i < 6; i++) {
                    await this.setStateAsync(`fehler.${6 - i}`, this.setFehlerText(lines[7 + i]), true);
                    await this.setStateAsync(`abschaltungen.${6 - i}`, this.setAbschaltText(lines[14 + i]), true);
                }

                const statusParts = lines[21].split(';');
                await this.setStateAsync('status.WPtyp', String(statusParts[2]), true);
                await this.setStateAsync('status.SW', String(statusParts[3]), true);
                await this.setStateAsync('status.BivStufe', String(statusParts[4]), true);
                await this.setStateAsync('status.ANL', this.setStatusText(String(lines[21])), true);

                await this.setStateAsync('info.connection', true, true);
                return true;
            },
            done
        );
    }

    private callLuxtronik2100(done: (success?: boolean) => void): void {
        this.executeRead(
            '2100',
            (buf) => this.isSingleLineComplete('2100', buf),
            async (raw) => {
                const arr = this.extractDataParts('2100', raw);
                if (!arr) {
                    return false;
                }

                this.lastData2100 = arr.slice();
                await this.setStateAsync('temperaturen.einstellungen.RLBegr', Number(arr[2]) / 10, true);
                await this.setStateAsync('temperaturen.einstellungen.HystHR', Number(arr[3]) / 10, true);
                await this.setStateAsync('control.HystHKs', Number(arr[3]) / 10, true);
                await this.setStateAsync('temperaturen.einstellungen.TRErhMax', Number(arr[4]) / 10, true);
                await this.setStateAsync('temperaturen.einstellungen.Freig2VD', Number(arr[5]) / 10, true);
                await this.setStateAsync('temperaturen.einstellungen.FreigZWE', Number(arr[6]) / 10, true);
                await this.setStateAsync('temperaturen.einstellungen.T-Luftabt', Number(arr[7]) / 10, true);
                await this.setStateAsync('temperaturen.einstellungen.TDIsoll', Number(arr[8]) / 10, true);
                await this.setStateAsync('temperaturen.einstellungen.HystBW', Number(arr[9]) / 10, true);
                await this.setStateAsync('control.HystBWs', Number(arr[9]) / 10, true);
                await this.setStateAsync('temperaturen.einstellungen.VL2VDBW', Number(arr[10]) / 10, true);
                await this.setStateAsync('temperaturen.einstellungen.TAussenMax', Number(arr[11]) / 10, true);
                await this.setStateAsync('temperaturen.einstellungen.TAussenMin', Number(arr[12]) / 10, true);
                await this.setStateAsync('temperaturen.einstellungen.TWQMin', Number(arr[13]) / 10, true);
                await this.setStateAsync('temperaturen.einstellungen.THGMax', Number(arr[14]) / 10, true);
                await this.setStateAsync('temperaturen.einstellungen.TLAbtEnde', Number(arr[15]) / 10, true);
                await this.setStateAsync('temperaturen.einstellungen.AbsenkBis', Number(arr[16]) / 10, true);
                await this.setStateAsync('temperaturen.einstellungen.VLmax', Number(arr[17]) / 10, true);
                return true;
            },
            done
        );
    }

    private callLuxtronik3405(done: (success?: boolean) => void): void {
        this.executeRead(
            '3405',
            (buf) => this.isSingleLineComplete('3405', buf),
            async (raw) => {
                const arr = this.extractDataParts('3405', raw);
                if (!arr) {
                    return false;
                }
                const val = parseInt(arr[2], 10);
                await this.setStateAsync('status.ModusHeizung', val, true);
                await this.setStateAsync('control.ModusHeizung', val, true);
                return true;
            },
            done
        );
    }

    private callLuxtronik3505(done: (success?: boolean) => void): void {
        this.executeRead(
            '3505',
            (buf) => this.isSingleLineComplete('3505', buf),
            async (raw) => {
                const arr = this.extractDataParts('3505', raw);
                if (!arr) {
                    return false;
                }
                const val = parseInt(arr[2], 10);
                await this.setStateAsync('status.ModusWW', val, true);
                await this.setStateAsync('control.ModusWW', val, true);
                return true;
            },
            done
        );
    }

    private callLuxtronik3400(done: (success?: boolean) => void): void {
        this.executeRead(
            '3400',
            (buf) => this.isSingleLineComplete('3400', buf),
            async (raw) => {
                const arr = this.extractDataParts('3400', raw);
                if (!arr || arr.length !== 11) {
                    return false;
                }

                this.lastData3400 = arr.slice();
                this.log.debug(`3400 gelesen: AbwRLs=${Number(arr[2]) / 10} °C (Roh: ${arr.join(';')})`);

                await this.setStateAsync('heizkurve.AbwRLs', Number(arr[2]) / 10, true);
                await this.setStateAsync('heizkurve.Endpunkt', Number(arr[3]) / 10, true);
                await this.setStateAsync('heizkurve.ParaV', Number(arr[4]) / 10, true);
                await this.setStateAsync('heizkurve.NachtAbs', Number(arr[5]) / 10, true);

                await this.setStateAsync('control.AbwRLs', Number(arr[2]) / 10, true);
                await this.setStateAsync('control.EndpunktHK', Number(arr[3]) / 10, true);
                await this.setStateAsync('control.ParaVHK', Number(arr[4]) / 10, true);
                await this.setStateAsync('control.NachtAbs', Number(arr[5]) / 10, true);
                return true;
            },
            done
        );
    }

    private callLuxtronik3200(done: (success?: boolean) => void): void {
        this.executeRead(
            '3200',
            (buf) => this.isSingleLineComplete('3200', buf),
            async (raw) => {
                const arr = this.extractDataParts('3200', raw);
                if (!arr) {
                    return false;
                }

                const s1 = `${arr[2].padStart(2, '0')}:${arr[3].padStart(2, '0')}`;
                const e1 = `${arr[4].padStart(2, '0')}:${arr[5].padStart(2, '0')}`;
                const s2 = `${arr[6].padStart(2, '0')}:${arr[7].padStart(2, '0')}`;
                const e2 = `${arr[8].padStart(2, '0')}:${arr[9].padStart(2, '0')}`;

                if (!this.schaltzbwblock) {
                    await this.setStateAsync('control.SchaltzWoBW.Start1', s1, true);
                    await this.setStateAsync('control.SchaltzWoBW.Ende1', e1, true);
                    await this.setStateAsync('control.SchaltzWoBW.Start2', s2, true);
                    await this.setStateAsync('control.SchaltzWoBW.Ende2', e2, true);
                }
                await this.setStateAsync('status.SchaltzWoBW.Start1', s1, true);
                await this.setStateAsync('status.SchaltzWoBW.Ende1', e1, true);
                await this.setStateAsync('status.SchaltzWoBW.Start2', s2, true);
                await this.setStateAsync('status.SchaltzWoBW.Ende2', e2, true);
                return true;
            },
            done
        );
    }

    private updateHeizkurveField(index: number, value: number): void {
        if (!this.lastData3400 || this.lastData3400.length !== 11) {
            this.enqueueCommand('read_3400_for_write', (cb) => {
                this.callLuxtronik3400(cb);
            });
        }
        this.enqueueCommand('write_3401', (cb) => {
            if (!this.lastData3400 || this.lastData3400.length < 11) {
                this.log.warn('Heizkurve nicht geändert: aktuelle Werte (3400) konnten nicht gelesen werden');
                cb(false);
                return;
            }
            const dataHK = this.lastData3400.map((v) => String(v).trim());
            dataHK[0] = '3401';
            dataHK[index] = String(value);
            this.lastData3400[index] = String(value);
            this.executeWrite('3401', dataHK.join(';'), null, cb);
        });
        this.enqueueCommand('verify_3400', (cb) => this.callLuxtronik3400(cb));
    }

    private updateHystereseField(index: number, value: number): void {
        if (!this.lastData2100 || this.lastData2100.length < 10) {
            this.enqueueCommand('read_2100_for_write', (cb) => {
                this.callLuxtronik2100(cb);
            });
        }
        this.enqueueCommand('write_2101', (cb) => {
            if (!this.lastData2100 || this.lastData2100.length < 10) {
                this.log.warn('Hysterese nicht geändert: aktuelle Werte (2100) konnten nicht gelesen werden');
                cb(false);
                return;
            }
            const dataHyst = this.lastData2100.map((v) => String(v).trim());
            dataHyst[0] = '2101';
            dataHyst[index] = String(value);
            this.lastData2100[index] = String(value);
            this.executeWrite('2101', dataHyst.join(';'), null, cb);
        });
        this.enqueueCommand('verify_2100', (cb) => this.callLuxtronik2100(cb));
    }

    private controlBws(statebws: number): void {
        this.enqueueCommand('write_3501', (cb) => {
            this.executeWrite(
                '3501',
                `3501;1;${statebws}`,
                async (raw) => {
                    const arr = this.extractDataParts('3501', raw);
                    const confirmedVal = arr ? arr[2] : undefined;
                    if (confirmedVal !== undefined && Number(confirmedVal) === Number(this.lastsetbws)) {
                        this.bwretry = 0;
                        await this.setStateAsync('temperaturen.BWs', Number(confirmedVal) / 10, true);
                        await this.setStateAsync('control.BWs', Number(confirmedVal) / 10, true);
                        this.enqueueCommand('verify_1100', (verifyCb) => this.callLuxtronik1100(verifyCb));
                    } else {
                        this.bwretry++;
                        if (this.bwretry <= 3 && this.lastsetbws !== null) {
                            this.log.debug(
                                `BW-Solltemperatur setzen nicht erfolgreich, Wiederholung (${this.bwretry}/3)`
                            );
                            this.controlBws(this.lastsetbws);
                        } else {
                            this.log.warn('Warmwasser-Soll konnte nicht gesetzt werden, Vorgang abgebrochen');
                        }
                    }
                },
                cb
            );
        });
    }

    private controlModusWw(statemodusww: number): void {
        this.enqueueCommand('write_3506', (cb) => {
            this.executeWrite(
                '3506',
                `3506;1;${statemodusww}`,
                async () => {
                    await this.setStateAsync('status.ModusWW', statemodusww, true);
                    await this.setStateAsync('control.ModusWW', statemodusww, true);
                },
                cb
            );
        });
    }

    private controlModusHeizung(statemodusheizung: number): void {
        this.enqueueCommand('write_3406', (cb) => {
            this.executeWrite(
                '3406',
                `3406;1;${statemodusheizung}`,
                async () => {
                    await this.setStateAsync('status.ModusHeizung', statemodusheizung, true);
                    await this.setStateAsync('control.ModusHeizung', statemodusheizung, true);
                },
                cb
            );
        });
    }

    private controlSchaltzWoBW(arr: (string | number)[]): void {
        this.schaltzbwblock = false;
        this.enqueueCommand('write_3201', (cb) => {
            this.executeWrite(
                '3201',
                arr.join(';'),
                () => {
                    this.enqueueCommand('verify_3200', (verifyCb) => this.callLuxtronik3200(verifyCb));
                },
                cb
            );
        });
    }

    private controlZeit(arr: (string | number)[]): void {
        this.zeitblock = false;
        this.enqueueCommand('write_2701', (cb) => {
            this.executeWrite('2701', arr.join(';'), null, cb);
        });
    }

    private pollLuxtronik(): void {
        if (this.pollQueue.length > 0 || this.isProcessingQueue) {
            this.log.debug('Vorheriger Poll oder Befehl noch aktiv, überspringe diesen Zyklus');
            return;
        }
        this.enqueuePoll('1800', (cb) => this.callLuxtronik1800(cb));
        this.enqueuePoll('2100', (cb) => this.callLuxtronik2100(cb));
        this.enqueuePoll('3405', (cb) => this.callLuxtronik3405(cb));
        this.enqueuePoll('3505', (cb) => this.callLuxtronik3505(cb));
        this.enqueuePoll('3400', (cb) => this.callLuxtronik3400(cb));
        this.enqueuePoll('3200', (cb) => this.callLuxtronik3200(cb));
    }

    private toTenths(value: ioBroker.StateValue, id: string): number | null {
        const n = Number(value);
        if (value === null || value === '' || typeof value === 'boolean' || !Number.isFinite(n)) {
            this.log.warn(`Ungültiger Wert für ${id}: ${String(value)} - Befehl wird ignoriert`);
            return null;
        }
        return Math.round(n * 10);
    }

    private async controlLuxtronik(id: string, stateVal: ioBroker.StateValue): Promise<void> {
        try {
            let tenths: number | null = null;
            if (NUMERIC_CONTROLS.includes(id)) {
                tenths = this.toTenths(stateVal, id);
                if (tenths === null) {
                    return;
                }
            }

            switch (id) {
                case 'control.BWs':
                    this.bwretry = 0;
                    this.lastsetbws = tenths!;
                    this.controlBws(tenths!);
                    break;
                case 'control.ModusWW':
                    this.controlModusWw(Number(stateVal));
                    break;
                case 'control.ModusHeizung':
                    this.controlModusHeizung(Number(stateVal));
                    break;
                case 'control.AbwRLs':
                    this.updateHeizkurveField(2, tenths!);
                    break;
                case 'control.EndpunktHK':
                    this.updateHeizkurveField(3, tenths!);
                    break;
                case 'control.ParaVHK':
                    this.updateHeizkurveField(4, tenths!);
                    break;
                case 'control.NachtAbs':
                    this.updateHeizkurveField(5, tenths!);
                    break;
                case 'control.HystHKs':
                    this.updateHystereseField(3, tenths!);
                    break;
                case 'control.HystBWs':
                    this.updateHystereseField(9, tenths!);
                    break;
                case 'control.SchaltzWoBW.senden':
                    await this.readStatesAndSendSchaltzeiten();
                    break;
                case 'control.Zeit.senden':
                    await this.readStatesAndSendZeit();
                    break;
                default:
                    if (id.includes('control.SchaltzWoBW.Start') || id.includes('control.SchaltzWoBW.Ende')) {
                        this.schaltzbwblock = true;
                        this.later(() => {
                            this.schaltzbwblock = false;
                        }, 60000);
                    } else if (
                        id.includes('control.Zeit.uhrzeit') ||
                        id.includes('control.Zeit.dow') ||
                        id.includes('control.Zeit.datum')
                    ) {
                        this.zeitblock = true;
                        this.later(() => {
                            this.zeitblock = false;
                        }, 60000);
                    }
            }
        } catch (e) {
            this.log.warn(`controlluxtronik-Fehler: ${String(e)}`);
        }
    }

    private async readStatesAndSendSchaltzeiten(): Promise<void> {
        const keys = [
            'control.SchaltzWoBW.Start1',
            'control.SchaltzWoBW.Ende1',
            'control.SchaltzWoBW.Start2',
            'control.SchaltzWoBW.Ende2',
        ];
        const payload: (string | number)[] = ['3201', '8'];

        for (const key of keys) {
            const state = await this.getStateAsync(key);
            if (!state || typeof state.val !== 'string') {
                this.log.warn(`Fehler beim Auslesen von ${key}`);
                return;
            }
            payload.push(Number(state.val.slice(0, 2)), Number(state.val.slice(-2)));
        }

        this.controlSchaltzWoBW(payload);
    }

    private async readStatesAndSendZeit(): Promise<void> {
        const [sDatum, sDow, sZeit] = await Promise.all([
            this.getStateAsync('control.Zeit.datum'),
            this.getStateAsync('control.Zeit.dow'),
            this.getStateAsync('control.Zeit.uhrzeit'),
        ]);

        if (!sDatum?.val || sDow?.val === undefined || sDow?.val === null || !sZeit?.val) {
            this.log.warn('Fehler beim Auslesen der Zeitfelder');
            return;
        }

        const dStr = String(sDatum.val);
        const tStr = String(sZeit.val);
        const payload: (string | number)[] = [
            '2701',
            '7',
            Number(dStr.slice(-2)),
            Number(dStr.slice(3, 5)),
            Number(dStr.slice(0, 2)),
            Number(sDow.val),
            Number(tStr.slice(0, 2)),
            Number(tStr.slice(3, 5)),
            Number(tStr.slice(-2)),
        ];
        this.controlZeit(payload);
    }

    private setFehlerText(fehlerinfo?: string): string {
        try {
            if (!fehlerinfo) {
                return '';
            }
            const arr = fehlerinfo.split(';');
            const code = arr[3];
            const zeit = `${`00${arr[4]}`.slice(-2)}.${`00${arr[5]}`.slice(-2)}.${`00${arr[6]}`.slice(-2)}, ${`00${arr[7]}`.slice(-2)}:${`00${arr[8]}`.slice(-2)}`;
            const text = ERROR_CODES[code] !== undefined ? ERROR_CODES[code] : `Unbekannter Fehler (${code})`;
            return `${text} ${zeit}`;
        } catch (e) {
            this.log.warn(`setfehlertext - Fehler: ${String(e)}`);
            return '';
        }
    }

    private setAbschaltText(abschaltinfo?: string): string {
        try {
            if (!abschaltinfo) {
                return '';
            }
            const arr = abschaltinfo.split(';');
            const code = arr[3];
            const zeit = `${`00${arr[4]}`.slice(-2)}.${`00${arr[5]}`.slice(-2)}.${`00${arr[6]}`.slice(-2)}, ${`00${arr[7]}`.slice(-2)}:${`00${arr[8]}`.slice(-2)}`;
            const map: Record<string, string> = {
                '010': 'weniger Waerme, ',
                '001': 'Waermepumpenstoerung, ',
                '002': 'Anlagenstoerung, ',
            };
            return `${map[code] ?? ''} ${zeit}`;
        } catch (e) {
            this.log.warn(`setabschalttext - Fehler: ${String(e)}`);
            return '';
        }
    }

    private setStatusText(statuscode: string): string {
        try {
            const code = statuscode.split(';')[5];
            const map: Record<string, string> = {
                '0': 'Heizung',
                '1': 'Warmwasser',
                '2': 'Schwimmbad',
                '3': 'EVU-Sperre',
                '4': 'Abtauen',
                '5': 'Bereitschaft',
            };
            return map[code] ?? 'Status unklar';
        } catch (e) {
            this.log.warn(`setstatustext - Fehler: ${String(e)}`);
            return 'Status unklar';
        }
    }

    private toTimeString(totalseconds: string | number): string {
        try {
            const total = Number(totalseconds) || 0;
            const hours = Math.floor(total / 3600);
            const minutes = Math.floor((total - hours * 3600) / 60);
            const seconds = Math.floor(total - hours * 3600 - minutes * 60);
            const hStr = hours < 10 ? `0${hours}` : String(hours);
            const mStr = minutes < 10 ? `0${minutes}` : String(minutes);
            const sStr = seconds < 10 ? `0${seconds}` : String(seconds);
            return `${hStr}h ${mStr}min ${sStr}s`;
        } catch (e) {
            this.log.warn(`toTimeString - Fehler: ${String(e)}`);
            return '00h 00min 00s';
        }
    }
}

if (require.main !== module) {
    module.exports = (options: Partial<utils.AdapterOptions> | undefined) => new Luxtronik1(options);
} else {
    (() => new Luxtronik1())();
}
